// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! ORC metadata processing

use arrow::datatypes::SchemaRef;
use datafusion_common::stats::Precision;
use datafusion_common::{DataFusionError, Result, Statistics};
use object_store::{ObjectMeta, ObjectStore};
use orc_rust::reader::metadata::read_metadata_async;
use orc_rust::schema::ArrowSchemaOptions;
use std::collections::HashMap;
use std::sync::Arc;

use crate::reader::ObjectStoreChunkReader;

/// Read ORC file metadata and extract schema
pub async fn read_orc_schema(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
) -> Result<SchemaRef> {
    let mut reader =
        ObjectStoreChunkReader::with_size(Arc::clone(store), object.location.clone(), object.size);

    let file_metadata = read_metadata_async(&mut reader).await.map_err(|e| {
        DataFusionError::External(format!("Failed to read ORC metadata: {}", e).into())
    })?;

    // Convert ORC schema to Arrow schema
    let root_data_type = file_metadata.root_data_type();
    let metadata: HashMap<String, String> = file_metadata
        .user_custom_metadata()
        .iter()
        .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).to_string()))
        .collect();

    let options = ArrowSchemaOptions::new();
    let schema = root_data_type.create_arrow_schema_with_options(&metadata, options);

    Ok(Arc::new(schema))
}

/// Read ORC file statistics
pub async fn read_orc_statistics(
    store: &Arc<dyn ObjectStore>,
    object: &ObjectMeta,
    _table_schema: SchemaRef,
) -> Result<Statistics> {
    let mut reader =
        ObjectStoreChunkReader::with_size(Arc::clone(store), object.location.clone(), object.size);

    let file_metadata = read_metadata_async(&mut reader).await.map_err(|e| {
        DataFusionError::External(format!("Failed to read ORC metadata: {}", e).into())
    })?;

    // Extract statistics from ORC file metadata
    let num_rows = file_metadata.number_of_rows();

    // TODO: Extract column-level statistics (min/max/null counts) from file_metadata
    // For now, return basic statistics
    Ok(Statistics {
        num_rows: Precision::Exact(num_rows as usize),
        total_byte_size: Precision::Exact(object.size as usize),
        column_statistics: vec![],
    })
}
