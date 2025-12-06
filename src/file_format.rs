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

//! [`OrcFormat`]: ORC [`FileFormat`] abstractions

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_common::{GetExt, Result};
use datafusion_datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::TableSchema;
use datafusion_physical_plan::ExecutionPlan;
use futures::StreamExt;
use futures::TryStreamExt;
use object_store::ObjectStore;

use crate::metadata::{read_orc_schema, read_orc_statistics};
use crate::source::OrcSource;

/// Factory struct used to create [`OrcFormat`]
#[derive(Debug, Default)]
pub struct OrcFormatFactory;

impl OrcFormatFactory {
    /// Creates an instance of [`OrcFormatFactory`]
    pub fn new() -> Self {
        Self
    }
}

impl FileFormatFactory for OrcFormatFactory {
    fn create(
        &self,
        _state: &dyn datafusion_session::Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(OrcFormat))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(OrcFormat)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for OrcFormatFactory {
    fn get_ext(&self) -> String {
        "orc".to_string()
    }
}

/// The Apache ORC `FileFormat` implementation
#[derive(Debug, Default)]
pub struct OrcFormat;

impl OrcFormat {
    /// Construct a new Format with default options
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl FileFormat for OrcFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "orc".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &datafusion_datasource::file_compression_type::FileCompressionType,
    ) -> Result<String> {
        // ORC files have built-in compression, so the extension is always "orc"
        Ok("orc".to_string())
    }

    fn compression_type(
        &self,
    ) -> Option<datafusion_datasource::file_compression_type::FileCompressionType> {
        // ORC files have built-in compression support
        None
    }

    async fn infer_schema(
        &self,
        state: &dyn datafusion_session::Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[object_store::ObjectMeta],
    ) -> Result<arrow::datatypes::SchemaRef> {
        use futures::stream::iter;

        // Read schemas from all objects concurrently
        let store_clone = Arc::clone(store);
        let schemas: Vec<_> = iter(objects.iter())
            .map(|object| {
                let store = Arc::clone(&store_clone);
                async move { read_orc_schema(&store, object).await }
            })
            .boxed() // Workaround for lifetime issues
            .buffered(state.config_options().execution.meta_fetch_concurrency)
            .try_collect()
            .await?;

        // Merge all schemas
        // Schema::try_merge needs owned Schema, not Arc<Schema>
        let schemas: Vec<_> = schemas.into_iter().map(|s| (*s).clone()).collect();
        let merged_schema = arrow::datatypes::Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn datafusion_session::Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: arrow::datatypes::SchemaRef,
        object: &object_store::ObjectMeta,
    ) -> Result<datafusion_common::Statistics> {
        read_orc_statistics(store, object, table_schema).await
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn datafusion_session::Session,
        conf: datafusion_datasource::file_scan_config::FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create OrcSource from the file scan config
        // conf.file_schema() returns SchemaRef, we need to convert it to TableSchema
        let file_schema = conf.file_schema();
        let table_schema = TableSchema::from_file_schema(file_schema.clone());
        let source = Arc::new(OrcSource::new(table_schema));

        // Create new FileScanConfig with OrcSource
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(source)
            .build();

        // Create DataSourceExec
        Ok(DataSourceExec::from_data_source(conf))
    }

    fn file_source(&self) -> Arc<dyn datafusion_datasource::file::FileSource> {
        // Return a default OrcSource
        // The actual schema will be set when create_physical_plan is called
        Arc::new(OrcSource::new(TableSchema::from_file_schema(Arc::new(
            arrow::datatypes::Schema::empty(),
        ))))
    }
}
