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

//! OrcSource implementation for reading ORC files

use std::any::Any;
use std::sync::Arc;

use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::TableSchema;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectStore;

use crate::opener::OrcOpener;

/// Execution plan for reading one or more ORC files
#[derive(Debug)]
pub struct OrcSource {
    /// Table schema
    table_schema: TableSchema,
    /// Execution plan metrics
    metrics: ExecutionPlanMetricsSet,
}

impl OrcSource {
    /// Create a new OrcSource
    pub fn new(table_schema: impl Into<TableSchema>) -> Self {
        Self {
            table_schema: table_schema.into(),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl FileSource for OrcSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn FileOpener> {
        // Extract projection indices from the file scan config
        // For now, we'll project all columns (no projection pushdown yet)
        let file_schema = base_config.file_schema();
        let projection: Arc<[usize]> = (0..file_schema.fields().len()).collect::<Vec<_>>().into();

        // Get batch size from config or use default
        let batch_size = base_config.batch_size.unwrap_or(8192); // Default batch size

        // Get limit from config
        let limit = base_config.limit;

        // Get file schema (without partition columns)
        let logical_file_schema = base_config.file_schema().clone();

        // Get partition fields
        let partition_fields = base_config.table_partition_cols().clone();

        // Get metrics
        let metrics = self.metrics.clone();

        Arc::new(OrcOpener::new(
            partition,
            projection,
            batch_size,
            limit,
            logical_file_schema,
            partition_fields,
            metrics,
            object_store,
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        // TODO: Implement batch size configuration
        Arc::new(Self {
            table_schema: self.table_schema.clone(),
            metrics: self.metrics.clone(),
        })
    }

    fn with_schema(&self, schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(Self {
            table_schema: schema,
            metrics: self.metrics.clone(),
        })
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        // TODO: Implement projection support
        Arc::new(Self {
            table_schema: self.table_schema.clone(),
            metrics: self.metrics.clone(),
        })
    }

    fn with_statistics(&self, _statistics: datafusion_common::Statistics) -> Arc<dyn FileSource> {
        // TODO: Implement statistics support
        Arc::new(Self {
            table_schema: self.table_schema.clone(),
            metrics: self.metrics.clone(),
        })
    }

    fn statistics(&self) -> datafusion_common::Result<datafusion_common::Statistics> {
        todo!("Statistics not yet implemented")
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "orc"
    }
}
