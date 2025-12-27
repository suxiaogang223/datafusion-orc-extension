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
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::TableSchema;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use object_store::ObjectStore;
use orc_rust::predicate::Predicate as OrcPredicate;

use crate::opener::OrcOpener;
use crate::predicate::convert_physical_expr_to_predicate;

/// Execution plan for reading one or more ORC files
pub struct OrcSource {
    /// Table schema
    table_schema: TableSchema,
    /// Execution plan metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional predicate filter pushed down from DataFusion
    predicate: Option<Arc<dyn PhysicalExpr>>,
    /// Converted orc-rust predicate (cached for efficiency)
    orc_predicate: Option<OrcPredicate>,
}

impl Debug for OrcSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrcSource")
            .field("table_schema", &self.table_schema)
            .field("predicate", &self.predicate)
            .finish()
    }
}

impl OrcSource {
    /// Create a new OrcSource
    pub fn new(table_schema: impl Into<TableSchema>) -> Self {
        Self {
            table_schema: table_schema.into(),
            metrics: ExecutionPlanMetricsSet::new(),
            predicate: None,
            orc_predicate: None,
        }
    }

    /// Create a new OrcSource with a predicate filter
    ///
    /// The predicate will be converted to an orc-rust Predicate and used
    /// for stripe-level filtering during file reads.
    pub fn with_predicate(mut self, predicate: Arc<dyn PhysicalExpr>) -> Self {
        // Try to convert DataFusion predicate to orc-rust predicate
        let file_schema = self.table_schema.file_schema();
        let orc_pred = convert_physical_expr_to_predicate(&predicate, file_schema);
        self.orc_predicate = orc_pred;
        self.predicate = Some(predicate);
        self
    }

    /// Get the orc-rust predicate (if conversion was successful)
    pub fn orc_predicate(&self) -> Option<&OrcPredicate> {
        self.orc_predicate.as_ref()
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
        let file_schema = base_config.file_schema();
        let projection: Arc<[usize]> = base_config
            .file_column_projection_indices()
            .map(|indices| indices.into())
            .unwrap_or_else(|| (0..file_schema.fields().len()).collect::<Vec<_>>().into());

        // Get batch size from config or use default
        let batch_size = base_config.batch_size.unwrap_or(8192); // Default batch size

        // Get limit from config
        let limit = base_config.limit;

        // Get projected file schema (without partition columns)
        let logical_file_schema = base_config.projected_file_schema();

        // Get partition fields
        let partition_fields = base_config.table_partition_cols().clone();

        // Get metrics
        let metrics = self.metrics.clone();

        // Clone the orc predicate for the opener
        let orc_predicate = self.orc_predicate.clone();

        Arc::new(OrcOpener::new(
            partition,
            projection,
            batch_size,
            limit,
            logical_file_schema,
            partition_fields,
            metrics,
            object_store,
            orc_predicate,
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            table_schema: self.table_schema.clone(),
            metrics: self.metrics.clone(),
            predicate: self.predicate.clone(),
            orc_predicate: self.orc_predicate.clone(),
        })
    }

    fn with_schema(&self, schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(Self {
            table_schema: schema,
            metrics: self.metrics.clone(),
            predicate: self.predicate.clone(),
            orc_predicate: self.orc_predicate.clone(),
        })
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        Arc::new(Self {
            table_schema: self.table_schema.clone(),
            metrics: self.metrics.clone(),
            predicate: self.predicate.clone(),
            orc_predicate: self.orc_predicate.clone(),
        })
    }

    fn with_statistics(&self, _statistics: datafusion_common::Statistics) -> Arc<dyn FileSource> {
        Arc::new(Self {
            table_schema: self.table_schema.clone(),
            metrics: self.metrics.clone(),
            predicate: self.predicate.clone(),
            orc_predicate: self.orc_predicate.clone(),
        })
    }

    fn statistics(&self) -> datafusion_common::Result<datafusion_common::Statistics> {
        Ok(datafusion_common::Statistics::new_unknown(
            self.table_schema.table_schema().as_ref(),
        ))
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "orc"
    }

    /// Returns the filter expression that will be applied during the file scan.
    fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
        self.predicate.clone()
    }
}
