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

//! File opening and configuration logic

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::PartitionedFile;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::ObjectStore;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::projection::ProjectionMask;
use std::sync::{Arc, Mutex};

use crate::reader::ObjectStoreChunkReader;

/// Implements [`FileOpener`] for an ORC file
pub struct OrcOpener {
    /// Execution partition index
    pub partition_index: usize,
    /// Column indexes in `table_schema` needed by the query
    pub projection: Arc<[usize]>,
    /// Target number of rows in each output RecordBatch
    pub batch_size: usize,
    /// Optional limit on the number of rows to read
    pub limit: Option<usize>,
    /// Schema of the output table without partition columns
    pub logical_file_schema: SchemaRef,
    /// Partition columns
    pub partition_fields: Vec<arrow::datatypes::FieldRef>,
    /// Metrics for reporting
    pub metrics: ExecutionPlanMetricsSet,
    /// ObjectStore for reading files
    pub object_store: Arc<dyn ObjectStore>,
}

impl OrcOpener {
    /// Create a new OrcOpener
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition_index: usize,
        projection: Arc<[usize]>,
        batch_size: usize,
        limit: Option<usize>,
        logical_file_schema: SchemaRef,
        partition_fields: Vec<arrow::datatypes::FieldRef>,
        metrics: ExecutionPlanMetricsSet,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            partition_index,
            projection,
            batch_size,
            limit,
            logical_file_schema,
            partition_fields,
            metrics,
            object_store,
        }
    }
}

impl FileOpener for OrcOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let object_store = Arc::clone(&self.object_store);
        let file_location = partitioned_file.object_meta.location.clone();
        let file_size = partitioned_file.object_meta.size;
        let batch_size = self.batch_size;
        let limit = self.limit;
        let projection = Arc::clone(&self.projection);
        let logical_file_schema = Arc::clone(&self.logical_file_schema);

        let future: BoxFuture<'static, Result<BoxStream<'static, Result<RecordBatch>>>> =
            Box::pin(async move {
                // Create ArrowStreamReader for async reading
                // We need to create a new reader since try_new_async takes ownership
                let stream_reader =
                    ObjectStoreChunkReader::with_size(object_store, file_location, file_size);

                // Build the async reader using ArrowReaderBuilder
                // try_new_async takes ownership of the reader
                let mut arrow_reader_builder = ArrowReaderBuilder::try_new_async(stream_reader)
                    .await
                    .map_err(|e| {
                        DataFusionError::External(
                            format!("Failed to create ORC reader builder: {}", e).into(),
                        )
                    })?;

                // Apply projection if not all columns are needed
                let file_metadata = arrow_reader_builder.file_metadata();
                let root_data_type = file_metadata.root_data_type();

                // Check if we need to apply projection
                // If projection includes all columns, use ProjectionMask::all()
                // Otherwise, create a projection mask
                let projection_mask = if projection.len() == root_data_type.children().len()
                    && projection.iter().enumerate().all(|(i, &idx)| i == idx)
                {
                    ProjectionMask::all()
                } else {
                    // TODO: implement projection pushdown once schema reconciliation is available
                    ProjectionMask::all()
                };

                // Apply projection and batch size
                arrow_reader_builder = arrow_reader_builder
                    .with_projection(projection_mask)
                    .with_batch_size(batch_size);

                // Build the async stream reader
                let arrow_stream_reader = arrow_reader_builder.build_async();

                // Convert ArrowError to DataFusionError
                let projected_stream = arrow_stream_reader.map(|result| {
                    result.map_err(|e| {
                        DataFusionError::External(format!("Failed to read ORC batch: {}", e).into())
                    })
                });

                let needs_projection = {
                    let logical_len = logical_file_schema.fields().len();
                    let projection_len = projection.len();
                    projection_len != logical_len
                        || !projection.iter().enumerate().all(|(i, &idx)| i == idx)
                };

                let base_stream: BoxStream<'static, Result<RecordBatch>> = if needs_projection {
                    let logical_file_schema = Arc::clone(&logical_file_schema);
                    let projection = Arc::clone(&projection);
                    projected_stream
                        .map(move |result| {
                            result.and_then(|batch| {
                                project_batch(batch, &logical_file_schema, projection.as_ref())
                            })
                        })
                        .boxed()
                } else {
                    projected_stream.boxed()
                };

                // Apply limit if specified
                let stream: BoxStream<'static, Result<RecordBatch>> = if let Some(limit_val) = limit
                {
                    // Use Arc<Mutex<>> to share mutable state across closures
                    let rows_read = Arc::new(Mutex::new(0usize));
                    base_stream
                        .take_while({
                            let rows_read = Arc::clone(&rows_read);
                            move |result| {
                                // Continue on error to propagate it
                                if result.is_err() {
                                    return futures::future::ready(true);
                                }

                                let count = rows_read.lock().unwrap();
                                // Check if we've already reached the limit BEFORE processing this batch
                                // This allows the batch that would exceed the limit to pass through
                                // so it can be truncated in the map closure
                                if *count >= limit_val {
                                    return futures::future::ready(false);
                                }
                                // Allow the batch to pass through (even if it will exceed the limit)
                                // The map closure will handle truncation
                                futures::future::ready(true)
                            }
                        })
                        .map({
                            let rows_read = Arc::clone(&rows_read);
                            move |result| {
                                match result {
                                    Ok(batch) => {
                                        let mut count = rows_read.lock().unwrap();
                                        let batch_rows = batch.num_rows();

                                        if *count >= limit_val {
                                            // Already reached limit, return empty batch
                                            Ok(RecordBatch::new_empty(batch.schema()))
                                        } else if *count + batch_rows <= limit_val {
                                            // Batch fits within limit
                                            *count += batch_rows;
                                            Ok(batch)
                                        } else {
                                            // Batch exceeds limit, truncate it
                                            let remaining = limit_val - *count;
                                            *count = limit_val;
                                            Ok(batch.slice(0, remaining))
                                        }
                                    }
                                    Err(e) => Err(e),
                                }
                            }
                        })
                        .boxed()
                } else {
                    base_stream.boxed()
                };

                Ok(stream)
            });

        Ok(future)
    }
}

fn project_batch(
    batch: RecordBatch,
    logical_schema: &SchemaRef,
    projection: &[usize],
) -> Result<RecordBatch> {
    if projection.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(logical_schema)));
    }

    let columns = projection
        .iter()
        .map(|&idx| batch.column(idx).clone())
        .collect::<Vec<_>>();

    RecordBatch::try_new(Arc::clone(logical_schema), columns)
        .map_err(|e| DataFusionError::External(format!("Failed to project ORC batch: {e}").into()))
}
