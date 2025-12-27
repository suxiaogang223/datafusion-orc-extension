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
use arrow::record_batch::RecordBatchOptions;
use datafusion_common::{DataFusionError, Result};
use datafusion_datasource::file_stream::{FileOpenFuture, FileOpener};
use datafusion_datasource::PartitionedFile;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::ObjectStore;
use orc_rust::arrow_reader::ArrowReaderBuilder;
use orc_rust::predicate::Predicate as OrcPredicate;
use orc_rust::projection::ProjectionMask;
use orc_rust::schema::RootDataType;
use std::collections::HashMap;
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
    /// Optional predicate for stripe-level filtering
    pub predicate: Option<OrcPredicate>,
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
        predicate: Option<OrcPredicate>,
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
            predicate,
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
        let predicate = self.predicate.clone();

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
                let (projection_mask, projection_map, file_column_count) = {
                    let file_metadata = arrow_reader_builder.file_metadata();
                    let root_data_type = file_metadata.root_data_type();
                    let file_len = root_data_type.children().len();

                    let (mask, map) = build_projection_mask(root_data_type, projection.as_ref())?;

                    (mask, Arc::new(map), file_len)
                };

                // Apply projection and batch size
                arrow_reader_builder = arrow_reader_builder
                    .with_projection(projection_mask)
                    .with_batch_size(batch_size);

                // Apply predicate for stripe-level filtering if available
                if let Some(pred) = predicate {
                    arrow_reader_builder = arrow_reader_builder.with_predicate(pred);
                }

                // Build the async stream reader
                let arrow_stream_reader = arrow_reader_builder.build_async();

                // Convert ArrowError to DataFusionError
                let projected_stream = arrow_stream_reader.map(|result| {
                    result.map_err(|e| {
                        DataFusionError::External(format!("Failed to read ORC batch: {}", e).into())
                    })
                });

                let needs_projection = should_apply_batch_projection(
                    projection.as_ref(),
                    file_column_count,
                    &logical_file_schema,
                );

                let base_stream: BoxStream<'static, Result<RecordBatch>> = if needs_projection {
                    let logical_file_schema = Arc::clone(&logical_file_schema);
                    let projection = Arc::clone(&projection);
                    let projection_map = Arc::clone(&projection_map);
                    projected_stream
                        .map(move |result| {
                            result.and_then(|batch| {
                                project_batch(
                                    batch,
                                    &logical_file_schema,
                                    projection.as_ref(),
                                    &projection_map,
                                )
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
    projection_map: &HashMap<usize, usize>,
) -> Result<RecordBatch> {
    if projection.is_empty() {
        let mut options = RecordBatchOptions::new();
        options.row_count = Some(batch.num_rows());
        return RecordBatch::try_new_with_options(Arc::clone(logical_schema), vec![], &options)
            .map_err(|e| {
                DataFusionError::External(
                    format!("Failed to build empty projection batch: {e}").into(),
                )
            });
    }

    let columns = projection
        .iter()
        .map(|&file_index| {
            let batch_index = projection_map.get(&file_index).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Projection index {file_index} not found in ORC projection map"
                ))
            })?;
            Ok(batch.column(*batch_index).clone())
        })
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(Arc::clone(logical_schema), columns)
        .map_err(|e| DataFusionError::External(format!("Failed to project ORC batch: {e}").into()))
}

fn build_projection_mask(
    root_data_type: &RootDataType,
    projection: &[usize],
) -> Result<(ProjectionMask, HashMap<usize, usize>)> {
    let file_len = root_data_type.children().len();

    if projection.is_empty() {
        return Ok((
            ProjectionMask::roots(root_data_type, std::iter::empty()),
            HashMap::new(),
        ));
    }

    let is_identity = projection.len() == file_len
        && projection
            .iter()
            .enumerate()
            .all(|(expected, &idx)| expected == idx);

    if is_identity {
        let mapping = (0..file_len).map(|idx| (idx, idx)).collect();
        return Ok((ProjectionMask::all(), mapping));
    }

    let mut ordinals = projection.to_vec();
    ordinals.sort_unstable();
    ordinals.dedup();

    let mask_indices = ordinals
        .iter()
        .map(|&ordinal| {
            root_data_type
                .children()
                .get(ordinal)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Projection index {ordinal} is out of bounds for ORC file with {file_len} columns"
                    ))
                })
                .map(|col| col.data_type().column_index())
        })
        .collect::<Result<Vec<_>>>()?;

    let mapping = ordinals
        .iter()
        .enumerate()
        .map(|(position, &ordinal)| (ordinal, position))
        .collect();

    Ok((ProjectionMask::roots(root_data_type, mask_indices), mapping))
}

fn should_apply_batch_projection(
    projection: &[usize],
    file_column_count: usize,
    logical_schema: &SchemaRef,
) -> bool {
    if projection.is_empty() {
        return true;
    }

    let is_full_identity = projection.len() == file_column_count
        && projection
            .iter()
            .enumerate()
            .all(|(expected, &idx)| expected == idx);

    if is_full_identity {
        return false;
    }

    let is_in_file_order = projection.windows(2).all(|pair| pair[0] < pair[1]);

    let schema_matches_projection = logical_schema.fields().len() == projection.len();

    !(is_in_file_order && schema_matches_projection)
}
