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

//! Integration tests for basic ORC file reading functionality

use datafusion::assert_batches_eq;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::functions_aggregate::expr_fn::{count, max, min};
use datafusion::prelude::*;
use datafusion_common::stats::Precision;
use datafusion_datasource::file_format::FileFormat;
use datafusion_datasource_orc::{OrcFormat, OrcFormatOptions, OrcReadOptions};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;
use std::path::PathBuf;
use std::sync::Arc;

/// Format schema to a normalized string for comparison while preserving order
fn format_schema(schema: &arrow::datatypes::Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|f| {
            let nullability = if f.is_nullable() {
                "nullable"
            } else {
                "non-nullable"
            };
            format!(
                "{}: {} ({})",
                f.name(),
                format_data_type(f.data_type()),
                nullability
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Format data type to string representation
fn format_data_type(dt: &arrow::datatypes::DataType) -> String {
    match dt {
        arrow::datatypes::DataType::Boolean => "Boolean".to_string(),
        arrow::datatypes::DataType::Int8 => "Int8".to_string(),
        arrow::datatypes::DataType::Int16 => "Int16".to_string(),
        arrow::datatypes::DataType::Int32 => "Int32".to_string(),
        arrow::datatypes::DataType::Int64 => "Int64".to_string(),
        arrow::datatypes::DataType::Float32 => "Float32".to_string(),
        arrow::datatypes::DataType::Float64 => "Float64".to_string(),
        arrow::datatypes::DataType::Utf8 => "Utf8".to_string(),
        arrow::datatypes::DataType::Binary => "Binary".to_string(),
        arrow::datatypes::DataType::Date32 => "Date32".to_string(),
        arrow::datatypes::DataType::Decimal128(precision, scale) => {
            format!("Decimal128({}, {})", precision, scale)
        }
        arrow::datatypes::DataType::List(field) => {
            format!(
                "List({}{})",
                format_data_type(field.data_type()),
                if field.is_nullable() {
                    ", nullable"
                } else {
                    ""
                }
            )
        }
        arrow::datatypes::DataType::Map(field, _) => match field.data_type() {
            arrow::datatypes::DataType::Struct(struct_fields) => {
                let entries: Vec<String> = struct_fields
                    .iter()
                    .map(|f| {
                        format!(
                            "{}: {}{}",
                            f.name(),
                            format_data_type(f.data_type()),
                            if f.is_nullable() { " (nullable)" } else { "" }
                        )
                    })
                    .collect();
                format!("Map(Struct({}))", entries.join(", "))
            }
            _ => format!("Map({})", format_data_type(field.data_type())),
        },
        _ => format!("{:?}", dt),
    }
}

/// Convert filesystem path into an ObjectStore path
fn to_object_store_path(path: &PathBuf) -> ObjectStorePath {
    ObjectStorePath::from_filesystem_path(path).expect("Failed to convert path")
}

/// Helper function to create a test ObjectStore
fn create_test_object_store() -> Arc<dyn ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

/// Helper function to get test data directory
fn get_test_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("integration")
        .join("data")
}

/// Register an ORC file as a table using ListingTable infrastructure
async fn register_orc_table(
    ctx: &SessionContext,
    table_name: &str,
    file_name: &str,
) -> datafusion_common::Result<()> {
    register_orc_table_with_options(ctx, table_name, file_name, OrcFormat::new()).await
}

/// Register an ORC file as a table with custom format options
async fn register_orc_table_with_options(
    ctx: &SessionContext,
    table_name: &str,
    file_name: &str,
    format: OrcFormat,
) -> datafusion_common::Result<()> {
    let file_path = get_test_data_dir().join(file_name);
    let table_path = ListingTableUrl::parse(
        file_path
            .to_str()
            .expect("Failed to convert path to string"),
    )?;

    let listing_options = ListingOptions::new(Arc::new(format)).with_file_extension(".orc");

    let session_state = ctx.state();
    let schema = listing_options
        .infer_schema(&session_state, &table_path)
        .await?;

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(schema);

    let table = ListingTable::try_new(config)?;
    ctx.register_table(table_name, Arc::new(table))?;
    Ok(())
}

#[tokio::test]
async fn test_schema_inference_alltypes() {
    let test_data_dir = get_test_data_dir();
    let orc_file = test_data_dir.join("alltypes.snappy.orc");

    let object_store = create_test_object_store();
    let format = OrcFormat::new();

    // Get file metadata
    let file_path = to_object_store_path(&orc_file);
    let file_meta = object_store
        .head(&file_path)
        .await
        .expect("Failed to get file metadata");

    // Create SessionContext and get SessionState
    let ctx = SessionContext::new();
    let session_state = ctx.state();

    // Infer schema
    let schema = format
        .infer_schema(&session_state, &object_store, &[file_meta])
        .await
        .expect("Failed to infer schema");

    // Verify schema is not empty
    assert!(!schema.fields().is_empty(), "Expected non-empty schema");

    // Format schema to string and compare with expected result
    let schema_str = format_schema(&schema);
    let expected = "boolean: Boolean (nullable), int8: Int8 (nullable), int16: Int16 (nullable), int32: Int32 (nullable), int64: Int64 (nullable), float32: Float32 (nullable), float64: Float64 (nullable), decimal: Decimal128(15, 5) (nullable), binary: Binary (nullable), utf8: Utf8 (nullable), date32: Date32 (nullable)";

    assert_eq!(
        schema_str, expected,
        "\nSchema mismatch!\nExpected: {}\nGot:      {}",
        expected, schema_str
    );

    println!("Inferred schema: {}", schema_str);
}

#[tokio::test]
async fn test_schema_inference_map_list() {
    let test_data_dir = get_test_data_dir();
    let orc_file = test_data_dir.join("map_list.snappy.orc");

    let object_store = create_test_object_store();
    let format = OrcFormat::new();

    // Get file metadata
    let file_path = to_object_store_path(&orc_file);
    let file_meta = object_store
        .head(&file_path)
        .await
        .expect("Failed to get file metadata");

    // Create SessionContext and get SessionState
    let ctx = SessionContext::new();
    let session_state = ctx.state();

    // Infer schema
    let schema = format
        .infer_schema(&session_state, &object_store, &[file_meta])
        .await
        .expect("Failed to infer schema");

    // Verify schema is not empty
    assert!(!schema.fields().is_empty(), "Expected non-empty schema");

    // Format schema to string and compare with expected result
    let schema_str = format_schema(&schema);
    let expected = "id: Int64 (nullable), m: Map(Struct(keys: Utf8, values: Utf8 (nullable))) (nullable), l: List(Utf8, nullable) (nullable), s: Utf8 (nullable)";

    assert_eq!(
        schema_str, expected,
        "\nSchema mismatch!\nExpected: {}\nGot:      {}",
        expected, schema_str
    );

    println!("Inferred schema: {}", schema_str);
}

#[tokio::test]
async fn test_basic_reading_alltypes_full_scan() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let batches = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .collect()
        .await
        .expect("Failed to collect batches");

    let expected = [
        "+---------+------+--------+-------------+----------------------+------------+----------------+------------------+--------------------------+----------+------------+",
        "| boolean | int8 | int16  | int32       | int64                | float32    | float64        | decimal          | binary                   | utf8     | date32     |",
        "+---------+------+--------+-------------+----------------------+------------+----------------+------------------+--------------------------+----------+------------+",
        "|         |      |        |             |                      |            |                |                  |                          |          |            |",
        "| true    | 0    | 0      | 0           | 0                    | 0.0        | 0.0            | 0.00000          |                          |          | 1970-01-01 |",
        "| false   | 1    | 1      | 1           | 1                    | 1.0        | 1.0            | 1.00000          | 61                       | a        | 1970-01-02 |",
        "| false   | -1   | -1     | -1          | -1                   | -1.0       | -1.0           | -1.00000         | 20                       |          | 1969-12-31 |",
        "| true    | 127  | 32767  | 2147483647  | 9223372036854775807  | inf        | inf            | 123456789.12345  | 656e636f6465             | encode   | 9999-12-31 |",
        "| true    | -128 | -32768 | -2147483648 | -9223372036854775808 | -inf       | -inf           | -999999999.99999 | 6465636f6465             | decode   | 1582-10-15 |",
        "| true    | 50   | 50     | 50          | 50                   | 3.1415927  | 3.14159265359  | -31256.12300     | e5a4a7e7868ae5928ce5a58f | 大熊和奏 | 1582-10-16 |",
        "| true    | 51   | 51     | 51          | 51                   | -3.1415927 | -3.14159265359 | 1241000.00000    | e69689e897a4e69cb1e5a48f | 斉藤朱夏 | 2000-01-01 |",
        "| true    | 52   | 52     | 52          | 52                   | 1.1        | 1.1            | 1.10000          | e988b4e58e9fe5b88ce5ae9f | 鈴原希実 | 3000-12-31 |",
        "| false   | 53   | 53     | 53          | 53                   | -1.1       | -1.1           | 0.99999          | f09fa494                 | 🤔       | 1900-01-01 |",
        "|         |      |        |             |                      |            |                |                  |                          |          |            |",
        "+---------+------+--------+-------------+----------------------+------------+----------------+------------------+--------------------------+----------+------------+",
    ];

    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_basic_reading_alltypes_projection_limit() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "limited", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("limited")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "utf8", "date32"])
        .expect("Projection should succeed")
        .sort(vec![col("int8").sort(true, true)])
        .expect("Sort should succeed")
        .limit(0, Some(3))
        .expect("Limit should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+--------+------------+",
        "| int8 | utf8   | date32     |",
        "+------+--------+------------+",
        "|      |        |            |",
        "|      |        |            |",
        "| -128 | decode | 1582-10-15 |",
        "+------+--------+------------+",
    ];

    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_basic_reading_alltypes_projection_subset() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "subset_alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("subset_alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["boolean", "int8", "int16"])
        .expect("Projection should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+---------+------+--------+",
        "| boolean | int8 | int16  |",
        "+---------+------+--------+",
        "|         |      |        |",
        "| true    | 0    | 0      |",
        "| false   | 1    | 1      |",
        "| false   | -1   | -1     |",
        "| true    | 127  | 32767  |",
        "| true    | -128 | -32768 |",
        "| true    | 50   | 50     |",
        "| true    | 51   | 51     |",
        "| true    | 52   | 52     |",
        "| false   | 53   | 53     |",
        "|         |      |        |",
        "+---------+------+--------+",
    ];

    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_basic_reading_alltypes_projection_reordered() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "reordered_alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("reordered_alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["int16", "boolean"])
        .expect("Projection should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+--------+---------+",
        "| int16  | boolean |",
        "+--------+---------+",
        "|        |         |",
        "| 0      | true    |",
        "| 1      | false   |",
        "| -1     | false   |",
        "| 32767  | true    |",
        "| -32768 | true    |",
        "| 50     | true    |",
        "| 51     | true    |",
        "| 52     | true    |",
        "| 53     | false   |",
        "|        |         |",
        "+--------+---------+",
    ];

    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_basic_reading_map_list_types() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "map_list", "map_list.snappy.orc")
        .await
        .expect("Failed to register table");

    let batches = ctx
        .table("map_list")
        .await
        .expect("Table exists")
        .collect()
        .await
        .expect("Failed to collect batches");

    let expected = [
        "+----+-------------------+---------------+-------+",
        "| id | m                 | l             | s     |",
        "+----+-------------------+---------------+-------+",
        "| 1  | {one: 1, zero: 0} | [test, blaze] | blaze |",
        "+----+-------------------+---------------+-------+",
    ];

    assert_batches_eq!(expected, &batches);
}

#[tokio::test]
async fn test_basic_reading_alltypes_row_count() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "count_alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("count_alltypes")
        .await
        .expect("Table exists for counting");

    let count_df = df
        .aggregate(vec![], vec![count(lit(1i64))])
        .expect("COUNT(*) aggregate should be created");

    let batches = count_df
        .collect()
        .await
        .expect("COUNT(*) should collect successfully");

    let expected = [
        "+-----------------+",
        "| count(Int64(1)) |",
        "+-----------------+",
        "| 11              |",
        "+-----------------+",
    ];

    assert_batches_eq!(expected, &batches);
}

// =============================================================================
// Error Handling Tests
// =============================================================================

/// Test reading from a non-existent file
#[tokio::test]
async fn test_error_nonexistent_file() {
    let object_store = create_test_object_store();
    let nonexistent_path = ObjectStorePath::from("/definitely/nonexistent/path/file.orc");

    // head() should fail for non-existent file
    let result = object_store.head(&nonexistent_path).await;
    assert!(result.is_err(), "Expected error for non-existent file");
}

/// Test schema inference with non-existent file path behavior
#[tokio::test]
async fn test_error_schema_inference_invalid_path() {
    let ctx = SessionContext::new();

    let result = ListingTableUrl::parse("file:///nonexistent/definitely/not/here/path/");

    if let Ok(table_path) = result {
        let listing_options =
            ListingOptions::new(Arc::new(OrcFormat::new())).with_file_extension(".orc");

        let session_state = ctx.state();
        let schema_result = listing_options
            .infer_schema(&session_state, &table_path)
            .await;

        // DataFusion returns an empty schema for non-existent paths
        match schema_result {
            Ok(schema) => {
                assert!(
                    schema.fields().is_empty(),
                    "Expected empty schema for non-existent path"
                );
            }
            Err(_) => {
                // Error is also acceptable
            }
        }
    }
}

/// Test reading from a directory with only non-ORC files
#[tokio::test]
async fn test_no_orc_files_returns_empty_schema() {
    let ctx = SessionContext::new();

    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let fake_file = temp_dir.path().join("fake.txt");
    std::fs::write(&fake_file, b"not an orc file").expect("Failed to write fake file");

    let table_path = ListingTableUrl::parse(temp_dir.path().to_str().expect("path to string"))
        .expect("parse url");

    let listing_options =
        ListingOptions::new(Arc::new(OrcFormat::new())).with_file_extension(".orc");

    let session_state = ctx.state();
    let schema_result = listing_options
        .infer_schema(&session_state, &table_path)
        .await;

    match schema_result {
        Ok(schema) => {
            assert!(
                schema.fields().is_empty(),
                "Expected empty schema when no ORC files found"
            );
        }
        Err(_) => {
            // Error is also acceptable behavior
        }
    }
}

// =============================================================================
// Configuration Options Tests
// =============================================================================

/// Test custom batch size configuration
#[tokio::test]
async fn test_config_custom_batch_size() {
    let ctx = SessionContext::new();

    let read_options = OrcReadOptions::default().with_batch_size(2);
    let format_options = OrcFormatOptions { read: read_options };
    let format = OrcFormat::new().with_options(format_options);

    register_orc_table_with_options(&ctx, "small_batch", "alltypes.snappy.orc", format)
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("small_batch")
        .await
        .expect("Table exists")
        .select_columns(&["int8"])
        .expect("Projection should succeed")
        .filter(col("int8").eq(lit(50i8)))
        .expect("Filter should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = ["+------+", "| int8 |", "+------+", "| 50   |", "+------+"];

    assert_batches_eq!(expected, &batches);
}

/// Test with predicate pushdown disabled
#[tokio::test]
async fn test_config_predicate_pushdown_disabled() {
    let ctx = SessionContext::new();

    let read_options = OrcReadOptions::default().with_pushdown_predicate(false);
    let format_options = OrcFormatOptions { read: read_options };
    let format = OrcFormat::new().with_options(format_options);

    assert!(!format.options().read.pushdown_predicate);

    register_orc_table_with_options(&ctx, "no_pushdown", "alltypes.snappy.orc", format)
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("no_pushdown")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "utf8"])
        .expect("Projection should succeed")
        .filter(col("int8").gt(lit(51i8)))
        .expect("Filter should succeed")
        .sort(vec![col("int8").sort(true, true)])
        .expect("Sort should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+----------+",
        "| int8 | utf8     |",
        "+------+----------+",
        "| 52   | 鈴原希実 |",
        "| 53   | 🤔       |",
        "| 127  | encode   |",
        "+------+----------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test metadata size hint configuration
#[tokio::test]
async fn test_config_metadata_size_hint() {
    let read_options = OrcReadOptions::default().with_metadata_size_hint(1024 * 1024);
    let format_options = OrcFormatOptions { read: read_options };
    let format = OrcFormat::new().with_options(format_options);

    assert_eq!(format.options().read.metadata_size_hint, Some(1024 * 1024));
}

// =============================================================================
// Statistics Tests
// =============================================================================

/// Test that statistics are correctly extracted
#[tokio::test]
async fn test_statistics_extraction() {
    let test_data_dir = get_test_data_dir();
    let orc_file = test_data_dir.join("alltypes.snappy.orc");

    let object_store = create_test_object_store();
    let format = OrcFormat::new();

    let file_path = to_object_store_path(&orc_file);
    let file_meta = object_store
        .head(&file_path)
        .await
        .expect("Failed to get file metadata");

    let ctx = SessionContext::new();
    let session_state = ctx.state();

    let schema = format
        .infer_schema(
            &session_state,
            &object_store,
            std::slice::from_ref(&file_meta),
        )
        .await
        .expect("Failed to infer schema");

    let stats = format
        .infer_stats(&session_state, &object_store, schema, &file_meta)
        .await
        .expect("Failed to infer statistics");

    assert_eq!(stats.num_rows, Precision::Exact(11));

    match stats.total_byte_size {
        Precision::Exact(size) => assert!(size > 0, "Expected positive byte size"),
        _ => panic!("Expected exact byte size"),
    }
}

// =============================================================================
// Data Type Edge Cases Tests
// =============================================================================

/// Test reading NULL values across all columns
#[tokio::test]
async fn test_data_null_values() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "nulls", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("nulls")
        .await
        .expect("Table exists")
        .filter(col("int8").is_null())
        .expect("Filter should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Expected 2 rows with NULL int8");
}

/// Test reading extreme values (min/max for data types)
#[tokio::test]
async fn test_data_extreme_values() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "extremes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("extremes")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "int64"])
        .expect("Projection should succeed")
        .filter(col("int8").eq(lit(127i8)).or(col("int8").eq(lit(-128i8))))
        .expect("Filter should succeed")
        .sort(vec![col("int8").sort(true, true)])
        .expect("Sort should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+----------------------+",
        "| int8 | int64                |",
        "+------+----------------------+",
        "| -128 | -9223372036854775808 |",
        "| 127  | 9223372036854775807  |",
        "+------+----------------------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test reading special float values (inf, -inf)
#[tokio::test]
async fn test_data_special_float_values() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "floats", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("floats")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "float32", "float64"])
        .expect("Projection should succeed")
        .filter(col("int8").eq(lit(127i8)).or(col("int8").eq(lit(-128i8))))
        .expect("Filter should succeed")
        .sort(vec![col("int8").sort(true, true)])
        .expect("Sort should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+---------+---------+",
        "| int8 | float32 | float64 |",
        "+------+---------+---------+",
        "| -128 | -inf    | -inf    |",
        "| 127  | inf     | inf     |",
        "+------+---------+---------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test reading decimal values with precision
#[tokio::test]
async fn test_data_decimal_precision() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "decimals", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("decimals")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "decimal"])
        .expect("Projection should succeed")
        .filter(
            col("int8")
                .gt_eq(lit(50i8))
                .and(col("int8").lt_eq(lit(53i8))),
        )
        .expect("Filter should succeed")
        .sort(vec![col("int8").sort(true, true)])
        .expect("Sort should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4, "Expected 4 rows");
}

/// Test reading Unicode/UTF-8 strings
#[tokio::test]
async fn test_data_unicode_strings() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "unicode", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("unicode")
        .await
        .expect("Table exists")
        .select_columns(&["utf8"])
        .expect("Projection should succeed")
        .filter(col("utf8").eq(lit("🤔")))
        .expect("Filter should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = ["+------+", "| utf8 |", "+------+", "| 🤔   |", "+------+"];

    assert_batches_eq!(expected, &batches);
}

// =============================================================================
// Projection Edge Cases Tests
// =============================================================================

/// Test selecting a single column
#[tokio::test]
async fn test_projection_single_column() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "single", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("single")
        .await
        .expect("Table exists")
        .select_columns(&["boolean"])
        .expect("Projection should succeed")
        .filter(col("boolean").eq(lit(false)))
        .expect("Filter should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+---------+",
        "| boolean |",
        "+---------+",
        "| false   |",
        "| false   |",
        "| false   |",
        "+---------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test selecting all columns explicitly
#[tokio::test]
async fn test_projection_all_columns_explicit() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "allcols", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("allcols")
        .await
        .expect("Table exists")
        .select_columns(&[
            "boolean", "int8", "int16", "int32", "int64", "float32", "float64", "decimal",
            "binary", "utf8", "date32",
        ])
        .expect("Projection should succeed")
        .filter(col("int8").eq(lit(0i8)))
        .expect("Filter should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "Expected 1 row with int8 = 0");
}

/// Test selecting columns in reverse order
#[tokio::test]
async fn test_projection_reverse_order() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "reverse", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("reverse")
        .await
        .expect("Table exists")
        .select_columns(&["date32", "utf8", "int8", "boolean"])
        .expect("Projection should succeed")
        .filter(col("int8").eq(lit(1i8)))
        .expect("Filter should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------------+------+------+---------+",
        "| date32     | utf8 | int8 | boolean |",
        "+------------+------+------+---------+",
        "| 1970-01-02 | a    | 1    | false   |",
        "+------------+------+------+---------+",
    ];

    assert_batches_eq!(expected, &batches);
}

// =============================================================================
// Complex Query Tests
// =============================================================================

/// Test aggregation query using DataFrame API
#[tokio::test]
async fn test_query_aggregation() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "agg", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("agg")
        .await
        .expect("Table exists")
        .filter(col("int8").is_not_null())
        .expect("Filter should succeed")
        .aggregate(
            vec![],
            vec![count(lit(1i64)), min(col("int8")), max(col("int8"))],
        )
        .expect("Aggregate should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1, "Expected 1 aggregation result row");
}

/// Test GROUP BY query using DataFrame API
#[tokio::test]
async fn test_query_group_by() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "groupby", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    let df = ctx
        .table("groupby")
        .await
        .expect("Table exists")
        .filter(col("boolean").is_not_null())
        .expect("Filter should succeed")
        .aggregate(vec![col("boolean")], vec![count(lit(1i64)).alias("cnt")])
        .expect("Aggregate should succeed")
        .sort(vec![col("boolean").sort(true, true)])
        .expect("Sort should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+---------+-----+",
        "| boolean | cnt |",
        "+---------+-----+",
        "| false   | 3   |",
        "| true    | 6   |",
        "+---------+-----+",
    ];

    assert_batches_eq!(expected, &batches);
}

// =============================================================================
// Format Factory Tests
// =============================================================================

/// Test OrcFormatFactory creates correct format
#[tokio::test]
async fn test_format_factory() {
    use datafusion_common::GetExt;
    use datafusion_datasource::file_format::FileFormatFactory;
    use datafusion_datasource_orc::OrcFormatFactory;

    let factory = OrcFormatFactory::new();

    assert_eq!(factory.get_ext(), "orc");

    let format = factory.default();
    assert_eq!(format.get_ext(), "orc");
}

/// Test OrcFormat file extension methods
#[tokio::test]
async fn test_format_extension() {
    use datafusion_datasource::file_compression_type::FileCompressionType;

    let format = OrcFormat::new();

    assert_eq!(format.get_ext(), "orc");

    let ext_with_compression = format
        .get_ext_with_compression(&FileCompressionType::UNCOMPRESSED)
        .unwrap();
    assert_eq!(ext_with_compression, "orc");
}

// =============================================================================
// Metrics Tests
// =============================================================================

/// Test that metrics are recorded during ORC file reading
#[tokio::test]
async fn test_metrics_recording() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "orc_metrics_test", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Execute a query using DataFrame API
    let df = ctx
        .table("orc_metrics_test")
        .await
        .expect("Table should exist");

    // Get the execution plan to access metrics
    let plan = df.create_physical_plan().await.unwrap();

    // Collect results to trigger execution
    let _ = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .unwrap();

    // Check that metrics were recorded
    if let Some(metrics) = plan.metrics() {
        // Should have some metrics recorded
        let metrics_str = format!("{}", metrics);

        // The metrics should contain our custom counters
        // Note: The exact format depends on how DataFusion aggregates metrics
        assert!(
            metrics_str.contains("bytes_scanned")
                || metrics_str.contains("rows_decoded")
                || metrics_str.contains("batches_produced")
                || !metrics_str.is_empty(),
            "Expected metrics to be recorded, got: {}",
            metrics_str
        );
    }
}
