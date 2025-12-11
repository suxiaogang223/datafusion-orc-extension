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
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::prelude::*;
use datafusion_datasource::file_format::FileFormat;
use datafusion_orc_extension::OrcFormat;
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
    let file_path = get_test_data_dir().join(file_name);
    let table_path = ListingTableUrl::parse(
        file_path
            .to_str()
            .expect("Failed to convert path to string"),
    )?;

    let listing_options =
        ListingOptions::new(Arc::new(OrcFormat::new())).with_file_extension(".orc");

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
