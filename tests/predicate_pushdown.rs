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

//! Integration tests for ORC predicate pushdown functionality
//!
//! These tests verify that filter predicates are correctly converted
//! to ORC predicates and applied during file reads.

use datafusion::assert_batches_eq;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use datafusion_orc_extension::OrcFormat;
use std::path::PathBuf;
use std::sync::Arc;

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

/// Test filtering with equality predicate (=)
#[tokio::test]
async fn test_filter_equality() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter: int8 = 50
    let df = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "utf8"])
        .expect("Projection should succeed")
        .filter(col("int8").eq(lit(50i8)))
        .expect("Filter should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+----------+",
        "| int8 | utf8     |",
        "+------+----------+",
        "| 50   | 大熊和奏 |",
        "+------+----------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test filtering with greater than predicate (>)
#[tokio::test]
async fn test_filter_greater_than() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter: int8 > 51
    let df = ctx
        .table("alltypes")
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

/// Test filtering with less than predicate (<)
#[tokio::test]
async fn test_filter_less_than() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter: int8 < 0 (exclude NULLs and positive values)
    let df = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "utf8"])
        .expect("Projection should succeed")
        .filter(col("int8").lt(lit(0i8)))
        .expect("Filter should succeed")
        .sort(vec![col("int8").sort(true, true)])
        .expect("Sort should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+--------+",
        "| int8 | utf8   |",
        "+------+--------+",
        "| -128 | decode |",
        "| -1   |        |",
        "+------+--------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test filtering with compound AND predicate
#[tokio::test]
async fn test_filter_and_compound() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter: int8 >= 50 AND int8 <= 52
    let df = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "utf8"])
        .expect("Projection should succeed")
        .filter(
            col("int8")
                .gt_eq(lit(50i8))
                .and(col("int8").lt_eq(lit(52i8))),
        )
        .expect("Filter should succeed")
        .sort(vec![col("int8").sort(true, true)])
        .expect("Sort should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+----------+",
        "| int8 | utf8     |",
        "+------+----------+",
        "| 50   | 大熊和奏 |",
        "| 51   | 斉藤朱夏 |",
        "| 52   | 鈴原希実 |",
        "+------+----------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test filtering with OR predicate
#[tokio::test]
async fn test_filter_or_predicate() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter: int8 = 0 OR int8 = 1
    let df = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "utf8"])
        .expect("Projection should succeed")
        .filter(col("int8").eq(lit(0i8)).or(col("int8").eq(lit(1i8))))
        .expect("Filter should succeed")
        .sort(vec![col("int8").sort(true, true)])
        .expect("Sort should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+------+",
        "| int8 | utf8 |",
        "+------+------+",
        "| 0    |      |",
        "| 1    | a    |",
        "+------+------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test filtering with IS NULL predicate
#[tokio::test]
async fn test_filter_is_null() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter: int8 IS NULL
    let df = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "utf8"])
        .expect("Projection should succeed")
        .filter(col("int8").is_null())
        .expect("Filter should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+------+",
        "| int8 | utf8 |",
        "+------+------+",
        "|      |      |",
        "|      |      |",
        "+------+------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test filtering with IS NOT NULL predicate
#[tokio::test]
async fn test_filter_is_not_null() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter: int8 IS NOT NULL (and order by int8 to get predictable results)
    let df = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "utf8"])
        .expect("Projection should succeed")
        .filter(col("int8").is_not_null())
        .expect("Filter should succeed")
        .sort(vec![col("int8").sort(true, true)])
        .expect("Sort should succeed")
        .limit(0, Some(3))
        .expect("Limit should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    // First 3 non-null int8 values
    let expected = [
        "+------+--------+",
        "| int8 | utf8   |",
        "+------+--------+",
        "| -128 | decode |",
        "| -1   |        |",
        "| 0    |        |",
        "+------+--------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test filtering with string equality
#[tokio::test]
async fn test_filter_string_equality() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter: utf8 = 'encode'
    let df = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "utf8"])
        .expect("Projection should succeed")
        .filter(col("utf8").eq(lit("encode")))
        .expect("Filter should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+--------+",
        "| int8 | utf8   |",
        "+------+--------+",
        "| 127  | encode |",
        "+------+--------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test filtering with boolean predicate
#[tokio::test]
async fn test_filter_boolean() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter: boolean = false
    let df = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "boolean"])
        .expect("Projection should succeed")
        .filter(col("boolean").eq(lit(false)))
        .expect("Filter should succeed")
        .sort(vec![col("int8").sort(true, true)])
        .expect("Sort should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+---------+",
        "| int8 | boolean |",
        "+------+---------+",
        "| -1   | false   |",
        "| 1    | false   |",
        "| 53   | false   |",
        "+------+---------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test filtering with float comparison
#[tokio::test]
async fn test_filter_float() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter: float64 > 3.0 AND float64 < 4.0
    let df = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["int8", "float64"])
        .expect("Projection should succeed")
        .filter(
            col("float64")
                .gt(lit(3.0f64))
                .and(col("float64").lt(lit(4.0f64))),
        )
        .expect("Filter should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+---------------+",
        "| int8 | float64       |",
        "+------+---------------+",
        "| 50   | 3.14159265359 |",
        "+------+---------------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test filtering with NOT equality
#[tokio::test]
async fn test_filter_not_equal() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter: int8 != 50 AND int8 IS NOT NULL (take first 3 ordered by int8)
    let df = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["int8"])
        .expect("Projection should succeed")
        .filter(col("int8").not_eq(lit(50i8)).and(col("int8").is_not_null()))
        .expect("Filter should succeed")
        .sort(vec![col("int8").sort(true, true)])
        .expect("Sort should succeed")
        .limit(0, Some(3))
        .expect("Limit should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+", "| int8 |", "+------+", "| -128 |", "| -1   |", "| 0    |", "+------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test that filter with projection works correctly
#[tokio::test]
async fn test_filter_with_projection() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter first, then project to a subset of columns
    // int8 > 50 includes values 51, 52, 53, 127
    let df = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .filter(col("int8").gt(lit(50i8)))
        .expect("Filter should succeed")
        .select_columns(&["utf8", "date32"])
        .expect("Projection should succeed")
        .sort(vec![col("utf8").sort(true, true)])
        .expect("Sort should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    // Results sorted by utf8: encode, 斉藤朱夏, 鈴原希実, 🤔
    let expected = [
        "+----------+------------+",
        "| utf8     | date32     |",
        "+----------+------------+",
        "| encode   | 9999-12-31 |",
        "| 斉藤朱夏 | 2000-01-01 |",
        "| 鈴原希実 | 3000-12-31 |",
        "| 🤔       | 1900-01-01 |",
        "+----------+------------+",
    ];

    assert_batches_eq!(expected, &batches);
}

/// Test that filter with LIMIT works correctly
#[tokio::test]
async fn test_filter_with_limit() {
    let ctx = SessionContext::new();
    register_orc_table(&ctx, "alltypes", "alltypes.snappy.orc")
        .await
        .expect("Failed to register table");

    // Filter and limit results
    let df = ctx
        .table("alltypes")
        .await
        .expect("Table exists")
        .select_columns(&["int8"])
        .expect("Projection should succeed")
        .filter(col("int8").gt(lit(0i8)))
        .expect("Filter should succeed")
        .sort(vec![col("int8").sort(true, true)])
        .expect("Sort should succeed")
        .limit(0, Some(2))
        .expect("Limit should succeed");

    let batches = df.collect().await.expect("Failed to collect batches");

    let expected = [
        "+------+", "| int8 |", "+------+", "| 1    |", "| 50   |", "+------+",
    ];

    assert_batches_eq!(expected, &batches);
}
