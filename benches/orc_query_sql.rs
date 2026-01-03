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

//! Benchmarks of SQL queries against ORC data
//!
//! This benchmark suite tests ORC datasource performance with various query patterns:
//! - Full table scans
//! - Column projection
//! - Predicate pushdown
//! - Aggregations
//! - Joins
//!
//! Uses test data files from tests/integration/data/

use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::*;
use datafusion_datasource_orc::OrcFormat;
use parking_lot::Mutex;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Get path to test data file
fn get_test_data_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("integration")
        .join("data")
        .join(file_name)
}

async fn create_context_with_orc_table(
    orc_path: &std::path::Path,
    table_name: &str,
) -> datafusion::error::Result<Arc<Mutex<SessionContext>>> {
    let ctx = SessionContext::new();

    let format = OrcFormat::new();
    let listing_options = ListingOptions::new(Arc::new(format)).with_file_extension(".orc");

    let table_path = ListingTableUrl::parse(format!("file://{}", orc_path.display()))?;
    let schema = listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await?;

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(schema);

    let table = ListingTable::try_new(config)?;
    ctx.register_table(table_name, Arc::new(table))?;

    Ok(Arc::new(Mutex::new(ctx)))
}

#[expect(clippy::needless_pass_by_value)]
fn query(ctx: Arc<Mutex<SessionContext>>, rt: &Runtime, sql: &str) {
    let df = rt.block_on(ctx.lock().sql(sql)).unwrap();
    black_box(rt.block_on(df.collect()).unwrap());
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Use existing test data file
    let orc_path = get_test_data_path("alltypes.snappy.orc");
    if !orc_path.exists() {
        eprintln!("Warning: Test data file not found: {:?}", orc_path);
        eprintln!("Skipping benchmarks. Run tests first to generate test data.");
        return;
    }

    let ctx = rt
        .block_on(create_context_with_orc_table(&orc_path, "alltypes"))
        .unwrap();

    let mut group = c.benchmark_group("orc_query_sql");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_secs(30));

    // Full table scan
    group.bench_function("full_table_scan", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT * FROM alltypes"))
    });

    // Column projection - single column
    group.bench_function("projection_single_column", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT int8 FROM alltypes"))
    });

    // Column projection - multiple columns
    group.bench_function("projection_multiple_columns", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT int8, int16, int32 FROM alltypes"))
    });

    // Predicate pushdown - equality
    group.bench_function("filter_equality", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT * FROM alltypes WHERE int8 = 0"))
    });

    // Predicate pushdown - range
    group.bench_function("filter_range", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT * FROM alltypes WHERE int32 > 0 AND int32 < 100",
            )
        })
    });

    // Predicate pushdown - null check
    group.bench_function("filter_is_null", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT * FROM alltypes WHERE int8 IS NULL",
            )
        })
    });

    // Aggregation
    group.bench_function("aggregate_count", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT COUNT(*) FROM alltypes"))
    });

    // Aggregation with filter
    group.bench_function("aggregate_with_filter", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT COUNT(*), AVG(int32) FROM alltypes WHERE int8 > 0",
            )
        })
    });

    // Limit
    group.bench_function("limit_100", |b| {
        b.iter(|| query(ctx.clone(), &rt, "SELECT * FROM alltypes LIMIT 100"))
    });

    // Projection + Filter + Limit
    group.bench_function("projection_filter_limit", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                &rt,
                "SELECT int8, int16 FROM alltypes WHERE int32 > 0 LIMIT 50",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
