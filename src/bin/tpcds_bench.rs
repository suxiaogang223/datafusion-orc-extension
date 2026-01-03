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

//! TPC-DS Benchmark Runner for ORC Datasource
//!
//! This tool runs TPC-DS queries against ORC format data files and collects
//! performance metrics. It is designed to mirror the DataFusion's TPC-DS
//! benchmark runner but uses ORC format instead of Parquet.
//!
//! # Usage
//!
//! ```bash
//! cargo run --release --bin tpcds_bench -- \
//!     --path /path/to/tpcds_sf1_orc \
//!     --query-path /path/to/tpc-ds/queries \
//!     --iterations 5 \
//!     --output results.json
//! ```

use std::fs;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::{self, pretty_format_batches};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result;
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::{
    FairSpillPool, GreedyMemoryPool, MemoryPool, TrackConsumersPool,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::*;
use datafusion_common::instant::Instant;
use datafusion_common::utils::get_available_parallelism;
use datafusion_common::{plan_err, DataFusionError};
use datafusion_datasource_orc::OrcFormat;
use log::info;
use serde::{Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;
use structopt::StructOpt;

// hack to avoid `default_value is meaningless for bool` errors
type BoolDefaultTrue = bool;

pub const TPCDS_QUERY_START_ID: usize = 1;
pub const TPCDS_QUERY_END_ID: usize = 99;

pub const TPCDS_TABLES: &[&str] = &[
    "call_center",
    "customer_address",
    "household_demographics",
    "promotion",
    "store_sales",
    "web_page",
    "catalog_page",
    "customer_demographics",
    "income_band",
    "reason",
    "store",
    "web_returns",
    "catalog_returns",
    "customer",
    "inventory",
    "ship_mode",
    "time_dim",
    "web_sales",
    "catalog_sales",
    "date_dim",
    "item",
    "store_returns",
    "warehouse",
    "web_site",
];

// ============================================================================
// Serialization helpers
// ============================================================================

fn serialize_start_time<S>(start_time: &SystemTime, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    ser.serialize_u64(
        start_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("current time is later than UNIX_EPOCH")
            .as_secs(),
    )
}

fn serialize_elapsed<S>(elapsed: &Duration, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let ms = elapsed.as_secs_f64() * 1000.0;
    ser.serialize_f64(ms)
}

// ============================================================================
// Benchmark data structures
// ============================================================================

#[derive(Debug, Serialize)]
pub struct RunContext {
    /// Benchmark crate version
    pub benchmark_version: String,
    /// DataFusion crate version
    pub datafusion_version: String,
    /// Number of CPU cores
    pub num_cpus: usize,
    /// Start time
    #[serde(serialize_with = "serialize_start_time")]
    pub start_time: SystemTime,
    /// CLI arguments
    pub arguments: Vec<String>,
}

impl Default for RunContext {
    fn default() -> Self {
        Self::new()
    }
}

impl RunContext {
    pub fn new() -> Self {
        Self {
            benchmark_version: env!("CARGO_PKG_VERSION").to_owned(),
            datafusion_version: datafusion::DATAFUSION_VERSION.to_owned(),
            num_cpus: get_available_parallelism(),
            start_time: SystemTime::now(),
            arguments: std::env::args().skip(1).collect::<Vec<String>>(),
        }
    }
}

/// A single iteration of a benchmark query
#[derive(Debug, Serialize)]
struct QueryIter {
    #[serde(serialize_with = "serialize_elapsed")]
    elapsed: Duration,
    row_count: usize,
}

/// A single benchmark case
#[derive(Debug, Serialize)]
pub struct BenchQuery {
    query: String,
    iterations: Vec<QueryIter>,
    #[serde(serialize_with = "serialize_start_time")]
    start_time: SystemTime,
    success: bool,
}

/// Internal representation of a single benchmark query iteration result.
pub struct QueryResult {
    pub elapsed: Duration,
    pub row_count: usize,
}

/// Collects benchmark run data and then serializes it at the end
pub struct BenchmarkRun {
    context: RunContext,
    queries: Vec<BenchQuery>,
    current_case: Option<usize>,
}

impl Default for BenchmarkRun {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchmarkRun {
    pub fn new() -> Self {
        Self {
            context: RunContext::new(),
            queries: vec![],
            current_case: None,
        }
    }

    /// Begin a new case. iterations added after this will be included in the new case
    pub fn start_new_case(&mut self, id: &str) {
        self.queries.push(BenchQuery {
            query: id.to_owned(),
            iterations: vec![],
            start_time: SystemTime::now(),
            success: true,
        });
        if let Some(c) = self.current_case.as_mut() {
            *c += 1;
        } else {
            self.current_case = Some(0);
        }
    }

    /// Write a new iteration to the current case
    pub fn write_iter(&mut self, elapsed: Duration, row_count: usize) {
        if let Some(idx) = self.current_case {
            self.queries[idx]
                .iterations
                .push(QueryIter { elapsed, row_count })
        } else {
            panic!("no cases existed yet");
        }
    }

    /// Print the names of failed queries, if any
    pub fn maybe_print_failures(&self) {
        let failed_queries: Vec<&str> = self
            .queries
            .iter()
            .filter_map(|q| (!q.success).then_some(q.query.as_str()))
            .collect();

        if !failed_queries.is_empty() {
            println!("Failed Queries: {}", failed_queries.join(", "));
        }
    }

    /// Mark current query as failed
    pub fn mark_failed(&mut self) {
        if let Some(idx) = self.current_case {
            self.queries[idx].success = false;
        } else {
            unreachable!("Cannot mark failure: no current case");
        }
    }

    /// Stringify data into formatted json
    pub fn to_json(&self) -> String {
        let mut output = HashMap::<&str, Value>::new();
        output.insert("context", serde_json::to_value(&self.context).unwrap());
        output.insert("queries", serde_json::to_value(&self.queries).unwrap());
        serde_json::to_string_pretty(&output).unwrap()
    }

    /// Write data as json into output path if it exists.
    pub fn maybe_write_json(&self, maybe_path: Option<&PathBuf>) -> Result<()> {
        if let Some(path) = maybe_path {
            std::fs::write(path, self.to_json())?;
        };
        Ok(())
    }
}

// ============================================================================
// Common options
// ============================================================================

/// Common benchmark options
#[derive(Debug, StructOpt, Clone)]
pub struct CommonOpt {
    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    pub iterations: usize,

    /// Number of partitions to process in parallel. Defaults to number of available cores.
    #[structopt(short = "n", long = "partitions")]
    pub partitions: Option<usize>,

    /// Batch size when reading ORC files
    #[structopt(short = "s", long = "batch-size")]
    pub batch_size: Option<usize>,

    /// The memory pool type to use, should be one of "fair" or "greedy"
    #[structopt(long = "mem-pool-type", default_value = "fair")]
    pub mem_pool_type: String,

    /// Memory limit (e.g. '100M', '1.5G'). If not specified, run with no memory limit.
    #[structopt(long = "memory-limit", parse(try_from_str = parse_memory_limit))]
    pub memory_limit: Option<usize>,

    /// Activate debug mode to see more details
    #[structopt(short, long)]
    pub debug: bool,
}

impl CommonOpt {
    /// Return an appropriately configured `SessionConfig`
    pub fn config(&self) -> Result<SessionConfig> {
        SessionConfig::from_env().map(|config| self.update_config(config))
    }

    /// Modify the existing config appropriately
    pub fn update_config(&self, mut config: SessionConfig) -> SessionConfig {
        if let Some(batch_size) = self.batch_size {
            config = config.with_batch_size(batch_size);
        }

        if let Some(partitions) = self.partitions {
            config = config.with_target_partitions(partitions);
        }

        config
    }

    /// Return an appropriately configured `RuntimeEnvBuilder`
    pub fn runtime_env_builder(&self) -> Result<RuntimeEnvBuilder> {
        let mut rt_builder = RuntimeEnvBuilder::new();
        const NUM_TRACKED_CONSUMERS: usize = 5;
        if let Some(memory_limit) = self.memory_limit {
            let pool: Arc<dyn MemoryPool> = match self.mem_pool_type.as_str() {
                "fair" => Arc::new(TrackConsumersPool::new(
                    FairSpillPool::new(memory_limit),
                    NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                )),
                "greedy" => Arc::new(TrackConsumersPool::new(
                    GreedyMemoryPool::new(memory_limit),
                    NonZeroUsize::new(NUM_TRACKED_CONSUMERS).unwrap(),
                )),
                _ => {
                    return Err(DataFusionError::Configuration(format!(
                        "Invalid memory pool type: {}",
                        self.mem_pool_type
                    )));
                }
            };
            rt_builder = rt_builder
                .with_memory_pool(pool)
                .with_disk_manager_builder(DiskManagerBuilder::default());
        }
        Ok(rt_builder)
    }
}

/// Parse memory limit from string to number of bytes
fn parse_memory_limit(limit: &str) -> Result<usize, String> {
    let (number, unit) = limit.split_at(limit.len() - 1);
    let number: f64 = number
        .parse()
        .map_err(|_| format!("Failed to parse number from memory limit '{limit}'"))?;

    match unit {
        "K" => Ok((number * 1024.0) as usize),
        "M" => Ok((number * 1024.0 * 1024.0) as usize),
        "G" => Ok((number * 1024.0 * 1024.0 * 1024.0) as usize),
        _ => Err(format!(
            "Unsupported unit '{unit}' in memory limit '{limit}'"
        )),
    }
}

// ============================================================================
// TPC-DS Runner
// ============================================================================

/// Get the SQL statements from the specified query file
pub fn get_query_sql(base_query_path: &str, query: usize) -> Result<Vec<String>> {
    if query > 0 && query < 100 {
        let filename = format!("{base_query_path}/{query}.sql");
        let mut errors = vec![];
        match fs::read_to_string(&filename) {
            Ok(contents) => {
                return Ok(contents
                    .split(';')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
                    .collect());
            }
            Err(e) => errors.push(format!("{filename}: {e}")),
        };

        plan_err!("invalid query. Could not find query: {:?}", errors)
    } else {
        plan_err!("invalid query. Expected value between 1 and 99")
    }
}

/// Run the TPC-DS benchmark with ORC format.
#[derive(Debug, StructOpt, Clone)]
#[structopt(
    name = "tpcds_bench",
    about = "TPC-DS benchmark runner for ORC datasource"
)]
pub struct RunOpt {
    /// Query number. If not specified, runs all queries
    #[structopt(short, long)]
    pub query: Option<usize>,

    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Path to data files (ORC format)
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// Path to query files
    #[structopt(parse(from_os_str), required = true, short = "Q", long = "query-path")]
    query_path: PathBuf,

    /// Load the data into a MemTable before executing the query
    #[structopt(short = "m", long = "mem-table")]
    mem_table: bool,

    /// Path to machine readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,

    /// Whether to disable collection of statistics (and cost based optimizations) or not.
    #[structopt(short = "S", long = "disable-statistics")]
    disable_statistics: bool,

    /// If true then hash join used, if false then sort merge join
    /// True by default.
    #[structopt(short = "j", long = "prefer_hash_join", default_value = "true")]
    prefer_hash_join: BoolDefaultTrue,

    /// Mark the first column of each table as sorted in ascending order.
    #[structopt(short = "t", long = "sorted")]
    sorted: bool,
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running TPC-DS benchmark (ORC format) with the following options: {self:?}");
        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => TPCDS_QUERY_START_ID..=TPCDS_QUERY_END_ID,
        };

        let mut benchmark_run = BenchmarkRun::new();
        let mut config = self
            .common
            .config()?
            .with_collect_statistics(!self.disable_statistics);
        config.options_mut().optimizer.prefer_hash_join = self.prefer_hash_join;
        let rt_builder = self.common.runtime_env_builder()?;
        let ctx = SessionContext::new_with_config_rt(config, rt_builder.build_arc()?);

        // Register tables
        self.register_tables(&ctx).await?;

        for query_id in query_range {
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let query_run = self.benchmark_query(query_id, &ctx).await;
            match query_run {
                Ok(query_results) => {
                    for iter in query_results {
                        benchmark_run.write_iter(iter.elapsed, iter.row_count);
                    }
                }
                Err(e) => {
                    benchmark_run.mark_failed();
                    eprintln!("Query {query_id} failed: {e}");
                }
            }
        }
        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        benchmark_run.maybe_print_failures();
        Ok(())
    }

    async fn benchmark_query(
        &self,
        query_id: usize,
        ctx: &SessionContext,
    ) -> Result<Vec<QueryResult>> {
        let mut millis = vec![];
        let mut query_results = vec![];

        let sql = &get_query_sql(self.query_path.to_str().unwrap(), query_id)?;

        if self.common.debug {
            println!("=== SQL for query {query_id} ===\n{}\n", sql.join(";\n"));
        }

        for i in 0..self.iterations() {
            let start = Instant::now();

            // query 15 is special, with 3 statements. the second statement is the one from which we
            // want to capture the results
            let mut result = vec![];

            for query in sql {
                result = self.execute_query(ctx, query).await?;
            }

            let elapsed = start.elapsed();
            let ms = elapsed.as_secs_f64() * 1000.0;
            millis.push(ms);
            info!("output:\n\n{}\n\n", pretty_format_batches(&result)?);
            let row_count = result.iter().map(|b| b.num_rows()).sum();
            println!(
                "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
            );
            query_results.push(QueryResult { elapsed, row_count });
        }

        let avg = millis.iter().sum::<f64>() / millis.len() as f64;
        println!("Query {query_id} avg time: {avg:.2} ms");

        Ok(query_results)
    }

    async fn register_tables(&self, ctx: &SessionContext) -> Result<()> {
        for table in TPCDS_TABLES {
            let table_provider = { self.get_table(ctx, table).await? };

            if self.mem_table {
                println!("Loading table '{table}' into memory");
                let start = Instant::now();
                let memtable =
                    MemTable::load(table_provider, Some(self.partitions()), &ctx.state()).await?;
                println!(
                    "Loaded table '{}' into memory in {} ms",
                    table,
                    start.elapsed().as_millis()
                );
                ctx.register_table(*table, Arc::new(memtable))?;
            } else {
                ctx.register_table(*table, table_provider)?;
            }
        }
        Ok(())
    }

    async fn execute_query(&self, ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
        let debug = self.common.debug;
        let plan = ctx.sql(sql).await?;
        let (state, plan) = plan.into_parts();

        if debug {
            println!("=== Logical plan ===\n{plan}\n");
        }

        let plan = state.optimize(&plan)?;
        if debug {
            println!("=== Optimized logical plan ===\n{plan}\n");
        }
        let physical_plan = state.create_physical_plan(&plan).await?;
        if debug {
            println!(
                "=== Physical plan ===\n{}\n",
                displayable(physical_plan.as_ref()).indent(true)
            );
        }
        let result = collect(physical_plan.clone(), state.task_ctx()).await?;
        if debug {
            println!(
                "=== Physical plan with metrics ===\n{}\n",
                DisplayableExecutionPlan::with_metrics(physical_plan.as_ref()).indent(true)
            );
            if !result.is_empty() {
                pretty::print_batches(&result)?;
            }
        }
        Ok(result)
    }

    async fn get_table(&self, ctx: &SessionContext, table: &str) -> Result<Arc<dyn TableProvider>> {
        let path = self.path.to_str().unwrap();
        let target_partitions = self.partitions();

        // Obtain a snapshot of the SessionState
        let state = ctx.state();
        let path = format!("{path}/{table}.orc");

        // Check if the file exists
        if !std::path::Path::new(&path).exists() {
            eprintln!("Warning registering {table}: Table file does not exist: {path}");
        }

        // Use ORC format
        let format = OrcFormat::new();

        let table_path = ListingTableUrl::parse(&path)?;
        let options = ListingOptions::new(Arc::new(format))
            .with_file_extension(".orc")
            .with_target_partitions(target_partitions)
            .with_collect_stat(state.config().collect_statistics());
        let schema = options.infer_schema(&state, &table_path).await?;

        if self.common.debug {
            println!("Inferred schema from {table_path} for table '{table}':\n{schema:#?}\n");
        }

        let options = if self.sorted {
            let key_column_name = schema.fields()[0].name();
            options.with_file_sort_order(vec![vec![col(key_column_name).sort(true, false)]])
        } else {
            options
        };

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(schema);

        Ok(Arc::new(ListingTable::try_new(config)?))
    }

    fn iterations(&self) -> usize {
        self.common.iterations
    }

    fn partitions(&self) -> usize {
        self.common
            .partitions
            .unwrap_or_else(get_available_parallelism)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let opt = RunOpt::from_args();
    opt.run().await
}
