# Cargo Bench 集成说明

## 概述

本项目集成了 `cargo bench` 支持，可以方便地运行 ORC datasource 的性能基准测试。

## DataFusion 的 Cargo Bench

DataFusion 主仓库使用 `criterion` 框架进行 benchmark，主要测试以下方面：

### DataFusion Core Benchmarks

位于 `datafusion/core/benches/`：

1. **`aggregate_query_sql.rs`** - 聚合查询性能
   - COUNT, SUM, AVG, MIN, MAX
   - GROUP BY 性能
   - COUNT DISTINCT

2. **`csv_load.rs`** - CSV 文件加载性能
   - 默认选项
   - 自定义 null 正则表达式

3. **`parquet_query_sql.rs`** - Parquet 查询性能
   - 全表扫描
   - 列投影
   - 谓词下推
   - 聚合操作

4. **`filter_query_sql.rs`** - 过滤查询性能
   - 等值过滤
   - 范围过滤
   - 复杂条件

5. **`sort_limit_query_sql.rs`** - 排序和限制性能

6. **`window_query_sql.rs`** - 窗口函数性能

7. **`physical_plan.rs`** - 物理计划执行性能

### 其他模块的 Benchmarks

- **`datafusion/physical-plan/benches/`** - 物理计划操作
  - `spill_io.rs` - 溢出 I/O 性能
  - `sort_preserving_merge.rs` - 排序合并
  - `aggregate_vectorized.rs` - 向量化聚合

- **`datafusion/physical-expr/benches/`** - 表达式评估
  - `binary_op.rs` - 二元运算
  - `case_when.rs` - CASE WHEN 表达式
  - `in_list.rs` - IN 列表

- **`datafusion/functions/benches/`** - 函数性能
  - 各种字符串、数学、日期函数

## ORC Datasource Benchmarks

### 当前实现的 Benchmark

**`benches/orc_query_sql.rs`** - ORC 查询性能测试

测试场景：
- ✅ 全表扫描 (`full_table_scan`)
- ✅ 单列投影 (`projection_single_column`)
- ✅ 多列投影 (`projection_multiple_columns`)
- ✅ 等值过滤 (`filter_equality`)
- ✅ 范围过滤 (`filter_range`)
- ✅ NULL 检查 (`filter_is_null`)
- ✅ 聚合 (`aggregate_count`)
- ✅ 带过滤的聚合 (`aggregate_with_filter`)
- ✅ LIMIT (`limit_100`)
- ✅ 组合操作 (`projection_filter_limit`)

### 运行 Benchmarks

```bash
# 运行所有 benchmarks
cargo bench

# 运行特定的 benchmark
cargo bench --bench orc_query_sql

# 运行特定的测试函数
cargo bench --bench orc_query_sql -- full_table_scan

# 使用 release 模式（推荐，性能更准确）
cargo bench --release

# 查看详细输出
cargo bench --bench orc_query_sql -- --nocapture
```

### Benchmark 输出示例

```
orc_query_sql/full_table_scan
                        time:   [2.3456 ms 2.4567 ms 2.5678 ms]
                        change: [+1.23% +2.34% +3.45%] (p = 0.05 < 0.05)
                        Performance has regressed.

orc_query_sql/projection_single_column
                        time:   [1.1234 ms 1.2345 ms 1.3456 ms]
                        change: [-2.34% -1.23% -0.12%] (p = 0.05 < 0.05)
                        Performance has improved.
```

### 配置 Benchmark

可以在 `benches/orc_query_sql.rs` 中调整：

```rust
let mut group = c.benchmark_group("orc_query_sql");
group.sample_size(10);  // 采样次数
group.measurement_time(Duration::from_secs(30));  // 测量时间
```

## 与 TPC-DS Benchmark 的区别

| 特性 | `cargo bench` | `tpcds_bench` |
|------|---------------|---------------|
| **用途** | 单元性能测试 | 端到端基准测试 |
| **数据** | 小型测试文件 | TPC-DS 完整数据集 |
| **查询** | 简单查询模式 | 复杂业务查询 |
| **输出** | Criterion 报告 | JSON 结果文件 |
| **集成** | Cargo 标准工具 | 独立二进制 |

## 添加新的 Benchmark

### 步骤 1: 创建 Benchmark 文件

在 `benches/` 目录下创建新文件，例如 `benches/orc_reader.rs`：

```rust
extern crate criterion;

use criterion::{Criterion, criterion_group, criterion_main};

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("my_benchmark", |b| {
        b.iter(|| {
            // Your benchmark code
        });
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
```

### 步骤 2: 在 Cargo.toml 中添加配置

```toml
[[bench]]
name = "orc_reader"
harness = false
```

### 步骤 3: 运行

```bash
cargo bench --bench orc_reader
```

## 最佳实践

1. **使用 Release 模式**: `cargo bench --release` 获得准确的性能数据
2. **多次运行**: Criterion 会自动运行多次并统计
3. **比较结果**: 使用 `cargo bench --bench <name> -- --baseline <baseline>` 比较不同版本
4. **保存基线**: `cargo bench --bench <name> -- --save-baseline <name>`

## 故障排查

### 问题 1: 找不到测试数据文件

**错误**: `Warning: Test data file not found`

**解决**: 确保测试数据文件存在于 `tests/integration/data/` 目录

```bash
# 运行测试以生成数据（如果需要）
cargo test
```

### 问题 2: Benchmark 运行很慢

**解决**: 调整 `sample_size` 和 `measurement_time`：

```rust
group.sample_size(5);  // 减少采样次数
group.measurement_time(Duration::from_secs(10));  // 减少测量时间
```

### 问题 3: 结果不稳定

**解决**: 
- 确保系统负载低
- 增加采样次数
- 使用 `--profile-time` 查看详细时间分布

## 参考资源

- [Criterion.rs 文档](https://docs.rs/criterion/)
- [DataFusion Benchmarks](../datafusion/benchmarks/README.md)
- [ORC 性能指标文档](./performance_metrics.md)

