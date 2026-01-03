# TPC-DS Benchmark 测试计划 (ORC 格式)

## 1. 概述

本项目提供了一个独立的 TPC-DS 基准测试工具，用于测试 DataFusion ORC datasource 的性能。该工具设计参考了 DataFusion 主仓库的 TPC-DS benchmark runner，但使用 ORC 文件格式代替 Parquet。

## 2. 工具使用

### 2.1 基本用法

```bash
# 运行单个查询
cargo run --release --bin tpcds_bench -- \
    --path /path/to/tpcds_sf1_orc \
    --query-path /path/to/tpc-ds/queries \
    --query 1 \
    --iterations 3

# 运行所有查询
cargo run --release --bin tpcds_bench -- \
    --path /path/to/tpcds_sf1_orc \
    --query-path /path/to/tpc-ds/queries \
    --iterations 5 \
    --output results.json
```

### 2.2 命令行参数

| 参数 | 短选项 | 描述 | 默认值 |
|------|--------|------|--------|
| `--path` | `-p` | TPC-DS ORC 数据目录路径 | 必需 |
| `--query-path` | `-Q` | TPC-DS 查询 SQL 文件目录路径 | 必需 |
| `--query` | `-q` | 要运行的查询编号 (1-99)，不指定则运行所有查询 | 全部 |
| `--iterations` | `-i` | 每个查询的运行次数 | 3 |
| `--partitions` | `-n` | 并行分区数 | CPU 核心数 |
| `--batch-size` | `-s` | 读取 ORC 文件时的批次大小 | 默认 |
| `--mem-table` | `-m` | 将数据加载到内存表 | false |
| `--output` | `-o` | 输出 JSON 结果文件路径 | 无 |
| `--disable-statistics` | `-S` | 禁用统计信息收集 | false |
| `--prefer_hash_join` | `-j` | 优先使用 Hash Join | true |
| `--sorted` | `-t` | 标记表的第一列为已排序 | false |
| `--memory-limit` | | 内存限制 (e.g. '1G', '500M') | 无限制 |
| `--mem-pool-type` | | 内存池类型 ('fair' 或 'greedy') | fair |
| `--debug` | `-d` | 启用调试输出 | false |

### 2.3 调试模式

使用 `--debug` 参数可以查看详细的执行计划和 metrics：

```bash
cargo run --release --bin tpcds_bench -- \
    --path /path/to/tpcds_sf1_orc \
    --query-path /path/to/tpc-ds/queries \
    --query 1 \
    --debug
```

## 3. 数据准备

### 3.1 方法 1: 从 TPC-DS 工具直接生成 ORC（推荐）

这是最直接的方法，从 TPC-DS 工具生成的文本文件直接转换为 ORC。

**步骤 1: 下载并编译 TPC-DS 工具**

```bash
# 从 TPC-DS 官网下载工具
wget http://www.tpc.org/tpcds/tools/tpcds_kit.zip
unzip tpcds_kit.zip
cd tools

# 编译（需要 gcc）
make OS=LINUX
```

**步骤 2: 生成数据**

```bash
# 生成 SF1 (Scale Factor 1) 的数据
./dsdgen -scale 1 -dir /path/to/tpcds/data

# 这会生成 24 个表的 .dat 文件，例如：
# call_center.dat, customer.dat, store_sales.dat 等
```

**步骤 3: 转换为 ORC**

```bash
# 使用提供的脚本转换为 ORC
python3 scripts/generate_tpcds_orc.py \
    --from-text /path/to/tpcds/data \
    --output /path/to/tpcds_sf1_orc
```

**完整示例：**

```bash
# 1. 生成 TPC-DS 数据
cd /tmp
./dsdgen -scale 1 -dir ./tpcds_data

# 2. 转换为 ORC
python3 /path/to/datafusion-datasource-orc/scripts/generate_tpcds_orc.py \
    --from-text ./tpcds_data \
    --output ./tpcds_sf1_orc

# 3. 验证
ls -1 ./tpcds_sf1_orc/*.orc | wc -l  # 应该输出 24
```

### 3.2 方法 2: 从 Parquet 文件转换

如果你已经有 Parquet 格式的 TPC-DS 数据（例如从 datafusion-benchmarks）：

```bash
# 克隆 datafusion-benchmarks
git clone https://github.com/apache/datafusion-benchmarks
cd datafusion-benchmarks

# 转换为 ORC
python3 ../datafusion-datasource-orc/scripts/convert_tpcds_to_orc.py \
    tpcds/data/sf1 \
    /path/to/tpcds_sf1_orc
```

或者使用 `generate_tpcds_orc.py`：

```bash
python3 scripts/generate_tpcds_orc.py \
    --from-parquet /path/to/parquet/data \
    --output /path/to/orc/output
```

### 3.3 脚本选项说明

**`generate_tpcds_orc.py` 支持的选项：**

- `--from-text <dir>`: 从文本文件转换（TPC-DS 工具生成的 .dat 文件）
- `--from-parquet <dir>`: 从 Parquet 文件转换
- `--output <dir>`: ORC 输出目录
- `--delimiter <char>`: 文本文件分隔符（默认: `|`）
- `--skip-existing`: 跳过已存在的文件
- `-q, --quiet`: 静默模式

**示例：**

```bash
# 使用自定义分隔符
python3 scripts/generate_tpcds_orc.py \
    --from-text /path/to/data \
    --output /path/to/orc \
    --delimiter "\t"

# 跳过已存在的文件（用于增量更新）
python3 scripts/generate_tpcds_orc.py \
    --from-text /path/to/data \
    --output /path/to/orc \
    --skip-existing
```

### 3.4 依赖安装

```bash
# 安装 Python 依赖
pip install pyarrow pandas

# 验证安装
python3 -c "import pyarrow; print(pyarrow.__version__)"
```

### 3.5 验证生成的文件

```bash
# 检查文件数量（应该是 24 个）
ls -1 /path/to/tpcds_sf1_orc/*.orc | wc -l

# 检查文件大小
du -sh /path/to/tpcds_sf1_orc/

# 使用 PyArrow 验证文件
python3 -c "
import pyarrow.orc as orc
table = orc.read_table('/path/to/tpcds_sf1_orc/customer.orc')
print(f'Rows: {len(table)}, Columns: {len(table.column_names)}')
print(f'Schema: {table.schema}')
"
```

### 3.6 获取查询文件

TPC-DS 查询文件可以从以下位置获取：
- DataFusion 仓库: `datafusion/core/tests/tpc-ds/`
- TPC-DS 官方工具生成

```bash
# 从 DataFusion 仓库复制
cp -r /path/to/datafusion/core/tests/tpc-ds /path/to/queries
```

## 4. 输出格式

### 4.1 控制台输出

```
Running TPC-DS benchmark (ORC format) with the following options: ...
Query 1 iteration 0 took 123.4 ms and returned 100 rows
Query 1 iteration 1 took 112.3 ms and returned 100 rows
Query 1 iteration 2 took 115.6 ms and returned 100 rows
Query 1 avg time: 117.10 ms
...
```

### 4.2 JSON 输出

```json
{
  "context": {
    "benchmark_version": "0.1.0",
    "datafusion_version": "51.0.0",
    "num_cpus": 8,
    "start_time": 1704307200,
    "arguments": ["--path", "/data/tpcds", "--query-path", "/queries"]
  },
  "queries": [
    {
      "query": "Query 1",
      "iterations": [
        { "elapsed": 123.4, "row_count": 100 },
        { "elapsed": 112.3, "row_count": 100 }
      ],
      "start_time": 1704307201,
      "success": true
    }
  ]
}
```

## 5. 与 Parquet 版本对比

运行 ORC benchmark 后，可以与 DataFusion 的 Parquet TPC-DS 结果进行对比：

```bash
# 运行 Parquet 版本 (在 datafusion 仓库)
cd /path/to/datafusion/benchmarks
cargo run --release --bin dfbench -- tpcds \
    --path /path/to/tpcds_sf1 \
    --query_path ../datafusion/core/tests/tpc-ds \
    -o results_parquet.json

# 运行 ORC 版本 (在本仓库)
cd /path/to/datafusion-datasource-orc
cargo run --release --bin tpcds_bench -- \
    --path /path/to/tpcds_sf1_orc \
    --query-path /path/to/tpc-ds/queries \
    --output results_orc.json
```

## 6. 故障排查

### 问题 1: 找不到表文件

确保 ORC 文件名格式为 `{table_name}.orc`，例如 `customer.orc`。

### 问题 2: Schema 不匹配

验证 ORC 文件的 schema 与查询预期一致。

### 问题 3: 查询失败

使用 `--debug` 查看详细错误信息和执行计划。

## 7. 已知限制

1. 某些 TPC-DS 查询可能由于 DataFusion 或 ORC datasource 的限制而失败
2. 性能可能因 ORC 文件的压缩和编码设置而异
3. 统计信息收集依赖于 ORC 文件的元数据
