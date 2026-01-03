# TPC-DS ORC 数据生成脚本

本目录包含用于生成 TPC-DS ORC 文件的脚本。

## 脚本说明

### 1. `generate_tpcds_orc.py` - 从 TPC-DS 工具生成 ORC

从 TPC-DS 工具生成的文本文件或 Parquet 文件转换为 ORC 格式。

**功能：**
- 支持从 TPC-DS 文本文件（`.dat` 或 `.txt`）转换
- 支持从 Parquet 文件转换
- 自动识别表名
- 支持自定义分隔符
- 跳过已存在的文件

**使用方法：**

```bash
# 从 TPC-DS 文本文件转换（推荐）
python3 generate_tpcds_orc.py \
    --from-text /path/to/tpcds/data \
    --output /path/to/orc/output

# 从 Parquet 文件转换
python3 generate_tpcds_orc.py \
    --from-parquet /path/to/parquet/data \
    --output /path/to/orc/output

# 使用自定义分隔符（如果 TPC-DS 文件不是用 | 分隔）
python3 generate_tpcds_orc.py \
    --from-text /path/to/data \
    --output /path/to/orc \
    --delimiter "\t"

# 跳过已存在的文件
python3 generate_tpcds_orc.py \
    --from-text /path/to/data \
    --output /path/to/orc \
    --skip-existing
```

### 2. `convert_tpcds_to_orc.py` - 从 Parquet 转换

专门用于将已有的 Parquet 格式 TPC-DS 数据转换为 ORC。

**使用方法：**

```bash
python3 convert_tpcds_to_orc.py \
    /path/to/parquet/data \
    /path/to/orc/output
```

## 完整工作流程

### 方法 1: 从 TPC-DS 工具直接生成 ORC（推荐）

1. **下载并编译 TPC-DS 工具**

```bash
# 下载 TPC-DS 工具
wget http://www.tpc.org/tpcds/tools/tpcds_kit.zip
unzip tpcds_kit.zip
cd tools

# 编译（需要 gcc）
make OS=LINUX
```

2. **生成数据**

```bash
# 生成 SF1 (Scale Factor 1) 的数据
./dsdgen -scale 1 -dir /path/to/tpcds/data

# 这会生成 24 个表的 .dat 文件
```

3. **转换为 ORC**

```bash
# 使用脚本转换为 ORC
python3 generate_tpcds_orc.py \
    --from-text /path/to/tpcds/data \
    --output /path/to/tpcds_sf1_orc
```

### 方法 2: 从 datafusion-benchmarks 的 Parquet 数据转换

如果你已经有 datafusion-benchmarks 仓库的 Parquet 数据：

```bash
# 克隆 datafusion-benchmarks
git clone https://github.com/apache/datafusion-benchmarks
cd datafusion-benchmarks

# 转换为 ORC
python3 ../datafusion-datasource-orc/scripts/convert_tpcds_to_orc.py \
    tpcds/data/sf1 \
    /path/to/tpcds_sf1_orc
```

### 方法 3: 使用 DataFusion 转换（如果支持）

如果 DataFusion 支持 ORC 写入，可以使用 SQL：

```sql
CREATE EXTERNAL TABLE parquet_table STORED AS PARQUET 
LOCATION '/path/to/parquet';

CREATE EXTERNAL TABLE orc_table STORED AS ORC 
LOCATION '/path/to/orc';

INSERT INTO orc_table SELECT * FROM parquet_table;
```

## 依赖安装

```bash
# 安装 Python 依赖
pip install pyarrow pandas

# 验证安装
python3 -c "import pyarrow; print(pyarrow.__version__)"
```

## 验证生成的文件

```bash
# 检查文件数量（应该是 24 个）
ls -1 /path/to/orc/*.orc | wc -l

# 检查文件大小
du -sh /path/to/orc/

# 使用 PyArrow 验证文件
python3 -c "
import pyarrow.orc as orc
table = orc.read_table('/path/to/orc/customer.orc')
print(f'Rows: {len(table)}, Columns: {len(table.column_names)}')
print(f'Schema: {table.schema}')
"
```

## 常见问题

### Q: TPC-DS 工具在哪里下载？

A: 从 [TPC-DS 官网](http://www.tpc.org/tpcds/) 下载工具包。

### Q: 文本文件的分隔符是什么？

A: TPC-DS 工具默认生成管道符（`|`）分隔的文件。如果不同，使用 `--delimiter` 参数。

### Q: 转换后的文件大小如何？

A: ORC 文件通常比未压缩的文本文件小很多，但可能比 Parquet 文件稍大或稍小，取决于压缩设置。

### Q: 支持哪些数据类型？

A: 脚本会自动推断类型。对于 Decimal 类型，会尽量保持精度。

## 性能提示

1. **批量转换**: 脚本会顺序处理所有文件，对于大量数据可能需要一些时间
2. **内存使用**: 大文件会一次性加载到内存，确保有足够内存
3. **并行处理**: 可以手动并行运行多个转换任务（每个表一个进程）

## 下一步

生成 ORC 文件后，可以使用 TPC-DS benchmark 工具测试：

```bash
cargo run --release --bin tpcds_bench -- \
    --path /path/to/tpcds_sf1_orc \
    --query-path /path/to/tpc-ds/queries \
    --iterations 5 \
    --output results.json
```

