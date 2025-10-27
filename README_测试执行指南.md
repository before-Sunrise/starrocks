# 谓词列延迟物化性能测试执行指南

## 概述

本测试方案基于StarRocks PR #64600，全面评估谓词列延迟物化功能在不同场景下的性能表现。测试覆盖了过滤性、列数量比例、数据类型等多个维度的正交组合。

## 测试环境要求

### 硬件要求
- **CPU**: 至少16核，推荐32核以上
- **内存**: 至少64GB，推荐128GB以上  
- **存储**: SSD存储，至少500GB可用空间
- **网络**: 千兆网络

### 软件要求
- **StarRocks**: 包含PR #64600的版本
- **MySQL客户端**: 用于执行SQL
- **Python 3.7+**: 用于结果分析
- **依赖包**: pandas, matplotlib, seaborn, numpy

## 文件说明

### 核心文件
- `test_predicate_late_materialization.md` - 详细测试方案文档
- `create_test_tables.sql` - 建表脚本
- `generate_test_data.sql` - 数据生成脚本  
- `performance_test_queries.sql` - 测试查询SQL
- `run_performance_test.sh` - 自动化测试执行脚本
- `analyze_test_results.py` - 结果分析脚本

### 输出文件
- `test_results/performance_test_YYYYMMDD_HHMMSS.txt` - 详细测试日志
- `test_results/performance_test_YYYYMMDD_HHMMSS.csv` - CSV格式结果
- `analysis_results/` - 分析图表和报告

## 执行步骤

### 第1步：环境准备

```bash
# 1. 确保StarRocks集群正常运行
# 2. 创建测试数据库
mysql -h127.0.0.1 -P9030 -uroot -e "CREATE DATABASE IF NOT EXISTS test_db;"

# 3. 安装Python依赖
pip install pandas matplotlib seaborn numpy
```

### 第2步：创建测试表

```bash
# 执行建表脚本
mysql -h127.0.0.1 -P9030 -uroot test_db < create_test_tables.sql
```

### 第3步：生成测试数据

```bash
# 执行数据生成脚本（耗时较长，约30-60分钟）
mysql -h127.0.0.1 -P9030 -uroot test_db < generate_test_data.sql
```

**注意**: 数据生成过程会创建1000万行 × 4个表 = 4000万行数据，请确保有足够的存储空间和时间。

### 第4步：执行性能测试

```bash
# 设置连接参数（根据实际环境调整）
export MYSQL_HOST="127.0.0.1"
export MYSQL_PORT="9030"
export MYSQL_USER="root"
export MYSQL_PASSWORD=""
export DATABASE="test_db"

# 执行自动化测试（耗时约2-4小时）
./run_performance_test.sh
```

### 第5步：分析测试结果

```bash
# 分析最新的测试结果
python analyze_test_results.py test_results/performance_test_*.csv --output-dir analysis_results
```

## 测试场景详解

### 维度组合

测试覆盖以下维度的所有组合（共12个场景）：

| 过滤性 | 列数量比例 | 数据类型 | 预期结果行数 |
|--------|------------|----------|--------------|
| 高(1%) | 少(1:10) | INT | ~10万 |
| 高(1%) | 少(1:10) | VARCHAR | ~10万 |
| 高(1%) | 多(2:1) | INT | ~10万 |
| 高(1%) | 多(2:1) | VARCHAR | ~10万 |
| 中(10%) | 少(1:10) | INT | ~100万 |
| 中(10%) | 少(1:10) | VARCHAR | ~100万 |
| 中(10%) | 多(2:1) | INT | ~100万 |
| 中(10%) | 多(2:1) | VARCHAR | ~100万 |
| 低(50%) | 少(1:10) | INT | ~500万 |
| 低(50%) | 少(1:10) | VARCHAR | ~500万 |
| 低(50%) | 多(2:1) | INT | ~500万 |
| 低(50%) | 多(2:1) | VARCHAR | ~500万 |

### 配置对比

**配置A**: 开启谓词列延迟物化
```sql
SET enable_predicate_col_late_materialize = true;
SET late_materialization_ratio = 1000;  -- 强制延迟物化
```

**配置B**: 关闭谓词列延迟物化  
```sql
SET enable_predicate_col_late_materialize = false;
SET late_materialization_ratio = 10;    -- 当前默认值
```

## 预期性能表现

### 延迟物化预期优势场景
1. **高过滤性 + 谓词列少**: 减少不必要的非谓词列IO
2. **高过滤性 + 谓词列多**: 谓词重排序提升过滤效率  
3. **VARCHAR类型**: 减少变长数据的内存拷贝开销

### 传统方式预期优势场景
1. **低过滤性**: 延迟物化的额外开销可能超过收益
2. **谓词列多 + 低过滤性**: 额外的谓词处理开销

## 故障排除

### 常见问题

**1. 内存不足**
```
错误: Out of memory
解决: 增加BE节点内存或减少数据量
```

**2. 磁盘空间不足**
```
错误: No space left on device  
解决: 清理磁盘空间或使用更大的存储
```

**3. 连接超时**
```
错误: Connection timeout
解决: 检查网络连接和StarRocks服务状态
```

**4. 查询超时**
```
错误: Query timeout
解决: 增加query_timeout参数或优化查询
```

### 性能调优建议

**1. BE节点配置**
```
# be.conf
mem_limit = 80%
storage_root_path = /data/starrocks
```

**2. 会话参数调优**
```sql
SET query_timeout = 300;
SET batch_size = 4096;
SET enable_vectorized_engine = true;
```

**3. 系统参数调优**
```bash
# 增加文件描述符限制
ulimit -n 65536

# 禁用swap
swapoff -a
```

## 结果解读

### 性能指标含义

- **执行时间**: 查询从开始到结束的总时间
- **性能提升**: (配置B时间 - 配置A时间) / 配置B时间 × 100%
- **推荐配置**: 基于执行时间更短的配置

### 分析维度

1. **总体性能**: 所有场景的平均表现
2. **按过滤性**: 不同选择性下的表现差异
3. **按列比例**: 谓词列与非谓词列比例的影响
4. **按数据类型**: CHAR vs VARCHAR的性能差异

### 决策建议

根据测试结果，可以得出在不同业务场景下的最佳配置选择：

- **OLAP分析场景**: 通常过滤性较高，推荐开启延迟物化
- **实时查询场景**: 过滤性可能较低，需要根据具体情况选择
- **宽表查询**: 非谓词列较多时，延迟物化优势明显
- **字符串密集型**: VARCHAR类型数据较多时，延迟物化收益更大

## 注意事项

1. **测试时间**: 完整测试需要3-5小时，请合理安排时间
2. **资源消耗**: 测试期间会产生大量IO和CPU负载
3. **数据清理**: 测试完成后可删除测试表释放空间
4. **生产环境**: 建议先在测试环境验证后再应用到生产环境

## 联系支持

如遇到问题，请提供以下信息：
- StarRocks版本信息
- 集群配置详情  
- 错误日志内容
- 测试环境描述

---

**最后更新**: 2024年10月27日