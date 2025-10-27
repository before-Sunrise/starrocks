# 谓词列延迟物化性能测试方案

## 测试背景

基于PR #64600，该PR实现了谓词列延迟物化功能，主要优化包括：
1. 支持谓词列的延迟物化读取
2. 根据谓词选择性对谓词列进行重排序
3. 优化同一页面内行的批量处理

## 测试目标

比较以下两种配置的性能表现：
- **配置A**：开启谓词列延迟物化 + `late_materialization_ratio=1000` (强制延迟物化)
- **配置B**：关闭谓词列延迟物化 + `late_materialization_ratio=10` (当前默认值)

## 测试维度

### 1. 过滤性维度 (Selectivity)
- **高过滤性 (High Selectivity)**：谓词过滤掉99%的数据 (selectivity ~1%)
- **中等过滤性 (Medium Selectivity)**：谓词过滤掉90%的数据 (selectivity ~10%)  
- **低过滤性 (Low Selectivity)**：谓词过滤掉50%的数据 (selectivity ~50%)

### 2. 列数量比例维度 (Column Ratio)
- **谓词列少**：2个谓词列 vs 20个非谓词列 (1:10比例)
- **谓词列多**：10个谓词列 vs 5个非谓词列 (2:1比例)

### 3. 数据类型维度 (Data Types)
- **定长字符类型**：CHAR(50)
- **变长字符类型**：VARCHAR(200)

## 测试表结构设计

### 表1：谓词列少 + 定长字符
```sql
CREATE TABLE test_predicate_few_char (
    -- 谓词列 (2个)
    filter_col1 CHAR(50),
    filter_col2 CHAR(50),
    
    -- 非谓词列 (20个)
    data_col1 CHAR(50), data_col2 CHAR(50), data_col3 CHAR(50), data_col4 CHAR(50),
    data_col5 CHAR(50), data_col6 CHAR(50), data_col7 CHAR(50), data_col8 CHAR(50),
    data_col9 CHAR(50), data_col10 CHAR(50), data_col11 CHAR(50), data_col12 CHAR(50),
    data_col13 CHAR(50), data_col14 CHAR(50), data_col15 CHAR(50), data_col16 CHAR(50),
    data_col17 CHAR(50), data_col18 CHAR(50), data_col19 CHAR(50), data_col20 CHAR(50)
) 
DUPLICATE KEY(filter_col1, filter_col2)
DISTRIBUTED BY HASH(filter_col1) BUCKETS 32;
```

### 表2：谓词列少 + 变长字符
```sql
CREATE TABLE test_predicate_few_varchar (
    -- 谓词列 (2个)
    filter_col1 VARCHAR(200),
    filter_col2 VARCHAR(200),
    
    -- 非谓词列 (20个)
    data_col1 VARCHAR(200), data_col2 VARCHAR(200), data_col3 VARCHAR(200), data_col4 VARCHAR(200),
    data_col5 VARCHAR(200), data_col6 VARCHAR(200), data_col7 VARCHAR(200), data_col8 VARCHAR(200),
    data_col9 VARCHAR(200), data_col10 VARCHAR(200), data_col11 VARCHAR(200), data_col12 VARCHAR(200),
    data_col13 VARCHAR(200), data_col14 VARCHAR(200), data_col15 VARCHAR(200), data_col16 VARCHAR(200),
    data_col17 VARCHAR(200), data_col18 VARCHAR(200), data_col19 VARCHAR(200), data_col20 VARCHAR(200)
) 
DUPLICATE KEY(filter_col1, filter_col2)
DISTRIBUTED BY HASH(filter_col1) BUCKETS 32;
```

### 表3：谓词列多 + 定长字符
```sql
CREATE TABLE test_predicate_many_char (
    -- 谓词列 (10个)
    filter_col1 CHAR(50), filter_col2 CHAR(50), filter_col3 CHAR(50), filter_col4 CHAR(50),
    filter_col5 CHAR(50), filter_col6 CHAR(50), filter_col7 CHAR(50), filter_col8 CHAR(50),
    filter_col9 CHAR(50), filter_col10 CHAR(50),
    
    -- 非谓词列 (5个)
    data_col1 CHAR(50), data_col2 CHAR(50), data_col3 CHAR(50), data_col4 CHAR(50), data_col5 CHAR(50)
) 
DUPLICATE KEY(filter_col1, filter_col2)
DISTRIBUTED BY HASH(filter_col1) BUCKETS 32;
```

### 表4：谓词列多 + 变长字符
```sql
CREATE TABLE test_predicate_many_varchar (
    -- 谓词列 (10个)
    filter_col1 VARCHAR(200), filter_col2 VARCHAR(200), filter_col3 VARCHAR(200), filter_col4 VARCHAR(200),
    filter_col5 VARCHAR(200), filter_col6 VARCHAR(200), filter_col7 VARCHAR(200), filter_col8 VARCHAR(200),
    filter_col9 VARCHAR(200), filter_col10 VARCHAR(200),
    
    -- 非谓词列 (5个)
    data_col1 VARCHAR(200), data_col2 VARCHAR(200), data_col3 VARCHAR(200), data_col4 VARCHAR(200), data_col5 VARCHAR(200)
) 
DUPLICATE KEY(filter_col1, filter_col2)
DISTRIBUTED BY HASH(filter_col1) BUCKETS 32;
```

## 数据生成策略

### 数据量
每个表插入 **1000万行** 数据，确保有足够的数据量来测试性能差异。

### 数据分布策略
为了实现不同的过滤性，采用以下数据分布：

#### 高过滤性 (1% selectivity)
```sql
-- filter_col1: 99%的值为'common_value'，1%的值为'rare_value_X'
-- filter_col2: 类似分布
```

#### 中等过滤性 (10% selectivity)  
```sql
-- filter_col1: 90%的值为'common_value'，10%的值为'medium_value_X'
-- filter_col2: 类似分布
```

#### 低过滤性 (50% selectivity)
```sql
-- filter_col1: 50%的值为'value_A'，50%的值为'value_B' 
-- filter_col2: 类似分布
```

## 测试查询设计

### 查询模板1：谓词列少的情况
```sql
-- 高过滤性查询
SELECT data_col1, data_col2, data_col3, data_col4, data_col5, 
       data_col6, data_col7, data_col8, data_col9, data_col10,
       data_col11, data_col12, data_col13, data_col14, data_col15,
       data_col16, data_col17, data_col18, data_col19, data_col20
FROM test_predicate_few_char 
WHERE filter_col1 = 'rare_value_1' AND filter_col2 = 'rare_value_2';

-- 中等过滤性查询  
SELECT data_col1, data_col2, data_col3, data_col4, data_col5,
       data_col6, data_col7, data_col8, data_col9, data_col10,
       data_col11, data_col12, data_col13, data_col14, data_col15,
       data_col16, data_col17, data_col18, data_col19, data_col20
FROM test_predicate_few_char 
WHERE filter_col1 = 'medium_value_1' AND filter_col2 = 'medium_value_2';

-- 低过滤性查询
SELECT data_col1, data_col2, data_col3, data_col4, data_col5,
       data_col6, data_col7, data_col8, data_col9, data_col10,
       data_col11, data_col12, data_col13, data_col14, data_col15,
       data_col16, data_col17, data_col18, data_col19, data_col20
FROM test_predicate_few_char 
WHERE filter_col1 = 'value_A' AND filter_col2 = 'value_B';
```

### 查询模板2：谓词列多的情况
```sql
-- 高过滤性查询
SELECT data_col1, data_col2, data_col3, data_col4, data_col5
FROM test_predicate_many_char 
WHERE filter_col1 = 'rare_value_1' 
  AND filter_col2 = 'rare_value_2'
  AND filter_col3 = 'rare_value_3'
  AND filter_col4 = 'rare_value_4'
  AND filter_col5 = 'rare_value_5'
  AND filter_col6 = 'rare_value_6'
  AND filter_col7 = 'rare_value_7'
  AND filter_col8 = 'rare_value_8'
  AND filter_col9 = 'rare_value_9'
  AND filter_col10 = 'rare_value_10';

-- 中等过滤性查询
SELECT data_col1, data_col2, data_col3, data_col4, data_col5
FROM test_predicate_many_char 
WHERE filter_col1 = 'medium_value_1' 
  AND filter_col2 = 'medium_value_2'
  AND filter_col3 = 'medium_value_3'
  AND filter_col4 = 'medium_value_4'
  AND filter_col5 = 'medium_value_5'
  AND filter_col6 = 'medium_value_6'
  AND filter_col7 = 'medium_value_7'
  AND filter_col8 = 'medium_value_8'
  AND filter_col9 = 'medium_value_9'
  AND filter_col10 = 'medium_value_10';

-- 低过滤性查询
SELECT data_col1, data_col2, data_col3, data_col4, data_col5
FROM test_predicate_many_char 
WHERE filter_col1 = 'value_A' 
  AND filter_col2 = 'value_B'
  AND filter_col3 = 'value_A'
  AND filter_col4 = 'value_B'
  AND filter_col5 = 'value_A'
  AND filter_col6 = 'value_B'
  AND filter_col7 = 'value_A'
  AND filter_col8 = 'value_B'
  AND filter_col9 = 'value_A'
  AND filter_col10 = 'value_B';
```

## 测试配置

### 配置A：开启谓词列延迟物化
```sql
-- 会话级别设置
SET enable_predicate_col_late_materialize = true;
SET late_materialization_ratio = 1000;  -- 强制延迟物化
```

### 配置B：关闭谓词列延迟物化  
```sql
-- 会话级别设置
SET enable_predicate_col_late_materialize = false;
SET late_materialization_ratio = 10;    -- 当前默认值
```

## 性能指标收集

### 1. 查询执行时间
- 总执行时间
- 扫描阶段耗时
- 过滤阶段耗时

### 2. 资源消耗指标
- CPU使用率
- 内存使用量
- IO读取量
- 网络传输量

### 3. 存储层指标
- 读取的页面数量
- 实际读取的数据量
- 缓存命中率

## 测试执行步骤

### 1. 环境准备
```bash
# 确保集群资源充足
# 清理缓存确保测试公平性
```

### 2. 数据准备
```sql
-- 创建所有测试表
-- 生成并插入测试数据
-- 收集表统计信息
ANALYZE TABLE test_predicate_few_char;
ANALYZE TABLE test_predicate_few_varchar;
ANALYZE TABLE test_predicate_many_char;
ANALYZE TABLE test_predicate_many_varchar;
```

### 3. 性能测试执行
对于每个测试场景：
1. 设置对应的配置参数
2. 清理系统缓存
3. 执行查询并记录性能指标
4. 重复执行3次取平均值
5. 切换配置重复测试

### 4. 结果分析
比较两种配置在不同维度组合下的性能表现：
- 执行时间对比
- 资源消耗对比  
- 扫描效率对比

## 预期结果分析

### 谓词列延迟物化预期优势场景：
1. **高过滤性 + 谓词列少**：减少不必要的非谓词列读取
2. **高过滤性 + 谓词列多**：谓词重排序提升过滤效率
3. **VARCHAR类型**：减少变长数据的内存拷贝

### 传统方式预期优势场景：
1. **低过滤性**：延迟物化的开销可能超过收益
2. **谓词列多 + 低过滤性**：额外的谓词处理开销

## 测试矩阵总结

总共需要测试 **24个场景** (3个过滤性 × 2个列比例 × 2个数据类型 × 2种配置)：

| 过滤性 | 列比例 | 数据类型 | 配置A性能 | 配置B性能 | 推荐配置 |
|--------|--------|----------|-----------|-----------|----------|
| 高(1%) | 少(1:10) | CHAR | ? | ? | ? |
| 高(1%) | 少(1:10) | VARCHAR | ? | ? | ? |
| 高(1%) | 多(2:1) | CHAR | ? | ? | ? |
| 高(1%) | 多(2:1) | VARCHAR | ? | ? | ? |
| 中(10%) | 少(1:10) | CHAR | ? | ? | ? |
| 中(10%) | 少(1:10) | VARCHAR | ? | ? | ? |
| 中(10%) | 多(2:1) | CHAR | ? | ? | ? |
| 中(10%) | 多(2:1) | VARCHAR | ? | ? | ? |
| 低(50%) | 少(1:10) | CHAR | ? | ? | ? |
| 低(50%) | 少(1:10) | VARCHAR | ? | ? | ? |
| 低(50%) | 多(2:1) | CHAR | ? | ? | ? |
| 低(50%) | 多(2:1) | VARCHAR | ? | ? | ? |

通过这个全面的测试方案，可以清楚地了解谓词列延迟物化在不同场景下的性能表现，为生产环境的配置选择提供数据支撑。