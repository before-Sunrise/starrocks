-- =====================================================
-- 谓词列延迟物化性能测试 - 数据生成脚本
-- =====================================================

-- 创建数据生成的辅助表
DROP TABLE IF EXISTS data_generator;
CREATE TABLE data_generator (
    id BIGINT,
    rand_val INT
) 
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 32;

-- 生成1000万行的基础数据 (分批插入以避免内存问题)
INSERT INTO data_generator
SELECT 
    row_number() OVER () as id,
    CAST(rand() * 1000000 AS INT) as rand_val
FROM TABLE(generate_series(1, 2500000));

INSERT INTO data_generator
SELECT 
    row_number() OVER () + 2500000 as id,
    CAST(rand() * 1000000 AS INT) as rand_val
FROM TABLE(generate_series(1, 2500000));

INSERT INTO data_generator
SELECT 
    row_number() OVER () + 5000000 as id,
    CAST(rand() * 1000000 AS INT) as rand_val
FROM TABLE(generate_series(1, 2500000));

INSERT INTO data_generator
SELECT 
    row_number() OVER () + 7500000 as id,
    CAST(rand() * 1000000 AS INT) as rand_val
FROM TABLE(generate_series(1, 2500000));

-- =====================================================
-- 填充表1：谓词列少 + 定长数值类型
-- =====================================================

-- 插入数据，实现不同过滤性
INSERT INTO test_predicate_few_int
SELECT 
    -- 谓词列：实现不同过滤性的数据分布
    CASE 
        WHEN rand_val % 1000 < 10 THEN 1000000 + (rand_val % 10)      -- 1% 高过滤性 (1000000-1000009)
        WHEN rand_val % 1000 < 100 THEN 2000000 + (rand_val % 10)     -- 9% 中等过滤性 (2000000-2000009)
        ELSE 3000000 + (rand_val % 10)                                 -- 90% 低过滤性 (3000000-3000009)
    END as filter_col1,
    
    CASE 
        WHEN (rand_val + 1) % 1000 < 10 THEN 1000000 + ((rand_val + 1) % 10)
        WHEN (rand_val + 1) % 1000 < 100 THEN 2000000 + ((rand_val + 1) % 10)
        ELSE 3000000 + ((rand_val + 1) % 10)
    END as filter_col2,
    
    -- 非谓词列：随机整数数据
    rand_val % 10000 as data_col1,
    (rand_val + 1) % 10000 as data_col2,
    (rand_val + 2) % 10000 as data_col3,
    (rand_val + 3) % 10000 as data_col4,
    (rand_val + 4) % 10000 as data_col5,
    (rand_val + 5) % 10000 as data_col6,
    (rand_val + 6) % 10000 as data_col7,
    (rand_val + 7) % 10000 as data_col8,
    (rand_val + 8) % 10000 as data_col9,
    (rand_val + 9) % 10000 as data_col10,
    (rand_val + 10) % 10000 as data_col11,
    (rand_val + 11) % 10000 as data_col12,
    (rand_val + 12) % 10000 as data_col13,
    (rand_val + 13) % 10000 as data_col14,
    (rand_val + 14) % 10000 as data_col15,
    (rand_val + 15) % 10000 as data_col16,
    (rand_val + 16) % 10000 as data_col17,
    (rand_val + 17) % 10000 as data_col18,
    (rand_val + 18) % 10000 as data_col19,
    (rand_val + 19) % 10000 as data_col20
FROM data_generator;

-- =====================================================
-- 填充表2：谓词列少 + 变长字符类型
-- =====================================================

INSERT INTO test_predicate_few_varchar
SELECT 
    -- 谓词列：变长字符，长度随机
    CASE 
        WHEN rand_val % 1000 < 10 THEN CONCAT('rare_value_', CAST(rand_val % 10 AS STRING), '_', REPEAT('x', rand_val % 150))
        WHEN rand_val % 1000 < 100 THEN CONCAT('medium_value_', CAST(rand_val % 10 AS STRING), '_', REPEAT('y', rand_val % 100))
        ELSE CONCAT('common_value_', CAST(rand_val % 10 AS STRING), '_', REPEAT('z', rand_val % 50))
    END as filter_col1,
    
    CASE 
        WHEN (rand_val + 1) % 1000 < 10 THEN CONCAT('rare_value_', CAST((rand_val + 1) % 10 AS STRING), '_', REPEAT('a', (rand_val + 1) % 150))
        WHEN (rand_val + 1) % 1000 < 100 THEN CONCAT('medium_value_', CAST((rand_val + 1) % 10 AS STRING), '_', REPEAT('b', (rand_val + 1) % 100))
        ELSE CONCAT('common_value_', CAST((rand_val + 1) % 10 AS STRING), '_', REPEAT('c', (rand_val + 1) % 50))
    END as filter_col2,
    
    -- 非谓词列：变长字符数据
    CONCAT('varchar_data_', CAST(rand_val % 10000 AS STRING), '_', REPEAT('d', rand_val % 150)) as data_col1,
    CONCAT('varchar_data_', CAST((rand_val + 1) % 10000 AS STRING), '_', REPEAT('e', (rand_val + 1) % 150)) as data_col2,
    CONCAT('varchar_data_', CAST((rand_val + 2) % 10000 AS STRING), '_', REPEAT('f', (rand_val + 2) % 150)) as data_col3,
    CONCAT('varchar_data_', CAST((rand_val + 3) % 10000 AS STRING), '_', REPEAT('g', (rand_val + 3) % 150)) as data_col4,
    CONCAT('varchar_data_', CAST((rand_val + 4) % 10000 AS STRING), '_', REPEAT('h', (rand_val + 4) % 150)) as data_col5,
    CONCAT('varchar_data_', CAST((rand_val + 5) % 10000 AS STRING), '_', REPEAT('i', (rand_val + 5) % 150)) as data_col6,
    CONCAT('varchar_data_', CAST((rand_val + 6) % 10000 AS STRING), '_', REPEAT('j', (rand_val + 6) % 150)) as data_col7,
    CONCAT('varchar_data_', CAST((rand_val + 7) % 10000 AS STRING), '_', REPEAT('k', (rand_val + 7) % 150)) as data_col8,
    CONCAT('varchar_data_', CAST((rand_val + 8) % 10000 AS STRING), '_', REPEAT('l', (rand_val + 8) % 150)) as data_col9,
    CONCAT('varchar_data_', CAST((rand_val + 9) % 10000 AS STRING), '_', REPEAT('m', (rand_val + 9) % 150)) as data_col10,
    CONCAT('varchar_data_', CAST((rand_val + 10) % 10000 AS STRING), '_', REPEAT('n', (rand_val + 10) % 150)) as data_col11,
    CONCAT('varchar_data_', CAST((rand_val + 11) % 10000 AS STRING), '_', REPEAT('o', (rand_val + 11) % 150)) as data_col12,
    CONCAT('varchar_data_', CAST((rand_val + 12) % 10000 AS STRING), '_', REPEAT('p', (rand_val + 12) % 150)) as data_col13,
    CONCAT('varchar_data_', CAST((rand_val + 13) % 10000 AS STRING), '_', REPEAT('q', (rand_val + 13) % 150)) as data_col14,
    CONCAT('varchar_data_', CAST((rand_val + 14) % 10000 AS STRING), '_', REPEAT('r', (rand_val + 14) % 150)) as data_col15,
    CONCAT('varchar_data_', CAST((rand_val + 15) % 10000 AS STRING), '_', REPEAT('s', (rand_val + 15) % 150)) as data_col16,
    CONCAT('varchar_data_', CAST((rand_val + 16) % 10000 AS STRING), '_', REPEAT('t', (rand_val + 16) % 150)) as data_col17,
    CONCAT('varchar_data_', CAST((rand_val + 17) % 10000 AS STRING), '_', REPEAT('u', (rand_val + 17) % 150)) as data_col18,
    CONCAT('varchar_data_', CAST((rand_val + 18) % 10000 AS STRING), '_', REPEAT('v', (rand_val + 18) % 150)) as data_col19,
    CONCAT('varchar_data_', CAST((rand_val + 19) % 10000 AS STRING), '_', REPEAT('w', (rand_val + 19) % 150)) as data_col20
FROM data_generator;

-- =====================================================
-- 填充表3：谓词列多 + 定长数值类型
-- =====================================================

INSERT INTO test_predicate_many_int
SELECT 
    -- 10个谓词列
    CASE 
        WHEN rand_val % 1000 < 10 THEN 1000000 + (rand_val % 10)
        WHEN rand_val % 1000 < 100 THEN 2000000 + (rand_val % 10)
        ELSE 3000000 + (rand_val % 10)
    END as filter_col1,
    
    CASE 
        WHEN (rand_val + 1) % 1000 < 10 THEN 1000000 + ((rand_val + 1) % 10)
        WHEN (rand_val + 1) % 1000 < 100 THEN 2000000 + ((rand_val + 1) % 10)
        ELSE 3000000 + ((rand_val + 1) % 10)
    END as filter_col2,
    
    CASE 
        WHEN (rand_val + 2) % 1000 < 10 THEN 1000000 + ((rand_val + 2) % 10)
        WHEN (rand_val + 2) % 1000 < 100 THEN 2000000 + ((rand_val + 2) % 10)
        ELSE 3000000 + ((rand_val + 2) % 10)
    END as filter_col3,
    
    CASE 
        WHEN (rand_val + 3) % 1000 < 10 THEN 1000000 + ((rand_val + 3) % 10)
        WHEN (rand_val + 3) % 1000 < 100 THEN 2000000 + ((rand_val + 3) % 10)
        ELSE 3000000 + ((rand_val + 3) % 10)
    END as filter_col4,
    
    CASE 
        WHEN (rand_val + 4) % 1000 < 10 THEN 1000000 + ((rand_val + 4) % 10)
        WHEN (rand_val + 4) % 1000 < 100 THEN 2000000 + ((rand_val + 4) % 10)
        ELSE 3000000 + ((rand_val + 4) % 10)
    END as filter_col5,
    
    CASE 
        WHEN (rand_val + 5) % 1000 < 10 THEN 1000000 + ((rand_val + 5) % 10)
        WHEN (rand_val + 5) % 1000 < 100 THEN 2000000 + ((rand_val + 5) % 10)
        ELSE 3000000 + ((rand_val + 5) % 10)
    END as filter_col6,
    
    CASE 
        WHEN (rand_val + 6) % 1000 < 10 THEN 1000000 + ((rand_val + 6) % 10)
        WHEN (rand_val + 6) % 1000 < 100 THEN 2000000 + ((rand_val + 6) % 10)
        ELSE 3000000 + ((rand_val + 6) % 10)
    END as filter_col7,
    
    CASE 
        WHEN (rand_val + 7) % 1000 < 10 THEN 1000000 + ((rand_val + 7) % 10)
        WHEN (rand_val + 7) % 1000 < 100 THEN 2000000 + ((rand_val + 7) % 10)
        ELSE 3000000 + ((rand_val + 7) % 10)
    END as filter_col8,
    
    CASE 
        WHEN (rand_val + 8) % 1000 < 10 THEN 1000000 + ((rand_val + 8) % 10)
        WHEN (rand_val + 8) % 1000 < 100 THEN 2000000 + ((rand_val + 8) % 10)
        ELSE 3000000 + ((rand_val + 8) % 10)
    END as filter_col9,
    
    CASE 
        WHEN (rand_val + 9) % 1000 < 10 THEN 1000000 + ((rand_val + 9) % 10)
        WHEN (rand_val + 9) % 1000 < 100 THEN 2000000 + ((rand_val + 9) % 10)
        ELSE 3000000 + ((rand_val + 9) % 10)
    END as filter_col10,
    
    -- 5个非谓词列
    rand_val % 10000 as data_col1,
    (rand_val + 1) % 10000 as data_col2,
    (rand_val + 2) % 10000 as data_col3,
    (rand_val + 3) % 10000 as data_col4,
    (rand_val + 4) % 10000 as data_col5
FROM data_generator;

-- =====================================================
-- 填充表4：谓词列多 + 变长字符类型
-- =====================================================

INSERT INTO test_predicate_many_varchar
SELECT 
    -- 10个谓词列 (变长)
    CASE 
        WHEN rand_val % 1000 < 10 THEN CONCAT('rare_value_', CAST(rand_val % 10 AS STRING), '_', REPEAT('x', rand_val % 150))
        WHEN rand_val % 1000 < 100 THEN CONCAT('medium_value_', CAST(rand_val % 10 AS STRING), '_', REPEAT('y', rand_val % 100))
        ELSE CONCAT('common_value_', CAST(rand_val % 10 AS STRING), '_', REPEAT('z', rand_val % 50))
    END as filter_col1,
    
    CASE 
        WHEN (rand_val + 1) % 1000 < 10 THEN CONCAT('rare_value_', CAST((rand_val + 1) % 10 AS STRING), '_', REPEAT('a', (rand_val + 1) % 150))
        WHEN (rand_val + 1) % 1000 < 100 THEN CONCAT('medium_value_', CAST((rand_val + 1) % 10 AS STRING), '_', REPEAT('b', (rand_val + 1) % 100))
        ELSE CONCAT('common_value_', CAST((rand_val + 1) % 10 AS STRING), '_', REPEAT('c', (rand_val + 1) % 50))
    END as filter_col2,
    
    CASE 
        WHEN (rand_val + 2) % 1000 < 10 THEN CONCAT('rare_value_', CAST((rand_val + 2) % 10 AS STRING), '_', REPEAT('d', (rand_val + 2) % 150))
        WHEN (rand_val + 2) % 1000 < 100 THEN CONCAT('medium_value_', CAST((rand_val + 2) % 10 AS STRING), '_', REPEAT('e', (rand_val + 2) % 100))
        ELSE CONCAT('common_value_', CAST((rand_val + 2) % 10 AS STRING), '_', REPEAT('f', (rand_val + 2) % 50))
    END as filter_col3,
    
    CASE 
        WHEN (rand_val + 3) % 1000 < 10 THEN CONCAT('rare_value_', CAST((rand_val + 3) % 10 AS STRING), '_', REPEAT('g', (rand_val + 3) % 150))
        WHEN (rand_val + 3) % 1000 < 100 THEN CONCAT('medium_value_', CAST((rand_val + 3) % 10 AS STRING), '_', REPEAT('h', (rand_val + 3) % 100))
        ELSE CONCAT('common_value_', CAST((rand_val + 3) % 10 AS STRING), '_', REPEAT('i', (rand_val + 3) % 50))
    END as filter_col4,
    
    CASE 
        WHEN (rand_val + 4) % 1000 < 10 THEN CONCAT('rare_value_', CAST((rand_val + 4) % 10 AS STRING), '_', REPEAT('j', (rand_val + 4) % 150))
        WHEN (rand_val + 4) % 1000 < 100 THEN CONCAT('medium_value_', CAST((rand_val + 4) % 10 AS STRING), '_', REPEAT('k', (rand_val + 4) % 100))
        ELSE CONCAT('common_value_', CAST((rand_val + 4) % 10 AS STRING), '_', REPEAT('l', (rand_val + 4) % 50))
    END as filter_col5,
    
    CASE 
        WHEN (rand_val + 5) % 1000 < 10 THEN CONCAT('rare_value_', CAST((rand_val + 5) % 10 AS STRING), '_', REPEAT('m', (rand_val + 5) % 150))
        WHEN (rand_val + 5) % 1000 < 100 THEN CONCAT('medium_value_', CAST((rand_val + 5) % 10 AS STRING), '_', REPEAT('n', (rand_val + 5) % 100))
        ELSE CONCAT('common_value_', CAST((rand_val + 5) % 10 AS STRING), '_', REPEAT('o', (rand_val + 5) % 50))
    END as filter_col6,
    
    CASE 
        WHEN (rand_val + 6) % 1000 < 10 THEN CONCAT('rare_value_', CAST((rand_val + 6) % 10 AS STRING), '_', REPEAT('p', (rand_val + 6) % 150))
        WHEN (rand_val + 6) % 1000 < 100 THEN CONCAT('medium_value_', CAST((rand_val + 6) % 10 AS STRING), '_', REPEAT('q', (rand_val + 6) % 100))
        ELSE CONCAT('common_value_', CAST((rand_val + 6) % 10 AS STRING), '_', REPEAT('r', (rand_val + 6) % 50))
    END as filter_col7,
    
    CASE 
        WHEN (rand_val + 7) % 1000 < 10 THEN CONCAT('rare_value_', CAST((rand_val + 7) % 10 AS STRING), '_', REPEAT('s', (rand_val + 7) % 150))
        WHEN (rand_val + 7) % 1000 < 100 THEN CONCAT('medium_value_', CAST((rand_val + 7) % 10 AS STRING), '_', REPEAT('t', (rand_val + 7) % 100))
        ELSE CONCAT('common_value_', CAST((rand_val + 7) % 10 AS STRING), '_', REPEAT('u', (rand_val + 7) % 50))
    END as filter_col8,
    
    CASE 
        WHEN (rand_val + 8) % 1000 < 10 THEN CONCAT('rare_value_', CAST((rand_val + 8) % 10 AS STRING), '_', REPEAT('v', (rand_val + 8) % 150))
        WHEN (rand_val + 8) % 1000 < 100 THEN CONCAT('medium_value_', CAST((rand_val + 8) % 10 AS STRING), '_', REPEAT('w', (rand_val + 8) % 100))
        ELSE CONCAT('common_value_', CAST((rand_val + 8) % 10 AS STRING), '_', REPEAT('x', (rand_val + 8) % 50))
    END as filter_col9,
    
    CASE 
        WHEN (rand_val + 9) % 1000 < 10 THEN CONCAT('rare_value_', CAST((rand_val + 9) % 10 AS STRING), '_', REPEAT('y', (rand_val + 9) % 150))
        WHEN (rand_val + 9) % 1000 < 100 THEN CONCAT('medium_value_', CAST((rand_val + 9) % 10 AS STRING), '_', REPEAT('z', (rand_val + 9) % 100))
        ELSE CONCAT('common_value_', CAST((rand_val + 9) % 10 AS STRING), '_', REPEAT('0', (rand_val + 9) % 50))
    END as filter_col10,
    
    -- 5个非谓词列 (变长)
    CONCAT('varchar_data_', CAST(rand_val % 10000 AS STRING), '_', REPEAT('1', rand_val % 150)) as data_col1,
    CONCAT('varchar_data_', CAST((rand_val + 1) % 10000 AS STRING), '_', REPEAT('2', (rand_val + 1) % 150)) as data_col2,
    CONCAT('varchar_data_', CAST((rand_val + 2) % 10000 AS STRING), '_', REPEAT('3', (rand_val + 2) % 150)) as data_col3,
    CONCAT('varchar_data_', CAST((rand_val + 3) % 10000 AS STRING), '_', REPEAT('4', (rand_val + 3) % 150)) as data_col4,
    CONCAT('varchar_data_', CAST((rand_val + 4) % 10000 AS STRING), '_', REPEAT('5', (rand_val + 4) % 150)) as data_col5
FROM data_generator;

-- 清理辅助表
DROP TABLE data_generator;

-- 收集统计信息
ANALYZE TABLE test_predicate_few_int;
ANALYZE TABLE test_predicate_few_varchar;
ANALYZE TABLE test_predicate_many_int;
ANALYZE TABLE test_predicate_many_varchar;

-- 显示表的行数
SELECT 'test_predicate_few_int' as table_name, COUNT(*) as row_count FROM test_predicate_few_int
UNION ALL
SELECT 'test_predicate_few_varchar' as table_name, COUNT(*) as row_count FROM test_predicate_few_varchar
UNION ALL
SELECT 'test_predicate_many_int' as table_name, COUNT(*) as row_count FROM test_predicate_many_int
UNION ALL
SELECT 'test_predicate_many_varchar' as table_name, COUNT(*) as row_count FROM test_predicate_many_varchar;