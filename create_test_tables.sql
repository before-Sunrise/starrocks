-- =====================================================
-- 谓词列延迟物化性能测试 - 建表脚本
-- =====================================================

-- 表1：谓词列少 + 定长数值类型
DROP TABLE IF EXISTS test_predicate_few_int;
CREATE TABLE test_predicate_few_int (
    -- 谓词列 (2个)
    filter_col1 INT NOT NULL,
    filter_col2 INT NOT NULL,
    
    -- 非谓词列 (20个)
    data_col1 INT NOT NULL,
    data_col2 INT NOT NULL,
    data_col3 INT NOT NULL,
    data_col4 INT NOT NULL,
    data_col5 INT NOT NULL,
    data_col6 INT NOT NULL,
    data_col7 INT NOT NULL,
    data_col8 INT NOT NULL,
    data_col9 INT NOT NULL,
    data_col10 INT NOT NULL,
    data_col11 INT NOT NULL,
    data_col12 INT NOT NULL,
    data_col13 INT NOT NULL,
    data_col14 INT NOT NULL,
    data_col15 INT NOT NULL,
    data_col16 INT NOT NULL,
    data_col17 INT NOT NULL,
    data_col18 INT NOT NULL,
    data_col19 INT NOT NULL,
    data_col20 INT NOT NULL
) 
DUPLICATE KEY(filter_col1, filter_col2)
DISTRIBUTED BY HASH(filter_col1) BUCKETS 32
PROPERTIES (
    "replication_num" = "3",
    "compression" = "LZ4"
);

-- 表2：谓词列少 + 变长字符类型
DROP TABLE IF EXISTS test_predicate_few_varchar;
CREATE TABLE test_predicate_few_varchar (
    -- 谓词列 (2个)
    filter_col1 VARCHAR(200) NOT NULL,
    filter_col2 VARCHAR(200) NOT NULL,
    
    -- 非谓词列 (20个)
    data_col1 VARCHAR(200) NOT NULL,
    data_col2 VARCHAR(200) NOT NULL,
    data_col3 VARCHAR(200) NOT NULL,
    data_col4 VARCHAR(200) NOT NULL,
    data_col5 VARCHAR(200) NOT NULL,
    data_col6 VARCHAR(200) NOT NULL,
    data_col7 VARCHAR(200) NOT NULL,
    data_col8 VARCHAR(200) NOT NULL,
    data_col9 VARCHAR(200) NOT NULL,
    data_col10 VARCHAR(200) NOT NULL,
    data_col11 VARCHAR(200) NOT NULL,
    data_col12 VARCHAR(200) NOT NULL,
    data_col13 VARCHAR(200) NOT NULL,
    data_col14 VARCHAR(200) NOT NULL,
    data_col15 VARCHAR(200) NOT NULL,
    data_col16 VARCHAR(200) NOT NULL,
    data_col17 VARCHAR(200) NOT NULL,
    data_col18 VARCHAR(200) NOT NULL,
    data_col19 VARCHAR(200) NOT NULL,
    data_col20 VARCHAR(200) NOT NULL
) 
DUPLICATE KEY(filter_col1, filter_col2)
DISTRIBUTED BY HASH(filter_col1) BUCKETS 32
PROPERTIES (
    "replication_num" = "3",
    "compression" = "LZ4"
);

-- 表3：谓词列多 + 定长数值类型
DROP TABLE IF EXISTS test_predicate_many_int;
CREATE TABLE test_predicate_many_int (
    -- 谓词列 (10个)
    filter_col1 INT NOT NULL,
    filter_col2 INT NOT NULL,
    filter_col3 INT NOT NULL,
    filter_col4 INT NOT NULL,
    filter_col5 INT NOT NULL,
    filter_col6 INT NOT NULL,
    filter_col7 INT NOT NULL,
    filter_col8 INT NOT NULL,
    filter_col9 INT NOT NULL,
    filter_col10 INT NOT NULL,
    
    -- 非谓词列 (5个)
    data_col1 INT NOT NULL,
    data_col2 INT NOT NULL,
    data_col3 INT NOT NULL,
    data_col4 INT NOT NULL,
    data_col5 INT NOT NULL
) 
DUPLICATE KEY(filter_col1, filter_col2)
DISTRIBUTED BY HASH(filter_col1) BUCKETS 32
PROPERTIES (
    "replication_num" = "3",
    "compression" = "LZ4"
);

-- 表4：谓词列多 + 变长字符类型
DROP TABLE IF EXISTS test_predicate_many_varchar;
CREATE TABLE test_predicate_many_varchar (
    -- 谓词列 (10个)
    filter_col1 VARCHAR(200) NOT NULL,
    filter_col2 VARCHAR(200) NOT NULL,
    filter_col3 VARCHAR(200) NOT NULL,
    filter_col4 VARCHAR(200) NOT NULL,
    filter_col5 VARCHAR(200) NOT NULL,
    filter_col6 VARCHAR(200) NOT NULL,
    filter_col7 VARCHAR(200) NOT NULL,
    filter_col8 VARCHAR(200) NOT NULL,
    filter_col9 VARCHAR(200) NOT NULL,
    filter_col10 VARCHAR(200) NOT NULL,
    
    -- 非谓词列 (5个)
    data_col1 VARCHAR(200) NOT NULL,
    data_col2 VARCHAR(200) NOT NULL,
    data_col3 VARCHAR(200) NOT NULL,
    data_col4 VARCHAR(200) NOT NULL,
    data_col5 VARCHAR(200) NOT NULL
) 
DUPLICATE KEY(filter_col1, filter_col2)
DISTRIBUTED BY HASH(filter_col1) BUCKETS 32
PROPERTIES (
    "replication_num" = "3",
    "compression" = "LZ4"
);

-- 显示创建的表
SHOW TABLES LIKE 'test_predicate_%';