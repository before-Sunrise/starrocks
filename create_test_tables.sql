-- =====================================================
-- 谓词列延迟物化性能测试 - 建表脚本
-- =====================================================

-- 表1：谓词列少 + 定长字符类型
DROP TABLE IF EXISTS test_predicate_few_char;
CREATE TABLE test_predicate_few_char (
    -- 谓词列 (2个)
    filter_col1 CHAR(50) NOT NULL,
    filter_col2 CHAR(50) NOT NULL,
    
    -- 非谓词列 (20个)
    data_col1 CHAR(50) NOT NULL,
    data_col2 CHAR(50) NOT NULL,
    data_col3 CHAR(50) NOT NULL,
    data_col4 CHAR(50) NOT NULL,
    data_col5 CHAR(50) NOT NULL,
    data_col6 CHAR(50) NOT NULL,
    data_col7 CHAR(50) NOT NULL,
    data_col8 CHAR(50) NOT NULL,
    data_col9 CHAR(50) NOT NULL,
    data_col10 CHAR(50) NOT NULL,
    data_col11 CHAR(50) NOT NULL,
    data_col12 CHAR(50) NOT NULL,
    data_col13 CHAR(50) NOT NULL,
    data_col14 CHAR(50) NOT NULL,
    data_col15 CHAR(50) NOT NULL,
    data_col16 CHAR(50) NOT NULL,
    data_col17 CHAR(50) NOT NULL,
    data_col18 CHAR(50) NOT NULL,
    data_col19 CHAR(50) NOT NULL,
    data_col20 CHAR(50) NOT NULL
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

-- 表3：谓词列多 + 定长字符类型
DROP TABLE IF EXISTS test_predicate_many_char;
CREATE TABLE test_predicate_many_char (
    -- 谓词列 (10个)
    filter_col1 CHAR(50) NOT NULL,
    filter_col2 CHAR(50) NOT NULL,
    filter_col3 CHAR(50) NOT NULL,
    filter_col4 CHAR(50) NOT NULL,
    filter_col5 CHAR(50) NOT NULL,
    filter_col6 CHAR(50) NOT NULL,
    filter_col7 CHAR(50) NOT NULL,
    filter_col8 CHAR(50) NOT NULL,
    filter_col9 CHAR(50) NOT NULL,
    filter_col10 CHAR(50) NOT NULL,
    
    -- 非谓词列 (5个)
    data_col1 CHAR(50) NOT NULL,
    data_col2 CHAR(50) NOT NULL,
    data_col3 CHAR(50) NOT NULL,
    data_col4 CHAR(50) NOT NULL,
    data_col5 CHAR(50) NOT NULL
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