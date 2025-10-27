-- =====================================================
-- 谓词列延迟物化性能测试 - 查询脚本
-- =====================================================

-- =====================================================
-- 配置A：开启谓词列延迟物化 + 强制延迟物化
-- =====================================================
-- SET enable_predicate_col_late_materialize = true;
-- SET late_materialization_ratio = 1000;

-- =====================================================
-- 配置B：关闭谓词列延迟物化 + 默认延迟物化
-- =====================================================
-- SET enable_predicate_col_late_materialize = false;
-- SET late_materialization_ratio = 10;

-- =====================================================
-- 测试场景1：谓词列少 + 定长字符类型
-- =====================================================

-- 1.1 高过滤性查询 (预期返回约10万行)
SELECT /*+ SET_VAR(query_timeout=300) */ 
    data_col1, data_col2, data_col3, data_col4, data_col5, 
    data_col6, data_col7, data_col8, data_col9, data_col10,
    data_col11, data_col12, data_col13, data_col14, data_col15,
    data_col16, data_col17, data_col18, data_col19, data_col20
FROM test_predicate_few_char 
WHERE filter_col1 = 'rare_value_1' AND filter_col2 = 'rare_value_1';

-- 1.2 中等过滤性查询 (预期返回约100万行)
SELECT /*+ SET_VAR(query_timeout=300) */ 
    data_col1, data_col2, data_col3, data_col4, data_col5,
    data_col6, data_col7, data_col8, data_col9, data_col10,
    data_col11, data_col12, data_col13, data_col14, data_col15,
    data_col16, data_col17, data_col18, data_col19, data_col20
FROM test_predicate_few_char 
WHERE filter_col1 = 'medium_value_1' AND filter_col2 = 'medium_value_1';

-- 1.3 低过滤性查询 (预期返回约500万行)
SELECT /*+ SET_VAR(query_timeout=300) */ 
    data_col1, data_col2, data_col3, data_col4, data_col5,
    data_col6, data_col7, data_col8, data_col9, data_col10,
    data_col11, data_col12, data_col13, data_col14, data_col15,
    data_col16, data_col17, data_col18, data_col19, data_col20
FROM test_predicate_few_char 
WHERE filter_col1 = 'common_value_1' AND filter_col2 = 'common_value_1';

-- =====================================================
-- 测试场景2：谓词列少 + 变长字符类型
-- =====================================================

-- 2.1 高过滤性查询
SELECT /*+ SET_VAR(query_timeout=300) */ 
    data_col1, data_col2, data_col3, data_col4, data_col5, 
    data_col6, data_col7, data_col8, data_col9, data_col10,
    data_col11, data_col12, data_col13, data_col14, data_col15,
    data_col16, data_col17, data_col18, data_col19, data_col20
FROM test_predicate_few_varchar 
WHERE filter_col1 LIKE 'rare_value_1_%' AND filter_col2 LIKE 'rare_value_1_%';

-- 2.2 中等过滤性查询
SELECT /*+ SET_VAR(query_timeout=300) */ 
    data_col1, data_col2, data_col3, data_col4, data_col5,
    data_col6, data_col7, data_col8, data_col9, data_col10,
    data_col11, data_col12, data_col13, data_col14, data_col15,
    data_col16, data_col17, data_col18, data_col19, data_col20
FROM test_predicate_few_varchar 
WHERE filter_col1 LIKE 'medium_value_1_%' AND filter_col2 LIKE 'medium_value_1_%';

-- 2.3 低过滤性查询
SELECT /*+ SET_VAR(query_timeout=300) */ 
    data_col1, data_col2, data_col3, data_col4, data_col5,
    data_col6, data_col7, data_col8, data_col9, data_col10,
    data_col11, data_col12, data_col13, data_col14, data_col15,
    data_col16, data_col17, data_col18, data_col19, data_col20
FROM test_predicate_few_varchar 
WHERE filter_col1 LIKE 'common_value_1_%' AND filter_col2 LIKE 'common_value_1_%';

-- =====================================================
-- 测试场景3：谓词列多 + 定长字符类型
-- =====================================================

-- 3.1 高过滤性查询
SELECT /*+ SET_VAR(query_timeout=300) */ 
    data_col1, data_col2, data_col3, data_col4, data_col5
FROM test_predicate_many_char 
WHERE filter_col1 = 'rare_value_1' 
  AND filter_col2 = 'rare_value_1'
  AND filter_col3 = 'rare_value_1'
  AND filter_col4 = 'rare_value_1'
  AND filter_col5 = 'rare_value_1'
  AND filter_col6 = 'rare_value_1'
  AND filter_col7 = 'rare_value_1'
  AND filter_col8 = 'rare_value_1'
  AND filter_col9 = 'rare_value_1'
  AND filter_col10 = 'rare_value_1';

-- 3.2 中等过滤性查询
SELECT /*+ SET_VAR(query_timeout=300) */ 
    data_col1, data_col2, data_col3, data_col4, data_col5
FROM test_predicate_many_char 
WHERE filter_col1 = 'medium_value_1' 
  AND filter_col2 = 'medium_value_1'
  AND filter_col3 = 'medium_value_1'
  AND filter_col4 = 'medium_value_1'
  AND filter_col5 = 'medium_value_1'
  AND filter_col6 = 'medium_value_1'
  AND filter_col7 = 'medium_value_1'
  AND filter_col8 = 'medium_value_1'
  AND filter_col9 = 'medium_value_1'
  AND filter_col10 = 'medium_value_1';

-- 3.3 低过滤性查询
SELECT /*+ SET_VAR(query_timeout=300) */ 
    data_col1, data_col2, data_col3, data_col4, data_col5
FROM test_predicate_many_char 
WHERE filter_col1 = 'common_value_1' 
  AND filter_col2 = 'common_value_1'
  AND filter_col3 = 'common_value_1'
  AND filter_col4 = 'common_value_1'
  AND filter_col5 = 'common_value_1'
  AND filter_col6 = 'common_value_1'
  AND filter_col7 = 'common_value_1'
  AND filter_col8 = 'common_value_1'
  AND filter_col9 = 'common_value_1'
  AND filter_col10 = 'common_value_1';

-- =====================================================
-- 测试场景4：谓词列多 + 变长字符类型
-- =====================================================

-- 4.1 高过滤性查询
SELECT /*+ SET_VAR(query_timeout=300) */ 
    data_col1, data_col2, data_col3, data_col4, data_col5
FROM test_predicate_many_varchar 
WHERE filter_col1 LIKE 'rare_value_1_%' 
  AND filter_col2 LIKE 'rare_value_1_%'
  AND filter_col3 LIKE 'rare_value_1_%'
  AND filter_col4 LIKE 'rare_value_1_%'
  AND filter_col5 LIKE 'rare_value_1_%'
  AND filter_col6 LIKE 'rare_value_1_%'
  AND filter_col7 LIKE 'rare_value_1_%'
  AND filter_col8 LIKE 'rare_value_1_%'
  AND filter_col9 LIKE 'rare_value_1_%'
  AND filter_col10 LIKE 'rare_value_1_%';

-- 4.2 中等过滤性查询
SELECT /*+ SET_VAR(query_timeout=300) */ 
    data_col1, data_col2, data_col3, data_col4, data_col5
FROM test_predicate_many_varchar 
WHERE filter_col1 LIKE 'medium_value_1_%' 
  AND filter_col2 LIKE 'medium_value_1_%'
  AND filter_col3 LIKE 'medium_value_1_%'
  AND filter_col4 LIKE 'medium_value_1_%'
  AND filter_col5 LIKE 'medium_value_1_%'
  AND filter_col6 LIKE 'medium_value_1_%'
  AND filter_col7 LIKE 'medium_value_1_%'
  AND filter_col8 LIKE 'medium_value_1_%'
  AND filter_col9 LIKE 'medium_value_1_%'
  AND filter_col10 LIKE 'medium_value_1_%';

-- 4.3 低过滤性查询
SELECT /*+ SET_VAR(query_timeout=300) */ 
    data_col1, data_col2, data_col3, data_col4, data_col5
FROM test_predicate_many_varchar 
WHERE filter_col1 LIKE 'common_value_1_%' 
  AND filter_col2 LIKE 'common_value_1_%'
  AND filter_col3 LIKE 'common_value_1_%'
  AND filter_col4 LIKE 'common_value_1_%'
  AND filter_col5 LIKE 'common_value_1_%'
  AND filter_col6 LIKE 'common_value_1_%'
  AND filter_col7 LIKE 'common_value_1_%'
  AND filter_col8 LIKE 'common_value_1_%'
  AND filter_col9 LIKE 'common_value_1_%'
  AND filter_col10 LIKE 'common_value_1_%';

-- =====================================================
-- 额外的聚合查询测试（减少网络传输影响）
-- =====================================================

-- 聚合查询1：谓词列少 + 定长字符 + 高过滤性
SELECT /*+ SET_VAR(query_timeout=300) */ 
    COUNT(*) as row_count,
    COUNT(DISTINCT data_col1) as distinct_data1,
    AVG(LENGTH(data_col5)) as avg_length
FROM test_predicate_few_char 
WHERE filter_col1 = 'rare_value_1' AND filter_col2 = 'rare_value_1';

-- 聚合查询2：谓词列少 + 变长字符 + 中等过滤性
SELECT /*+ SET_VAR(query_timeout=300) */ 
    COUNT(*) as row_count,
    COUNT(DISTINCT data_col1) as distinct_data1,
    AVG(LENGTH(data_col5)) as avg_length
FROM test_predicate_few_varchar 
WHERE filter_col1 LIKE 'medium_value_1_%' AND filter_col2 LIKE 'medium_value_1_%';

-- 聚合查询3：谓词列多 + 定长字符 + 高过滤性
SELECT /*+ SET_VAR(query_timeout=300) */ 
    COUNT(*) as row_count,
    COUNT(DISTINCT data_col1) as distinct_data1,
    AVG(LENGTH(data_col3)) as avg_length
FROM test_predicate_many_char 
WHERE filter_col1 = 'rare_value_1' 
  AND filter_col2 = 'rare_value_1'
  AND filter_col3 = 'rare_value_1'
  AND filter_col4 = 'rare_value_1'
  AND filter_col5 = 'rare_value_1'
  AND filter_col6 = 'rare_value_1'
  AND filter_col7 = 'rare_value_1'
  AND filter_col8 = 'rare_value_1'
  AND filter_col9 = 'rare_value_1'
  AND filter_col10 = 'rare_value_1';

-- 聚合查询4：谓词列多 + 变长字符 + 低过滤性
SELECT /*+ SET_VAR(query_timeout=300) */ 
    COUNT(*) as row_count,
    COUNT(DISTINCT data_col1) as distinct_data1,
    AVG(LENGTH(data_col3)) as avg_length
FROM test_predicate_many_varchar 
WHERE filter_col1 LIKE 'common_value_1_%' 
  AND filter_col2 LIKE 'common_value_1_%'
  AND filter_col3 LIKE 'common_value_1_%'
  AND filter_col4 LIKE 'common_value_1_%'
  AND filter_col5 LIKE 'common_value_1_%'
  AND filter_col6 LIKE 'common_value_1_%'
  AND filter_col7 LIKE 'common_value_1_%'
  AND filter_col8 LIKE 'common_value_1_%'
  AND filter_col9 LIKE 'common_value_1_%'
  AND filter_col10 LIKE 'common_value_1_%';