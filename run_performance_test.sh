#!/bin/bash

# =====================================================
# 谓词列延迟物化性能测试执行脚本
# =====================================================

# 配置参数
MYSQL_HOST=${MYSQL_HOST:-"127.0.0.1"}
MYSQL_PORT=${MYSQL_PORT:-"9030"}
MYSQL_USER=${MYSQL_USER:-"root"}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-""}
DATABASE=${DATABASE:-"test_db"}

# 测试结果文件
RESULT_DIR="./test_results"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULT_FILE="$RESULT_DIR/performance_test_${TIMESTAMP}.txt"

# 创建结果目录
mkdir -p $RESULT_DIR

# MySQL连接命令
MYSQL_CMD="mysql -h$MYSQL_HOST -P$MYSQL_PORT -u$MYSQL_USER"
if [ -n "$MYSQL_PASSWORD" ]; then
    MYSQL_CMD="$MYSQL_CMD -p$MYSQL_PASSWORD"
fi
MYSQL_CMD="$MYSQL_CMD $DATABASE"

# 日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a $RESULT_FILE
}

# 执行SQL并记录时间
execute_sql() {
    local sql="$1"
    local description="$2"
    local config="$3"
    
    log "=========================================="
    log "测试: $description"
    log "配置: $config"
    log "SQL: $sql"
    log "=========================================="
    
    # 清理缓存
    echo "SELECT sleep(2);" | $MYSQL_CMD > /dev/null 2>&1
    
    # 执行查询并记录时间
    start_time=$(date +%s.%N)
    result=$(echo "$sql" | $MYSQL_CMD 2>&1)
    end_time=$(date +%s.%N)
    
    # 计算执行时间
    execution_time=$(echo "$end_time - $start_time" | bc)
    
    log "执行时间: ${execution_time}秒"
    
    # 检查是否有错误
    if echo "$result" | grep -i "error" > /dev/null; then
        log "错误: $result"
    else
        # 记录返回行数（如果是SELECT查询）
        if echo "$sql" | grep -i "select" > /dev/null; then
            row_count=$(echo "$result" | wc -l)
            log "返回行数: $((row_count - 1))"  # 减去表头
        fi
    fi
    
    log ""
    
    # 返回执行时间供后续使用
    echo $execution_time
}

# 测试场景定义
declare -a test_scenarios=(
    "谓词列少+定长数值+高过滤性"
    "谓词列少+定长数值+中等过滤性"  
    "谓词列少+定长数值+低过滤性"
    "谓词列少+变长字符+高过滤性"
    "谓词列少+变长字符+中等过滤性"
    "谓词列少+变长字符+低过滤性"
    "谓词列多+定长数值+高过滤性"
    "谓词列多+定长数值+中等过滤性"
    "谓词列多+定长数值+低过滤性"
    "谓词列多+变长字符+高过滤性"
    "谓词列多+变长字符+中等过滤性"
    "谓词列多+变长字符+低过滤性"
)

declare -a test_queries=(
    "SELECT data_col1, data_col2, data_col3, data_col4, data_col5, data_col6, data_col7, data_col8, data_col9, data_col10, data_col11, data_col12, data_col13, data_col14, data_col15, data_col16, data_col17, data_col18, data_col19, data_col20 FROM test_predicate_few_int WHERE filter_col1 = 1000001 AND filter_col2 = 1000001;"
    
    "SELECT data_col1, data_col2, data_col3, data_col4, data_col5, data_col6, data_col7, data_col8, data_col9, data_col10, data_col11, data_col12, data_col13, data_col14, data_col15, data_col16, data_col17, data_col18, data_col19, data_col20 FROM test_predicate_few_int WHERE filter_col1 = 2000001 AND filter_col2 = 2000001;"
    
    "SELECT data_col1, data_col2, data_col3, data_col4, data_col5, data_col6, data_col7, data_col8, data_col9, data_col10, data_col11, data_col12, data_col13, data_col14, data_col15, data_col16, data_col17, data_col18, data_col19, data_col20 FROM test_predicate_few_int WHERE filter_col1 = 3000001 AND filter_col2 = 3000001;"
    
    "SELECT data_col1, data_col2, data_col3, data_col4, data_col5, data_col6, data_col7, data_col8, data_col9, data_col10, data_col11, data_col12, data_col13, data_col14, data_col15, data_col16, data_col17, data_col18, data_col19, data_col20 FROM test_predicate_few_varchar WHERE filter_col1 LIKE 'rare_value_1_%' AND filter_col2 LIKE 'rare_value_1_%';"
    
    "SELECT data_col1, data_col2, data_col3, data_col4, data_col5, data_col6, data_col7, data_col8, data_col9, data_col10, data_col11, data_col12, data_col13, data_col14, data_col15, data_col16, data_col17, data_col18, data_col19, data_col20 FROM test_predicate_few_varchar WHERE filter_col1 LIKE 'medium_value_1_%' AND filter_col2 LIKE 'medium_value_1_%';"
    
    "SELECT data_col1, data_col2, data_col3, data_col4, data_col5, data_col6, data_col7, data_col8, data_col9, data_col10, data_col11, data_col12, data_col13, data_col14, data_col15, data_col16, data_col17, data_col18, data_col19, data_col20 FROM test_predicate_few_varchar WHERE filter_col1 LIKE 'common_value_1_%' AND filter_col2 LIKE 'common_value_1_%';"
    
    "SELECT data_col1, data_col2, data_col3, data_col4, data_col5 FROM test_predicate_many_int WHERE filter_col1 = 1000001 AND filter_col2 = 1000001 AND filter_col3 = 1000001 AND filter_col4 = 1000001 AND filter_col5 = 1000001 AND filter_col6 = 1000001 AND filter_col7 = 1000001 AND filter_col8 = 1000001 AND filter_col9 = 1000001 AND filter_col10 = 1000001;"
    
    "SELECT data_col1, data_col2, data_col3, data_col4, data_col5 FROM test_predicate_many_int WHERE filter_col1 = 2000001 AND filter_col2 = 2000001 AND filter_col3 = 2000001 AND filter_col4 = 2000001 AND filter_col5 = 2000001 AND filter_col6 = 2000001 AND filter_col7 = 2000001 AND filter_col8 = 2000001 AND filter_col9 = 2000001 AND filter_col10 = 2000001;"
    
    "SELECT data_col1, data_col2, data_col3, data_col4, data_col5 FROM test_predicate_many_int WHERE filter_col1 = 3000001 AND filter_col2 = 3000001 AND filter_col3 = 3000001 AND filter_col4 = 3000001 AND filter_col5 = 3000001 AND filter_col6 = 3000001 AND filter_col7 = 3000001 AND filter_col8 = 3000001 AND filter_col9 = 3000001 AND filter_col10 = 3000001;"
    
    "SELECT data_col1, data_col2, data_col3, data_col4, data_col5 FROM test_predicate_many_varchar WHERE filter_col1 LIKE 'rare_value_1_%' AND filter_col2 LIKE 'rare_value_1_%' AND filter_col3 LIKE 'rare_value_1_%' AND filter_col4 LIKE 'rare_value_1_%' AND filter_col5 LIKE 'rare_value_1_%' AND filter_col6 LIKE 'rare_value_1_%' AND filter_col7 LIKE 'rare_value_1_%' AND filter_col8 LIKE 'rare_value_1_%' AND filter_col9 LIKE 'rare_value_1_%' AND filter_col10 LIKE 'rare_value_1_%';"
    
    "SELECT data_col1, data_col2, data_col3, data_col4, data_col5 FROM test_predicate_many_varchar WHERE filter_col1 LIKE 'medium_value_1_%' AND filter_col2 LIKE 'medium_value_1_%' AND filter_col3 LIKE 'medium_value_1_%' AND filter_col4 LIKE 'medium_value_1_%' AND filter_col5 LIKE 'medium_value_1_%' AND filter_col6 LIKE 'medium_value_1_%' AND filter_col7 LIKE 'medium_value_1_%' AND filter_col8 LIKE 'medium_value_1_%' AND filter_col9 LIKE 'medium_value_1_%' AND filter_col10 LIKE 'medium_value_1_%';"
    
    "SELECT data_col1, data_col2, data_col3, data_col4, data_col5 FROM test_predicate_many_varchar WHERE filter_col1 LIKE 'common_value_1_%' AND filter_col2 LIKE 'common_value_1_%' AND filter_col3 LIKE 'common_value_1_%' AND filter_col4 LIKE 'common_value_1_%' AND filter_col5 LIKE 'common_value_1_%' AND filter_col6 LIKE 'common_value_1_%' AND filter_col7 LIKE 'common_value_1_%' AND filter_col8 LIKE 'common_value_1_%' AND filter_col9 LIKE 'common_value_1_%' AND filter_col10 LIKE 'common_value_1_%';"
)

# 开始测试
log "======================================================" 
log "谓词列延迟物化性能测试开始"
log "时间: $(date)"
log "======================================================"

# 检查表是否存在
log "检查测试表是否存在..."
table_check=$(echo "SHOW TABLES LIKE 'test_predicate_%';" | $MYSQL_CMD 2>&1)
if echo "$table_check" | grep -q "test_predicate_"; then
    log "测试表已存在，开始性能测试"
else
    log "错误: 测试表不存在，请先运行建表和数据生成脚本"
    exit 1
fi

# 存储两种配置的测试结果
declare -A config_a_results
declare -A config_b_results

# 配置A测试：开启谓词列延迟物化
log "======================================================"
log "开始配置A测试：开启谓词列延迟物化 + 强制延迟物化"
log "======================================================"

config_a_setup="SET enable_predicate_col_late_materialize = true; SET late_materialization_ratio = 1000;"
echo "$config_a_setup" | $MYSQL_CMD

for i in "${!test_scenarios[@]}"; do
    scenario="${test_scenarios[$i]}"
    query="${test_queries[$i]}"
    
    # 执行3次取平均值
    total_time=0
    for run in {1..3}; do
        log "配置A - $scenario - 第${run}次执行"
        time_result=$(execute_sql "$query" "$scenario" "配置A(开启谓词列延迟物化)")
        total_time=$(echo "$total_time + $time_result" | bc)
    done
    
    avg_time=$(echo "scale=3; $total_time / 3" | bc)
    config_a_results["$scenario"]=$avg_time
    log "配置A - $scenario - 平均执行时间: ${avg_time}秒"
done

# 配置B测试：关闭谓词列延迟物化
log "======================================================"
log "开始配置B测试：关闭谓词列延迟物化 + 默认延迟物化"
log "======================================================"

config_b_setup="SET enable_predicate_col_late_materialize = false; SET late_materialization_ratio = 10;"
echo "$config_b_setup" | $MYSQL_CMD

for i in "${!test_scenarios[@]}"; do
    scenario="${test_scenarios[$i]}"
    query="${test_queries[$i]}"
    
    # 执行3次取平均值
    total_time=0
    for run in {1..3}; do
        log "配置B - $scenario - 第${run}次执行"
        time_result=$(execute_sql "$query" "$scenario" "配置B(关闭谓词列延迟物化)")
        total_time=$(echo "$total_time + $time_result" | bc)
    done
    
    avg_time=$(echo "scale=3; $total_time / 3" | bc)
    config_b_results["$scenario"]=$avg_time
    log "配置B - $scenario - 平均执行时间: ${avg_time}秒"
done

# 生成对比报告
log "======================================================"
log "性能对比报告"
log "======================================================"

printf "%-40s %-15s %-15s %-15s %-10s\n" "测试场景" "配置A(秒)" "配置B(秒)" "性能提升" "推荐配置" | tee -a $RESULT_FILE
printf "%-40s %-15s %-15s %-15s %-10s\n" "----------------------------------------" "---------------" "---------------" "---------------" "----------" | tee -a $RESULT_FILE

for scenario in "${test_scenarios[@]}"; do
    time_a="${config_a_results[$scenario]}"
    time_b="${config_b_results[$scenario]}"
    
    # 计算性能提升百分比
    if (( $(echo "$time_b > 0" | bc -l) )); then
        improvement=$(echo "scale=1; ($time_b - $time_a) / $time_b * 100" | bc)
        if (( $(echo "$improvement > 0" | bc -l) )); then
            improvement_str="+${improvement}%"
            recommended="配置A"
        else
            improvement_str="${improvement}%"
            recommended="配置B"
        fi
    else
        improvement_str="N/A"
        recommended="N/A"
    fi
    
    printf "%-40s %-15s %-15s %-15s %-10s\n" "$scenario" "$time_a" "$time_b" "$improvement_str" "$recommended" | tee -a $RESULT_FILE
done

log "======================================================"
log "测试完成！结果已保存到: $RESULT_FILE"
log "======================================================"

# 生成CSV格式的结果文件
CSV_FILE="$RESULT_DIR/performance_test_${TIMESTAMP}.csv"
echo "测试场景,配置A执行时间(秒),配置B执行时间(秒),性能提升(%),推荐配置" > $CSV_FILE

for scenario in "${test_scenarios[@]}"; do
    time_a="${config_a_results[$scenario]}"
    time_b="${config_b_results[$scenario]}"
    
    if (( $(echo "$time_b > 0" | bc -l) )); then
        improvement=$(echo "scale=1; ($time_b - $time_a) / $time_b * 100" | bc)
        if (( $(echo "$improvement > 0" | bc -l) )); then
            recommended="配置A"
        else
            recommended="配置B"
        fi
    else
        improvement="N/A"
        recommended="N/A"
    fi
    
    echo "$scenario,$time_a,$time_b,$improvement,$recommended" >> $CSV_FILE
done

log "CSV格式结果已保存到: $CSV_FILE"