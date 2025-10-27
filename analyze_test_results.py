#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
谓词列延迟物化性能测试结果分析脚本
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import argparse
import os
from datetime import datetime

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

def load_test_results(csv_file):
    """加载测试结果CSV文件"""
    try:
        df = pd.read_csv(csv_file, encoding='utf-8')
        return df
    except Exception as e:
        print(f"加载CSV文件失败: {e}")
        return None

def parse_scenario(scenario):
    """解析测试场景，提取维度信息"""
    parts = scenario.split('+')
    
    # 提取列数量比例
    if '谓词列少' in parts[0]:
        column_ratio = '谓词列少(1:10)'
    elif '谓词列多' in parts[0]:
        column_ratio = '谓词列多(2:1)'
    else:
        column_ratio = '未知'
    
    # 提取数据类型
    if '定长字符' in parts[1]:
        data_type = 'CHAR'
    elif '变长字符' in parts[1]:
        data_type = 'VARCHAR'
    else:
        data_type = '未知'
    
    # 提取过滤性
    if '高过滤性' in parts[2]:
        selectivity = '高过滤性(1%)'
    elif '中等过滤性' in parts[2]:
        selectivity = '中等过滤性(10%)'
    elif '低过滤性' in parts[2]:
        selectivity = '低过滤性(50%)'
    else:
        selectivity = '未知'
    
    return column_ratio, data_type, selectivity

def analyze_results(df):
    """分析测试结果"""
    print("=" * 60)
    print("谓词列延迟物化性能测试结果分析")
    print("=" * 60)
    
    # 解析场景维度
    df['列数量比例'] = df['测试场景'].apply(lambda x: parse_scenario(x)[0])
    df['数据类型'] = df['测试场景'].apply(lambda x: parse_scenario(x)[1])
    df['过滤性'] = df['测试场景'].apply(lambda x: parse_scenario(x)[2])
    
    # 转换性能提升为数值
    df['性能提升数值'] = df['性能提升(%)'].apply(lambda x: float(x) if x != 'N/A' else 0)
    
    # 总体统计
    print("\n1. 总体性能对比:")
    print(f"   总测试场景数: {len(df)}")
    config_a_better = len(df[df['推荐配置'] == '配置A'])
    config_b_better = len(df[df['推荐配置'] == '配置B'])
    print(f"   配置A(延迟物化)更优: {config_a_better} 个场景")
    print(f"   配置B(传统方式)更优: {config_b_better} 个场景")
    
    avg_improvement = df[df['性能提升数值'] != 0]['性能提升数值'].mean()
    print(f"   平均性能提升: {avg_improvement:.1f}%")
    
    # 按维度分析
    print("\n2. 按过滤性分析:")
    selectivity_analysis = df.groupby('过滤性').agg({
        '性能提升数值': 'mean',
        '推荐配置': lambda x: (x == '配置A').sum()
    }).round(1)
    selectivity_analysis.columns = ['平均性能提升(%)', '配置A优势场景数']
    print(selectivity_analysis)
    
    print("\n3. 按列数量比例分析:")
    column_analysis = df.groupby('列数量比例').agg({
        '性能提升数值': 'mean',
        '推荐配置': lambda x: (x == '配置A').sum()
    }).round(1)
    column_analysis.columns = ['平均性能提升(%)', '配置A优势场景数']
    print(column_analysis)
    
    print("\n4. 按数据类型分析:")
    datatype_analysis = df.groupby('数据类型').agg({
        '性能提升数值': 'mean',
        '推荐配置': lambda x: (x == '配置A').sum()
    }).round(1)
    datatype_analysis.columns = ['平均性能提升(%)', '配置A优势场景数']
    print(datatype_analysis)
    
    # 详细场景分析
    print("\n5. 详细场景分析:")
    detailed_analysis = df.groupby(['过滤性', '列数量比例', '数据类型']).agg({
        '性能提升数值': 'mean',
        '推荐配置': 'first'
    }).round(1)
    print(detailed_analysis)
    
    return df

def create_visualizations(df, output_dir):
    """创建可视化图表"""
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 设置图表样式
    plt.style.use('seaborn-v0_8')
    
    # 1. 总体性能对比柱状图
    fig, ax = plt.subplots(figsize=(12, 8))
    
    scenarios = df['测试场景'].str.replace('谓词列', '谓词列\n', regex=False)
    colors = ['green' if x == '配置A' else 'red' for x in df['推荐配置']]
    
    bars = ax.bar(range(len(df)), df['性能提升数值'], color=colors, alpha=0.7)
    ax.set_xlabel('测试场景')
    ax.set_ylabel('性能提升 (%)')
    ax.set_title('谓词列延迟物化性能测试结果\n(正值表示配置A更优，负值表示配置B更优)')
    ax.set_xticks(range(len(df)))
    ax.set_xticklabels(scenarios, rotation=45, ha='right')
    ax.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    ax.grid(True, alpha=0.3)
    
    # 添加数值标签
    for i, bar in enumerate(bars):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + (1 if height >= 0 else -3),
                f'{height:.1f}%', ha='center', va='bottom' if height >= 0 else 'top')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/overall_performance_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. 按维度分组的热力图
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    
    # 过滤性 vs 列数量比例
    pivot1 = df.pivot_table(values='性能提升数值', index='过滤性', columns='列数量比例', aggfunc='mean')
    sns.heatmap(pivot1, annot=True, fmt='.1f', cmap='RdYlGn', center=0, ax=axes[0,0])
    axes[0,0].set_title('过滤性 vs 列数量比例')
    
    # 过滤性 vs 数据类型
    pivot2 = df.pivot_table(values='性能提升数值', index='过滤性', columns='数据类型', aggfunc='mean')
    sns.heatmap(pivot2, annot=True, fmt='.1f', cmap='RdYlGn', center=0, ax=axes[0,1])
    axes[0,1].set_title('过滤性 vs 数据类型')
    
    # 列数量比例 vs 数据类型
    pivot3 = df.pivot_table(values='性能提升数值', index='列数量比例', columns='数据类型', aggfunc='mean')
    sns.heatmap(pivot3, annot=True, fmt='.1f', cmap='RdYlGn', center=0, ax=axes[1,0])
    axes[1,0].set_title('列数量比例 vs 数据类型')
    
    # 推荐配置分布
    config_counts = df.groupby(['过滤性', '推荐配置']).size().unstack(fill_value=0)
    config_counts.plot(kind='bar', ax=axes[1,1], color=['red', 'green'])
    axes[1,1].set_title('不同过滤性下的推荐配置分布')
    axes[1,1].set_xlabel('过滤性')
    axes[1,1].set_ylabel('场景数量')
    axes[1,1].legend(['配置A', '配置B'])
    axes[1,1].tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/dimension_analysis_heatmap.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 3. 执行时间对比图
    fig, ax = plt.subplots(figsize=(14, 8))
    
    x = np.arange(len(df))
    width = 0.35
    
    bars1 = ax.bar(x - width/2, df['配置A执行时间(秒)'], width, label='配置A(延迟物化)', alpha=0.8)
    bars2 = ax.bar(x + width/2, df['配置B执行时间(秒)'], width, label='配置B(传统方式)', alpha=0.8)
    
    ax.set_xlabel('测试场景')
    ax.set_ylabel('执行时间 (秒)')
    ax.set_title('两种配置的执行时间对比')
    ax.set_xticks(x)
    ax.set_xticklabels(scenarios, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/execution_time_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"\n可视化图表已保存到: {output_dir}/")

def generate_recommendations(df):
    """生成使用建议"""
    print("\n" + "=" * 60)
    print("使用建议")
    print("=" * 60)
    
    # 分析最佳使用场景
    config_a_scenarios = df[df['推荐配置'] == '配置A']
    config_b_scenarios = df[df['推荐配置'] == '配置B']
    
    print("\n1. 推荐使用谓词列延迟物化(配置A)的场景:")
    if len(config_a_scenarios) > 0:
        for _, row in config_a_scenarios.iterrows():
            print(f"   - {row['过滤性']} + {row['列数量比例']} + {row['数据类型']}: 性能提升 {row['性能提升数值']:.1f}%")
    else:
        print("   - 无明显优势场景")
    
    print("\n2. 推荐使用传统方式(配置B)的场景:")
    if len(config_b_scenarios) > 0:
        for _, row in config_b_scenarios.iterrows():
            print(f"   - {row['过滤性']} + {row['列数量比例']} + {row['数据类型']}: 性能劣化 {row['性能提升数值']:.1f}%")
    else:
        print("   - 无明显优势场景")
    
    # 总结性建议
    print("\n3. 总结性建议:")
    
    # 按过滤性分析
    high_selectivity_avg = df[df['过滤性'] == '高过滤性(1%)']['性能提升数值'].mean()
    medium_selectivity_avg = df[df['过滤性'] == '中等过滤性(10%)']['性能提升数值'].mean()
    low_selectivity_avg = df[df['过滤性'] == '低过滤性(50%)']['性能提升数值'].mean()
    
    print(f"   - 高过滤性场景平均性能提升: {high_selectivity_avg:.1f}%")
    print(f"   - 中等过滤性场景平均性能提升: {medium_selectivity_avg:.1f}%")
    print(f"   - 低过滤性场景平均性能提升: {low_selectivity_avg:.1f}%")
    
    if high_selectivity_avg > 0:
        print("   ✓ 在高过滤性场景下，延迟物化通常表现更好")
    if medium_selectivity_avg > 0:
        print("   ✓ 在中等过滤性场景下，延迟物化有一定优势")
    if low_selectivity_avg < 0:
        print("   ✗ 在低过滤性场景下，传统方式可能更优")
    
    # 按列比例分析
    few_columns_avg = df[df['列数量比例'] == '谓词列少(1:10)']['性能提升数值'].mean()
    many_columns_avg = df[df['列数量比例'] == '谓词列多(2:1)']['性能提升数值'].mean()
    
    print(f"   - 谓词列少场景平均性能提升: {few_columns_avg:.1f}%")
    print(f"   - 谓词列多场景平均性能提升: {many_columns_avg:.1f}%")
    
    if few_columns_avg > many_columns_avg:
        print("   ✓ 谓词列相对较少时，延迟物化优势更明显")
    else:
        print("   ✓ 谓词列较多时，延迟物化仍有优势")
    
    # 按数据类型分析
    char_avg = df[df['数据类型'] == 'CHAR']['性能提升数值'].mean()
    varchar_avg = df[df['数据类型'] == 'VARCHAR']['性能提升数值'].mean()
    
    print(f"   - CHAR类型场景平均性能提升: {char_avg:.1f}%")
    print(f"   - VARCHAR类型场景平均性能提升: {varchar_avg:.1f}%")
    
    if varchar_avg > char_avg:
        print("   ✓ VARCHAR类型数据上，延迟物化优势更明显")
    else:
        print("   ✓ CHAR类型数据上，延迟物化也有一定优势")

def main():
    parser = argparse.ArgumentParser(description='分析谓词列延迟物化性能测试结果')
    parser.add_argument('csv_file', help='测试结果CSV文件路径')
    parser.add_argument('--output-dir', default='./analysis_results', help='输出目录')
    
    args = parser.parse_args()
    
    # 加载数据
    df = load_test_results(args.csv_file)
    if df is None:
        return
    
    # 分析结果
    df = analyze_results(df)
    
    # 创建可视化
    create_visualizations(df, args.output_dir)
    
    # 生成建议
    generate_recommendations(df)
    
    # 保存分析报告
    report_file = f"{args.output_dir}/analysis_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    os.makedirs(args.output_dir, exist_ok=True)
    
    # 这里可以将分析结果保存到文件
    print(f"\n分析完成！结果已保存到: {args.output_dir}/")

if __name__ == '__main__':
    main()