# -*- coding: utf-8 -*-
"""
ETF股票池筛选模块：实现稳健仓和激进仓的ETF筛选逻辑
根据鱼盆模型策略要求，每周五更新股票池，各包含5只ETF
筛选规则基于流动性、规模、跟踪误差和行业属性
"""
import os
import json
import logging
import pandas as pd
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/etf_selector.log'), logging.StreamHandler()]
)
logger = logging.getLogger('etf_selector')

class ETFSelector:
    def __init__(self, data_source):
        """
        初始化ETF筛选器
        :param data_source: DataSource实例，用于获取ETF基础数据
        """
        self.data_source = data_source
        self.stock_pool_path = 'data/stock_pool'  # 股票池存储路径
        os.makedirs(self.stock_pool_path, exist_ok=True)
        
        # 筛选参数配置（可根据需求调整）
        self.params = {
            'stable': {  # 稳健仓参数
                'min_avg_volume': 2.0,    # 最小日均成交额（亿）
                'min_fund_size': 50.0,    # 最小基金规模（亿）
                'max_tracking_error': 0.01,  # 最大跟踪误差（1%）
                'preferred_industries': ['宽基', '消费', '医药', '红利']  # 优先行业
            },
            'aggressive': {  # 激进仓参数
                'min_avg_volume': 1.0,    # 最小日均成交额（亿）
                'min_fund_size': 5.0,     # 最小基金规模（亿）
                'max_tracking_error': 0.02,  # 最大跟踪误差（2%）
                'preferred_industries': ['科技', '半导体', '军工', '新能源', 'AI']  # 优先行业
            }
        }

    def select_stock_pool(self, force_refresh=False):
        """
        选择ETF股票池（稳健仓和激进仓各5只）
        :param force_refresh: bool, 是否强制刷新（忽略缓存）
        :return: dict, 包含稳健仓和激进仓的股票池
        """
        # 检查缓存是否有效（每周五更新）
        today = datetime.now()
        # 计算最近的周五
        last_friday = today - timedelta(days=(today.weekday() - 4) % 7)
        cache_date_str = last_friday.strftime('%Y%m%d')
        cache_file = os.path.join(self.stock_pool_path, f"stock_pool_{cache_date_str}.json")
        
        # 如果缓存有效且不强制刷新，则返回缓存
        if not force_refresh and os.path.exists(cache_file):
            try:
                with open(cache_file, 'r') as f:
                    stock_pool = json.load(f)
                logger.info(f"从缓存加载股票池，缓存日期: {cache_date_str}")
                return stock_pool
            except Exception as e:
                logger.warning(f"缓存文件损坏，重新计算: {str(e)}")

        # 获取ETF基础数据
        basic_df = self.data_source.get_etf_basic(force_refresh=force_refresh)
        if basic_df.empty:
            logger.error("无法获取ETF基础数据，无法筛选股票池")
            return {'stable': [], 'aggressive': []}

        # 筛选稳健仓和激进仓
        stable_pool = self._select_stable_pool(basic_df)
        aggressive_pool = self._select_aggressive_pool(basic_df)

        # 处理筛选结果为空的情况
        if stable_pool.empty:
            logger.warning("稳健仓筛选结果为空，使用默认宽基ETF")
            stable_pool = self._get_default_stable_pool(basic_df)
            
        if aggressive_pool.empty:
            logger.warning("激进仓筛选结果为空，使用默认行业ETF")
            aggressive_pool = self._get_default_aggressive_pool(basic_df)

        # 整合结果
        stock_pool = {
            'stable': stable_pool.to_dict('records'),
            'aggressive': aggressive_pool.to_dict('records'),
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }

        # 保存缓存
        with open(cache_file, 'w') as f:
            json.dump(stock_pool, f, ensure_ascii=False, indent=2)
        logger.info(f"股票池已保存至{cache_file}，稳健仓{len(stable_pool)}只，激进仓{len(aggressive_pool)}只")

        return stock_pool

    def _select_stable_pool(self, basic_df):
        """筛选稳健仓ETF"""
        params = self.params['stable']
        
        # 打印筛选前的基础数据统计，便于调试
        logger.info(f"稳健仓筛选前数据统计: 共{len(basic_df)}只ETF")
        logger.info(f"规模≥{params['min_fund_size']}亿: {len(basic_df[basic_df['fund_size'] >= params['min_fund_size']])}只")
        logger.info(f"成交额≥{params['min_avg_volume']}亿: {len(basic_df[basic_df['avg_volume'] >= params['min_avg_volume']])}只")
        logger.info(f"跟踪误差≤{params['max_tracking_error']*100}%: {len(basic_df[basic_df['tracking_error'] <= params['max_tracking_error']])}只")
        
        # 应用基础筛选条件
        filtered = basic_df[
            (basic_df['avg_volume'] >= params['min_avg_volume']) &
            (basic_df['fund_size'] >= params['min_fund_size']) &
            (basic_df['tracking_error'] <= params['max_tracking_error'])
        ].copy()

        if filtered.empty:
            logger.warning("应用所有筛选条件后无符合条件的稳健仓ETF，放宽跟踪误差条件")
            # 放宽跟踪误差条件
            filtered = basic_df[
                (basic_df['avg_volume'] >= params['min_avg_volume']) &
                (basic_df['fund_size'] >= params['min_fund_size']) &
                (basic_df['tracking_error'] <= params['max_tracking_error'] * 2)  # 放宽一倍
            ].copy()

        # 优先选择指定行业
        filtered['is_preferred'] = filtered['industry'].apply(
            lambda x: x in params['preferred_industries']
        )

        # 排序：优先行业 > 规模（降序）> 成交额（降序）> 跟踪误差（升序）
        filtered.sort_values(
            by=['is_preferred', 'fund_size', 'avg_volume', 'tracking_error'],
            ascending=[False, False, False, True],
            inplace=True
        )

        # 选择前5只
        selected = filtered.head(5).copy()
        
        # 添加选择理由
        if not selected.empty:
            selected['selection_reason'] = selected.apply(
                lambda row: f"规模{row['fund_size']}亿，日均成交{row['avg_volume']}亿，跟踪误差{row['tracking_error']*100}%",
                axis=1
            )

        logger.info(f"稳健仓筛选完成，共{len(selected)}只ETF")
        return selected[['etf_code', 'name', 'industry', 'fund_size', 'avg_volume', 'tracking_error', 'selection_reason']] if not selected.empty else selected

    def _select_aggressive_pool(self, basic_df):
        """筛选激进仓ETF"""
        params = self.params['aggressive']
        
        # 打印筛选前的基础数据统计，便于调试
        logger.info(f"激进仓筛选前数据统计: 共{len(basic_df)}只ETF")
        logger.info(f"规模≥{params['min_fund_size']}亿: {len(basic_df[basic_df['fund_size'] >= params['min_fund_size']])}只")
        logger.info(f"成交额≥{params['min_avg_volume']}亿: {len(basic_df[basic_df['avg_volume'] >= params['min_avg_volume']])}只")
        logger.info(f"跟踪误差≤{params['max_tracking_error']*100}%: {len(basic_df[basic_df['tracking_error'] <= params['max_tracking_error']])}只")
        
        # 应用基础筛选条件
        filtered = basic_df[
            (basic_df['avg_volume'] >= params['min_avg_volume']) &
            (basic_df['fund_size'] >= params['min_fund_size']) &
            (basic_df['tracking_error'] <= params['max_tracking_error'])
        ].copy()

        # 优先选择指定行业
        filtered['is_preferred'] = filtered['industry'].apply(
            lambda x: x in params['preferred_industries']
        )

        # 排序：优先行业 > 成交额（降序）> 规模（降序）> 跟踪误差（升序）
        filtered.sort_values(
            by=['is_preferred', 'avg_volume', 'fund_size', 'tracking_error'],
            ascending=[False, False, False, True],
            inplace=True
        )

        # 选择前5只
        selected = filtered.head(5).copy()
        
        # 添加选择理由
        if not selected.empty:
            selected['selection_reason'] = selected.apply(
                lambda row: f"{row['industry']}行业，日均成交{row['avg_volume']}亿，规模{row['fund_size']}亿",
                axis=1
            )

        logger.info(f"激进仓筛选完成，共{len(selected)}只ETF")
        return selected[['etf_code', 'name', 'industry', 'fund_size', 'avg_volume', 'tracking_error', 'selection_reason']] if not selected.empty else selected

    def _get_default_stable_pool(self, basic_df):
        """当筛选结果为空时，返回默认的稳健仓ETF"""
        default_codes = ['510300', '510500', '159915', '510880', '512000']  # 常见宽基ETF
        mask = basic_df['etf_code'].isin(default_codes)
        default_pool = basic_df[mask].copy()
        
        # 如果默认池中ETF不足5只，用其他ETF补充
        if len(default_pool) < 5:
            remaining = 5 - len(default_pool)
            supplement_df = basic_df[~mask].sort_values('fund_size', ascending=False).head(remaining)
            default_pool = pd.concat([default_pool, supplement_df])
            
        # 添加选择理由
        default_pool['selection_reason'] = '默认宽基ETF（主筛选条件无结果）'
        return default_pool[['etf_code', 'name', 'industry', 'fund_size', 'avg_volume', 'tracking_error', 'selection_reason']]

    def _get_default_aggressive_pool(self, basic_df):
        """当筛选结果为空时，返回默认的激进仓ETF"""
        default_codes = ['159813', '512760', '512660', '515030', '515790']  # 常见行业ETF
        mask = basic_df['etf_code'].isin(default_codes)
        default_pool = basic_df[mask].copy()
        
        # 如果默认池中ETF不足5只，用其他ETF补充
        if len(default_pool) < 5:
            remaining = 5 - len(default_pool)
            supplement_df = basic_df[~mask].sort_values('avg_volume', ascending=False).head(remaining)
            default_pool = pd.concat([default_pool, supplement_df])
            
        # 添加选择理由
        default_pool['selection_reason'] = '默认行业ETF（主筛选条件无结果）'
        return default_pool[['etf_code', 'name', 'industry', 'fund_size', 'avg_volume', 'tracking_error', 'selection_reason']]

    def get_current_pool(self):
        """获取当前股票池（从缓存）"""
        try:
            # 查找最新的缓存文件
            cache_files = [f for f in os.listdir(self.stock_pool_path) if f.startswith('stock_pool_')]
            if not cache_files:
                logger.warning("没有找到股票池缓存文件")
                return {'stable': [], 'aggressive': []}
            
            # 按日期排序，取最新的
            cache_files.sort(reverse=True)
            latest_cache = cache_files[0]
            
            with open(os.path.join(self.stock_pool_path, latest_cache), 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"获取当前股票池失败: {str(e)}")
            return {'stable': [], 'aggressive': []}

# 测试代码
if __name__ == "__main__":
    # 创建数据源实例
    from data_source_integration import DataSource
    ds = DataSource()
    
    # 创建ETF选择器实例
    selector = ETFSelector(ds)
    
    # 筛选股票池
    stock_pool = selector.select_stock_pool(force_refresh=True)
    
    # 打印结果
    print("\n=== 稳健仓ETF（5只） ===")
    for etf in stock_pool['stable'][:5]:  # 确保只打印5只
        print(f"{etf['etf_code']} {etf['name']} ({etf['industry']})")
        print(f"  规模: {etf['fund_size']}亿, 成交额: {etf['avg_volume']}亿, 跟踪误差: {etf['tracking_error']*100}%")
        print(f"  选择理由: {etf['selection_reason']}\n")
    
    print("\n=== 激进仓ETF（5只） ===")
    for etf in stock_pool['aggressive'][:5]:  # 确保只打印5只
        print(f"{etf['etf_code']} {etf['name']} ({etf['industry']})")
        print(f"  规模: {etf['fund_size']}亿, 成交额: {etf['avg_volume']}亿, 跟踪误差: {etf['tracking_error']*100}%")
        print(f"  选择理由: {etf['selection_reason']}\n")
    
    # 保存测试结果
    test_data_dir = 'test_data'
    os.makedirs(test_data_dir, exist_ok=True)
    with open(os.path.join(test_data_dir, 'test_stock_pool.json'), 'w') as f:
        json.dump(stock_pool, f, ensure_ascii=False, indent=2)
    print(f"股票池测试结果已保存至{test_data_dir}/test_stock_pool.json")
