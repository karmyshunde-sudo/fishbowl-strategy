# -*- coding: utf-8 -*-
"""
鱼盆模型策略核心算法模块：实现趋势跟踪、信号生成和仓位管理
基于20日均线的突破/跌破规则，区分稳健仓和激进仓策略参数
"""
import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/strategy.log'), logging.StreamHandler()]
)
logger = logging.getLogger('fishbowl_strategy')

# 策略参数配置
STRATEGY_PARAMS = {
    'stable': {  # 稳健仓策略参数
        'ma_period': 20,          # 均线周期
        'confirm_days': 3,        # 信号确认天数
        'initial_position': 0.3,  # 初始仓位比例(30%)
        'add_position': [0.2, 0.1],# 加仓比例(20%, 10%)
        'stop_loss_ratio': 0.15,   # 固定止损比例(15%)
        'tracking_stop_ratio': 0.05,# 跟踪止损步长(5%)
        'max_position': 0.7       # 最大仓位比例(70%)
    },
    'aggressive': {  # 激进仓策略参数
        'ma_period': 20,          # 均线周期
        'confirm_days': 2,        # 信号确认天数(更短)
        'initial_position': 0.2,  # 初始仓位比例(20%)
        'add_position': [0.15],   # 加仓比例(15%)
        'stop_loss_ratio': 0.15,   # 固定止损比例(15%)
        'tracking_stop_ratio': 0.03,# 跟踪止损步长(3%)
        'max_position': 0.5       # 最大仓位比例(50%)
    }
}

# 交易记录存储路径
TRANSACTION_LOG_PATH = 'data/transactions'
os.makedirs(TRANSACTION_LOG_PATH, exist_ok=True)

class FishBowlStrategy:
    def __init__(self, data_source, selector):
        """
        初始化鱼盆模型策略
        :param data_source: DataSource实例，用于获取行情数据
        :param selector: ETFSelector实例，用于获取股票池
        """
        self.data_source = data_source
        self.selector = selector
        self.current_positions = {  # 当前持仓
            'stable': {'etf_code': '', 'position': 0, 'avg_price': 0, 'stop_loss': 0},
            'aggressive': {'etf_code': '', 'position': 0, 'avg_price': 0, 'stop_loss': 0}
        }
        self.load_transaction_history()  # 加载历史交易记录

    def load_transaction_history(self):
        """加载历史交易记录"""
        try:
            latest_log = self._get_latest_transaction_log()
            if latest_log:
                with open(latest_log, 'r') as f:
                    self.transaction_history = json.load(f)
                logger.info(f"加载历史交易记录，共{len(self.transaction_history)}条记录")
            else:
                self.transaction_history = []
                logger.info("无历史交易记录，初始化空记录")
        except Exception as e:
            logger.error(f"加载交易记录失败: {str(e)}")
            self.transaction_history = []

    def _get_latest_transaction_log(self):
        """获取最新的交易记录文件"""
        log_files = [f for f in os.listdir(TRANSACTION_LOG_PATH) if f.startswith('transactions_')]
        if not log_files:
            return None
        log_files.sort(reverse=True)
        return os.path.join(TRANSACTION_LOG_PATH, log_files[0])

    def _save_transaction(self, transaction):
        """保存交易记录"""
        transaction['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.transaction_history.append(transaction)
        
        # 按日期分文件保存
        date_str = datetime.now().strftime('%Y%m%d')
        log_file = os.path.join(TRANSACTION_LOG_PATH, f"transactions_{date_str}.json")
        
        with open(log_file, 'w') as f:
            json.dump(self.transaction_history, f, ensure_ascii=False, indent=2)
        logger.info(f"交易记录已保存至{log_file}，类型: {transaction['action']}")

    def generate_signals(self, pool_type='stable'):
        """
        生成交易信号
        :param pool_type: str, 策略类型('stable'或'aggressive')
        :return: dict, 包含交易信号和建议
        """
        # 获取当前股票池
        stock_pool = self.selector.get_current_pool()
        if not stock_pool or len(stock_pool[pool_type]) == 0:
            logger.error(f"{pool_type}股票池为空，无法生成信号")
            return {'action': 'HOLD', 'reason': '股票池为空', 'etf_code': '', 'position': 0}

        # 获取参数配置
        params = STRATEGY_PARAMS[pool_type]
        
        # 获取当前持仓
        current_pos = self.current_positions[pool_type]
        
        # 如果当前有持仓，先检查是否需要卖出
        if current_pos['position'] > 0:
            signal = self._check_sell_signal(current_pos['etf_code'], params)
            if signal['action'] == 'SELL':
                return signal
        
        # 如果无持仓或需要换仓，从股票池选择最优ETF
        best_etf = self._select_best_etf(stock_pool[pool_type], params)
        if not best_etf:
            return {'action': 'HOLD', 'reason': '无符合条件的ETF', 'etf_code': '', 'position': 0}
        
        # 检查买入信号
        buy_signal = self._check_buy_signal(best_etf['etf_code'], params)
        if buy_signal['action'] == 'BUY':
            return buy_signal
        
        return {'action': 'HOLD', 'reason': '无交易信号', 'etf_code': '', 'position': 0}

    def _select_best_etf(self, etf_pool, params):
        """从股票池选择最优ETF"""
        candidates = []
        
        for etf in etf_pool:
            # 获取行情数据
            quote_df = self.data_source.get_etf_quote(etf['etf_code'])
            if quote_df.empty:
                logger.warning(f"{etf['etf_code']}无行情数据，跳过")
                continue
            
            # 计算趋势强度指标
            ma = quote_df['ma20'].iloc[-1]
            price = quote_df['close'].iloc[-1]
            trend_strength = (price - ma) / ma  # 价格偏离均线比例
            
            candidates.append({
                'etf_info': etf,
                'trend_strength': trend_strength,
                'quote_df': quote_df
            })
        
        if not candidates:
            return None
            
        # 按趋势强度排序，选择最强趋势
        candidates.sort(key=lambda x: x['trend_strength'], reverse=True)
        return candidates[0]['etf_info']

    def _check_buy_signal(self, etf_code, params):
        """检查买入信号"""
        # 获取行情数据
        quote_df = self.data_source.get_etf_quote(
            etf_code, 
            start_date=(datetime.now() - timedelta(days=params['ma_period'] * 2)).strftime('%Y-%m-%d')
        )
        
        if len(quote_df) < params['ma_period']:
            logger.warning(f"{etf_code}数据不足{params['ma_period']}天，无法判断趋势")
            return {'action': 'HOLD', 'reason': '数据不足', 'etf_code': etf_code, 'position': 0}
        
        # 计算均线
        ma = quote_df['close'].rolling(window=params['ma_period']).mean()
        latest_ma = ma.iloc[-1]
        prev_ma = ma.iloc[-2]
        
        # 价格突破均线
        price_break = (quote_df['close'].iloc[-params['confirm_days']:] > ma.iloc[-params['confirm_days']:]).all()
        # 均线呈上升趋势
        ma_trend = (latest_ma > prev_ma)
        
        if price_break and ma_trend:
            # 计算建议仓位
            position = params['initial_position']
            etf_info = next((e for e in self.selector.get_current_pool()['stable'] + self.selector.get_current_pool()['aggressive'] 
                            if e['etf_code'] == etf_code), None)
            
            return {
                'action': 'BUY',
                'reason': f"{params['confirm_days']}天站稳{params['ma_period']}日均线",
                'etf_code': etf_code,
                'etf_name': etf_info['name'] if etf_info else '',
                'position': position,
                'price': quote_df['close'].iloc[-1],
                'stop_loss': quote_df['close'].iloc[-1] * (1 - params['stop_loss_ratio'])
            }
        
        return {'action': 'HOLD', 'reason': '未突破均线或趋势未确认', 'etf_code': etf_code, 'position': 0}

    def _check_sell_signal(self, etf_code, params):
        """检查卖出信号"""
        # 获取行情数据
        quote_df = self.data_source.get_etf_quote(etf_code)
        if quote_df.empty:
            logger.warning(f"{etf_code}无行情数据，无法判断卖出信号")
            return {'action': 'HOLD', 'reason': '数据不足', 'etf_code': etf_code, 'position': 0}
        
        current_price = quote_df['close'].iloc[-1]
        current_pos = self.current_positions[pool_type]
        
        # 检查固定止损
        if current_price <= current_pos['stop_loss']:
            return {
                'action': 'SELL',
                'reason': f"触发固定止损({params['stop_loss_ratio']*100}%)",
                'etf_code': etf_code,
                'position': 0,
                'price': current_price,
                'loss_ratio': (current_price - current_pos['avg_price']) / current_pos['avg_price']
            }
        
        # 检查跌破均线
        ma = quote_df['close'].rolling(window=params['ma_period']).mean().iloc[-1]
        if current_price < ma and quote_df['close'].iloc[-params['confirm_days']:].min() < ma:
            return {
                'action': 'SELL',
                'reason': f"{params['confirm_days']}天跌破{params['ma_period']}日均线",
                'etf_code': etf_code,
                'position': 0,
                'price': current_price,
                'profit_ratio': (current_price - current_pos['avg_price']) / current_pos['avg_price']
            }
        
        # 检查跟踪止损
        max_price = max([t['price'] for t in self.transaction_history 
                        if t['etf_code'] == etf_code and t['action'] == 'BUY'])
        if max_price * (1 - params['tracking_stop_ratio']) > current_price:
            return {
                'action': 'SELL',
                'reason': f"触发跟踪止损({params['tracking_stop_ratio']*100}%)",
                'etf_code': etf_code,
                'position': 0,
                'price': current_price,
                'profit_ratio': (current_price - current_pos['avg_price']) / current_pos['avg_price']
            }
        
        return {'action': 'HOLD', 'reason': '未触发卖出条件', 'etf_code': etf_code, 'position': current_pos['position']}

    def execute_strategy(self, pool_type='stable'):
        """
        执行策略
        :param pool_type: str, 策略类型('stable'或'aggressive')
        :return: dict, 执行结果
        """
        signal = self.generate_signals(pool_type)
        
        if signal['action'] == 'BUY':
            return self._execute_buy(signal, pool_type)
        elif signal['action'] == 'SELL':
            return self._execute_sell(signal, pool_type)
        else:
            return {
                'status': 'success',
                'action': 'HOLD',
                'message': signal['reason'],
                'etf_code': signal['etf_code'],
                'current_position': self.current_positions[pool_type]['position']
            }

    def _execute_buy(self, signal, pool_type):
        """执行买入操作"""
        current_pos = self.current_positions[pool_type]
        
        # 如果当前有持仓且不是目标ETF，先卖出
        if current_pos['position'] > 0 and current_pos['etf_code'] != signal['etf_code']:
            sell_result = self._execute_sell({
                'action': 'SELL',
                'etf_code': current_pos['etf_code'],
                'position': 0,
                'reason': '换仓操作'
            }, pool_type)
            if sell_result['status'] != 'success':
                return sell_result
        
        # 执行买入
        self.current_positions[pool_type] = {
            'etf_code': signal['etf_code'],
            'position': signal['position'],
            'avg_price': signal['price'],
            'stop_loss': signal['stop_loss']
        }
        
        # 记录交易
        self._save_transaction({
            'action': 'BUY',
            'etf_code': signal['etf_code'],
            'etf_name': signal['etf_name'],
            'price': signal['price'],
            'position': signal['position'],
            'reason': signal['reason'],
            'strategy_type': pool_type
        })
        
        return {
            'status': 'success',
            'action': 'BUY',
            'message': f"买入{signal['etf_code']}，仓位{signal['position']*100}%",
            'etf_code': signal['etf_code'],
            'etf_name': signal['etf_name'],
            'price': signal['price'],
            'position': signal['position']
        }

    def _execute_sell(self, signal, pool_type):
        """执行卖出操作"""
        current_pos = self.current_positions[pool_type]
        
        if current_pos['etf_code'] != signal['etf_code']:
            logger.error(f"持仓不匹配，当前持仓: {current_pos['etf_code']}，信号: {signal['etf_code']}")
            return {'status': 'error', 'action': 'SELL', 'message': '持仓不匹配', 'etf_code': signal['etf_code']}
        
        # 记录交易
        profit_ratio = signal.get('profit_ratio', 0)
        self._save_transaction({
            'action': 'SELL',
            'etf_code': signal['etf_code'],
            'price': signal['price'],
            'position': current_pos['position'],
            'reason': signal['reason'],
            'profit_ratio': profit_ratio,
            'strategy_type': pool_type
        })
        
        # 清空持仓
        self.current_positions[pool_type] = {
            'etf_code': '',
            'position': 0,
            'avg_price': 0,
            'stop_loss': 0
        }
        
        return {
            'status': 'success',
            'action': 'SELL',
            'message': f"卖出{signal['etf_code']}，{signal['reason']}",
            'etf_code': signal['etf_code'],
            'price': signal['price'],
            'profit_ratio': profit_ratio
        }

    def print_transaction_history(self, start_date=None, end_date=None):
        """
        打印交易流水
        :param start_date: str, 开始日期(YYYY-MM-DD)
        :param end_date: str, 结束日期(YYYY-MM-DD)
        :return: list, 过滤后的交易记录
        """
        filtered = self.transaction_history
        
        # 日期过滤
        if start_date:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            filtered = [t for t in filtered if datetime.strptime(t['timestamp'].split()[0], '%Y-%m-%d') >= start_dt]
        
        if end_date:
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            filtered = [t for t in filtered if datetime.strptime(t['timestamp'].split()[0], '%Y-%m-%d') <= end_dt]
        
        # 格式化输出
        print("\n=== 交易流水 ===")
        for t in filtered:
            profit = f"{t['profit_ratio']*100:.2f}%" if 'profit_ratio' in t else "N/A"
            print(f"{t['timestamp']} | {t['action']} | {t['etf_code']} {t.get('etf_name','')} | 价格: {t['price']} | 仓位: {t['position']*100:.0f}% | 收益: {profit} | 原因: {t['reason']}")
        
        return filtered

# 测试代码
if __name__ == "__main__":
    from data_source_integration import DataSource
    from etf_selector import ETFSelector
    
    # 创建数据源和选择器实例
    ds = DataSource()
    selector = ETFSelector(ds)
    
    # 确保股票池已生成
    selector.select_stock_pool(force_refresh=True)
    
    # 创建策略实例
    strategy = FishBowlStrategy(ds, selector)
    
    # 测试稳健仓策略
    print("\n=== 测试稳健仓策略 ===")
    stable_result = strategy.execute_strategy(pool_type='stable')
    print(stable_result)
    
    # 测试激进仓策略
    print("\n=== 测试激进仓策略 ===")
    aggressive_result = strategy.execute_strategy(pool_type='aggressive')
    print(aggressive_result)
    
    # 打印交易流水
    print("\n=== 测试交易流水打印 ===")
    strategy.print_transaction_history()
