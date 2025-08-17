# -*- coding: utf-8 -*-
"""
鱼盆模型ETF量化策略主程序
支持多种任务类型，包括股票池更新、策略执行、套利检查和测试功能
命令行参数说明：
--task: 指定任务类型，可选值：
    update_stock_pool - 更新股票池
    push_stock_pool - 推送股票池消息
    execute_strategy - 执行策略检查
    check_arbitrage - 检查套利机会
    test_push - 测试消息推送
    test_strategy - 测试策略执行（仅返回结果）
    print_transactions - 打印交易流水
    manual_push_pool - 手动推送股票池
    force_execute - 执行策略并推送结果
    reset_position - 重置持仓（测试用）
--date: 指定日期（用于打印交易流水），格式YYYY-MM-DD
--pool_type: 指定策略类型（stable/aggressive）
"""
import os
import sys
import argparse
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/main.log'), logging.StreamHandler()]
)
logger = logging.getLogger('main')

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入核心模块
from data_source_integration import DataSource
from etf_selector import ETFSelector
from fishbowl_strategy import FishBowlStrategy
from wechat_notifier import WechatNotifier

def main():
    """主程序入口"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='鱼盆模型ETF量化策略')
    parser.add_argument('--task', required=True, help='任务类型')
    parser.add_argument('--date', help='指定日期（YYYY-MM-DD）')
    parser.add_argument('--pool_type', default='stable', help='策略类型（stable/aggressive）')
    args = parser.parse_args()

    # 初始化核心组件
    ds = DataSource()
    selector = ETFSelector(ds)
    strategy = FishBowlStrategy(ds, selector)
    notifier = WechatNotifier()

    try:
        # 根据任务类型执行相应操作
        if args.task == 'update_stock_pool':
            # 更新股票池
            selector.select_stock_pool(force_refresh=True)
            logger.info("股票池更新任务完成")

        elif args.task == 'push_stock_pool':
            # 推送股票池消息
            notifier.start()
            stock_pool = selector.get_current_pool()
            if not stock_pool:
                logger.error("获取股票池失败，无法推送")
                return

            # 逐条推送稳健仓
            for etf in stock_pool['stable'][:5]:  # 最多5只
                message = {
                    'content': f"CF系统时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                              f"【本周稳健仓ETF】\n"
                              f"代码：{etf['etf_code']}\n"
                              f"名称：{etf['name']}\n"
                              f"规模：{etf['fund_size']}亿\n"
                              f"成交额：{etf['avg_volume']}亿\n"
                              f"跟踪误差：{etf['tracking_error']*100}%\n"
                              f"选择理由：{etf['selection_reason']}"
                }
                notifier.add_message(message)

            # 逐条推送激进仓
            for etf in stock_pool['aggressive'][:5]:  # 最多5只
                message = {
                    'content': f"CF系统时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                              f"【本周激进仓ETF】\n"
                              f"代码：{etf['etf_code']}\n"
                              f"名称：{etf['name']}\n"
                              f"规模：{etf['fund_size']}亿\n"
                              f"成交额：{etf['avg_volume']}亿\n"
                              f"跟踪误差：{etf['tracking_error']*100}%\n"
                              f"选择理由：{etf['selection_reason']}"
                }
                notifier.add_message(message)

            # 等待消息发送完成
            while not (notifier.message_queue.empty() and notifier.retry_queue.empty()):
                time.sleep(10)
            notifier.终止()
            logger.info("股票池推送任务完成")

        elif args.task == 'execute_strategy':
            # 执行策略检查并推送
            notifier.start()
            
            # 执行稳健仓策略
            stable_result = strategy.execute_strategy(pool_type='stable')
            if stable_result['status'] == 'success':
                notifier.send_strategy_result(stable_result, 'stable')
            
            # 执行激进仓策略
            aggressive_result = strategy.execute_strategy(pool_type='aggressive')
            if aggressive_result['status'] == 'success':
                notifier.send_strategy_result(aggressive_result, 'aggressive')

            # 等待消息发送完成
            while not (notifier.message_queue.empty() and notifier.retry_queue.empty()):
                time.sleep(10)
            notifier.终止()
            logger.info("策略执行任务完成")

        elif args.task == 'check_arbitrage':
            # 检查套利机会（简化实现）
            notifier.start()
            
            # 模拟套利检查结果
            arbitrage_opportunities = [
                {
                    'etf_code': '510300',
                    'reason': '成分股重大利好',
                    'premium': '3.2%',
                    'target_price': '4.35元'
                }
            ]
            
            # 推送套利机会
            for opp in arbitrage_opportunities:
                message = {
                    'content': f"CF系统时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                              f"【套利机会提示】\n"
                              f"ETF代码：{opp['etf_code']}\n"
                              f"套利原因：{opp['reason']}\n"
                              f"溢价率：{opp['premium']}\n"
                              f"目标价：{opp['target_price']}"
                }
                notifier.add_message(message)

            # 等待消息发送完成
            while not (notifier.message_queue.empty() and notifier.retry_queue.empty()):
                time.sleep(10)
            notifier.终止()
            logger.info("套利检查任务完成")

        # 测试功能
        elif args.task == 'test_push':
            # 测试消息推送
            notifier.start()
            test_message = {
                'content': f"CF系统时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                          f"【测试消息】\n"
                          f"这是一条测试消息，用于验证推送功能是否正常。"
            }
            notifier.add_message(test_message)
            
            # 等待消息发送
            time.sleep(60)
            notifier.终止()
            logger.info("测试消息推送完成")

        elif args.task == 'test_strategy':
            # 测试策略执行（仅返回结果，不推送）
            result = strategy.execute_strategy(pool_type=args.pool_type)
            print("策略测试结果：")
            print(result)
            logger.info("策略测试完成")

        elif args.task == 'print_transactions':
            # 打印交易流水
            start_date = args.date if args.date else (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            end_date = datetime.now().strftime('%Y-%m-%d')
            print(f"交易流水（{start_date}至{end_date}）：")
            strategy.print_transaction_history(start_date, end_date)
            logger.info("交易流水打印完成")

        elif args.task == 'manual_push_pool':
            # 手动推送股票池
            args.task = 'push_stock_pool'
            main()  # 递归调用

        elif args.task == 'force_execute':
            # 执行策略并推送
            args.task = 'execute_strategy'
            main()  # 递归调用

        elif args.task == 'reset_position':
            # 重置持仓（测试用）
            strategy.current_positions = {
                'stable': {'etf_code': '', 'position': 0, 'avg_price': 0, '终止_loss': 0},
                'aggressive': {'etf_code': '', 'position': 0, 'avg_price': 0, '终止_loss': 0}
            }
            logger.info("持仓已重置")

        else:
            logger.error(f"未知任务类型：{args.task}")

    except Exception as e:
        logger.error(f"任务执行失败: {str(e)}", exc_info=True)
    finally:
        # 确保通知器停止
        if 'notifier' in locals() and notifier.running:
            notifier.终止()

if __name__ == "__main__":
    import time
    main()
