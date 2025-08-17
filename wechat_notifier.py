# -*- coding: utf-8 -*-
"""
企业微信消息推送模块：实现策略结果的格式化、限流控制和失败重试
支持逐条消息推送，每条间隔1分钟，交易时段内失败自动重试
"""
import os
import time
import json
import logging
import requests
import queue
from datetime import datetime, timedelta
from threading import Thread

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/wechat_notify.log'), logging.StreamHandler()]
)
logger = logging.getLogger('wechat_notifier')

# 企业微信Webhook配置（从环境变量获取）
WECHAT_WEBHOOK = os.getenv('WECHAT_WEBHOOK', 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=2f594b04-eb03-42b6-ad41-ff148bd57183')

# 交易时段配置（北京时间）
TRADING_HOURS = {
    'morning_start': '09:30',
    'morning_end': '11:30',
    'afternoon_start': '13:00',
    'afternoon_end': '15:00'
}

class WechatNotifier:
    def __init__(self):
        """初始化企业微信消息推送器"""
        self.message_queue = queue.Queue()  # 消息队列
        self.sending_thread = None  # 发送线程
        self.running = False  # 运行状态标志
        self.last_send_time = 0  # 上次发送时间（时间戳）
        self.message_interval = 60  # 消息间隔（秒）- 1分钟
        self.retry_queue = queue.Queue()  # 重试队列
        self.retry_interval = 1800  # 重试间隔（秒）- 30分钟

    def start(self):
        """启动消息发送线程"""
        if not self.running:
            self.running = True
            self.sending_thread = Thread(target=self._process_queue, daemon=True)
            self.sending_thread.start()
            logger.info("微信消息推送器已启动")

    def stop(self):
        """停止消息发送线程"""
        self.running = False
        if self.sending_thread and self.sending_thread.is_alive():
            self.sending_thread.join()
        logger.info("微信消息推送器已停止")

    def add_message(self, message, is_retry=False):
        """
        添加消息到发送队列
        :param message: dict, 消息内容
        :param is_retry: bool, 是否为重试消息
        """
        if is_retry:
            self.retry_queue.put(message)
            logger.info(f"添加重试消息到队列，内容: {message['content'][:20]}...")
        else:
            self.message_queue.put(message)
            logger.info(f"添加消息到队列，内容: {message['content'][:20]}...")

    def _process_queue(self):
        """处理消息队列，发送消息并处理重试"""
        while self.running:
            # 先处理重试队列
            if not self.retry_queue.empty() and self._is_trading_time():
                message = self.retry_queue.get()
                self._send_message(message)
                self.retry_queue.task_done()
                time.sleep(1)  # 短暂延迟
                continue
            
            # 处理普通消息队列
            if not self.message_queue.empty():
                message = self.message_queue.get()
                
                # 检查发送间隔
                current_time = time.time()
                if current_time - self.last_send_time < self.message_interval:
                    sleep_time = self.message_interval - (current_time - self.last_send_time)
                    logger.info(f"消息发送间隔不足，等待{sleep_time:.1f}秒")
                    time.sleep(sleep_time)
                
                # 发送消息
                success = self._send_message(message)
                self.last_send_time = time.time()
                
                # 如果发送失败且在交易时间，添加到重试队列
                if not success and self._is_trading_time():
                    logger.warning("消息发送失败，添加到重试队列")
                    self.retry_queue.put(message)
                
                self.message_queue.task_done()
            else:
                # 队列为空时短暂休眠
                time.sleep(1)

    def _send_message(self, message):
        """
        发送消息到企业微信
        :param message: dict, 消息内容
        :return: bool, 发送是否成功
        """
        try:
            # 构造企业微信消息格式
            payload = {
                "msgtype": "text",
                "text": {
                    "content": message['content']
                }
            }
            
            headers = {'Content-Type': 'application/json'}
            response = requests.post(
                WECHAT_WEBHOOK,
                data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
                headers=headers,
                timeout=10
            )
            
            # 检查响应
            if response.status_code == 200:
                result = response.json()
                if result.get('errcode') == 0:
                    logger.info(f"消息发送成功，内容: {message['content'][:50]}...")
                    return True
                else:
                    logger.error(f"消息发送失败，错误码: {result.get('errcode')}, 错误信息: {result.get('errmsg')}")
            else:
                logger.error(f"消息发送失败，HTTP状态码: {response.status_code}")
                
            return False
            
        except Exception as e:
            logger.error(f"消息发送异常: {str(e)}")
            return False

    def _is_trading_time(self):
        """判断当前是否为交易时间"""
        now = datetime.now()
        # 仅工作日（周一至周五）
        if now.weekday() >= 5:
            return False
            
        current_time = now.strftime('%H:%M')
        
        # 上午交易时段
        morning_start = TRADING_HOURS['morning_start']
        morning_end = TRADING_HOURS['morning_end']
        # 下午交易时段
        afternoon_start = TRADING_HOURS['afternoon_start']
        afternoon_end = TRADING_HOURS['afternoon_end']
        
        # 检查是否在交易时段内
        in_morning = morning_start <= current_time <= morning_end
        in_afternoon = afternoon_start <= current_time <= afternoon_end
        
        return in_morning or in_afternoon

    def format_strategy_message(self, strategy_result, pool_type):
        """
        格式化策略结果为消息内容
        :param strategy_result: dict, 策略执行结果
        :param pool_type: str, 策略类型('stable'或'aggressive')
        :return: str, 格式化后的消息内容
        """
        # 系统时间前缀
        system_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        prefix = f"CF系统时间：{system_time}\n"
        
        # 策略类型名称
        pool_name = "稳健仓" if pool_type == 'stable' else "激进仓"
        
        if strategy_result['action'] == 'BUY':
            content = f"{prefix}【{pool_name}策略执行结果】\n" \
                      f"操作：买入\n" \
                      f"ETF代码：{strategy_result['etf_code']}\n" \
                      f"ETF名称：{strategy_result['etf_name']}\n" \
                      f"建议价格：{strategy_result['price']:.2f}元\n" \
                      f"建议仓位：{strategy_result['position']*100:.0f}%\n" \
                      f"止损价格：{strategy_result['stop_loss']:.2f}元\n" \
                      f"操作理由：{strategy_result['message']}"
                      
        elif strategy_result['action'] == 'SELL':
            profit_str = f"收益：{strategy_result['profit_ratio']*100:.2f}%" if 'profit_ratio' in strategy_result else "收益：N/A"
            content = f"{prefix}【{pool_name}策略执行结果】\n" \
                      f"操作：卖出\n" \
                      f"ETF代码：{strategy_result['etf_code']}\n" \
                      f"卖出价格：{strategy_result['price']:.2f}元\n" \
                      f"{profit_str}\n" \
                      f"操作理由：{strategy_result['message']}"
                      
        elif strategy_result['action'] == 'HOLD':
            content = f"{prefix}【{pool_name}策略执行结果】\n" \
                      f"操作：持有\n" \
                      f"当前仓位：{strategy_result['current_position']*100:.0f}%\n" \
                      f"理由：{strategy_result['message']}"
                      
        else:
            content = f"{prefix}【{pool_name}策略执行结果】\n" \
                      f"操作：{strategy_result['action']}\n" \
                      f"信息：{strategy_result['message']}"
                      
        return content

    def send_strategy_result(self, strategy_result, pool_type):
        """
        发送策略执行结果消息
        :param strategy_result: dict, 策略执行结果
        :param pool_type: str, 策略类型('stable'或'aggressive')
        """
        if strategy_result.get('status') != 'success':
            logger.warning(f"策略执行失败，不发送消息: {strategy_result.get('message')}")
            return
            
        # 格式化消息
        message_content = self.format_strategy_message(strategy_result, pool_type)
        
        # 添加到消息队列
        self.add_message({
            'content': message_content,
            'timestamp': time.time(),
            'pool_type': pool_type
        })

# 测试代码
if __name__ == "__main__":
    # 创建消息推送器实例
    notifier = WechatNotifier()
    
    # 启动推送器
    notifier.start()
    
    try:
        # 测试发送买入消息
        test_buy_result = {
            'status': 'success',
            'action': 'BUY',
            'etf_code': '510300',
            'etf_name': '沪深300ETF',
            'price': 4.25,
            'position': 0.3,
            'stop_loss': 3.61,
            'message': '3天站稳20日均线'
        }
        notifier.send_strategy_result(test_buy_result, 'stable')
        
        # 测试发送持有消息
        test_hold_result = {
            'status': 'success',
            'action': 'HOLD',
            'current_position': 0.3,
            'message': '未触发交易条件'
        }
        notifier.send_strategy_result(test_hold_result, 'aggressive')
        
        # 等待消息发送
        time.sleep(120)  # 等待2分钟
        
    finally:
        # 停止推送器
        notifier.stop()
