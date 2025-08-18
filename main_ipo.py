# -*- coding: utf-8 -*-
"""
主程序：整合新股申购信息爬取与推送功能
每日早上10点执行，推送当日新股申购信息
"""
import time
import logging
from ipo_scraper import IPOInfoScraper
from wechat_notifier import WechatNotifier

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/main_ipo.log'), logging.StreamHandler()]
)
logger = logging.getLogger('main_ipo')

def main():
    """主函数：爬取并推送新股申购信息"""
    # 初始化组件
    scraper = IPOInfoScraper()
    notifier = WechatNotifier()
    
    try:
        # 启动通知器
        notifier.start()
        
        # 获取新股信息
        logger.info("开始爬取新股申购信息")
        messages = scraper.run()
        
        if messages:
            # 逐条推送消息（每条间隔1分钟）
            for msg in messages:
                notifier.add_message({'content': msg})
                logger.info(f"添加新股申购消息到队列，内容预览：{msg[:50]}...")
                # 不需要额外sleep，通知器内部已处理间隔
                
            # 等待所有消息发送完成
            while not (notifier.message_queue.empty() and notifier.retry_queue.empty()):
                time.sleep(10)
                
            logger.info(f"成功推送{len(messages)}条新股申购信息")
        else:
            logger.info("没有需要推送的新股申购信息")
            
    except Exception as e:
        logger.error(f"新股申购信息推送失败: {str(e)}", exc_info=True)
    finally:
        # 停止通知器
        notifier.stop()

if __name__ == "__main__":
    main()