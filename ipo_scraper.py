# -*- coding: utf-8 -*-
"""
新股申购信息爬取模块：获取沪市主板、深市主板、创业板、可转债和港股的申购信息
数据来源：东方财富网（A股/可转债）、阿斯达克财经网（港股）
推送时间：每个交易日早上10点（北京时间）
"""
import os
import time
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('logs/ipo_scraper.log'), logging.StreamHandler()]
)
logger = logging.getLogger('ipo_scraper')

class IPOInfoScraper:
    def __init__(self):
        """初始化新股申购信息爬取器"""
        self.ua = UserAgent()
        self.headers = {'User-Agent': self.ua.random}
        self.data_dir = 'data/ipo'
        os.makedirs(self.data_dir, exist_ok=True)
        
        # 数据源配置
        self.data_sources = {
            'a_stock': 'https://data.eastmoney.com/xg/xg/default.html',  # A股新股
            'convertible_bond': 'https://data.eastmoney.com/xg/kzz/default.html',  # 可转债
            'hk_stock': 'http://www.aastocks.com/sc/ipo/upcomingipo/ipo-schedule.aspx'  # 港股IPO
        }

    def _get_trading_dates(self):
        """获取最近的交易日（用于判断是否需要推送）"""
        today = datetime.now()
        # 判断今天是否为交易日（周一至周五，排除节假日）
        if today.weekday() >= 5:  # 周六或周日
            return False
        
        # 简单节假日判断（可扩展为更复杂的节假日表）
        holidays = [
            (1, 1), (5, 1), (10, 1),  # 法定节假日
            (2025, 1, 2), (2025, 1, 3), (2025, 1, 4)  # 2025年元旦假期
        ]
        today_date = (today.year, today.month, today.day)
        if today_date in [(y, m, d) for y, m, d in holidays]:
            return False
            
        return True

    def scrape_a_stock_ipo(self):
        """爬取A股新股申购信息（沪市主板、深市主板、创业板）"""
        try:
            url = self.data_sources['a_stock']
            response = requests.get(url, headers=self.headers)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 查找表格数据
            table = soup.find('table', {'id': 'dtTable'})
            if not table:
                logger.error("未找到A股新股表格")
                return pd.DataFrame()
                
            # 解析表头
            headers = [th.text.strip() for th in table.find_all('th')]
            
            # 解析行数据
            rows = []
            for tr in table.find_all('tr')[1:]:  # 跳过表头
                tds = tr.find_all('td')
                if len(tds) >= len(headers):
                    row = [td.text.strip() for td in tds]
                    rows.append(row)
            
            # 创建DataFrame
            df = pd.DataFrame(rows, columns=headers)
            
            # 筛选需要的字段
            if not df.empty:
                # 重命名列
                df.rename(columns={
                    '股票代码': 'code',
                    '股票名称': 'name',
                    '申购代码': 'apply_code',
                    '发行价格': 'price',
                    '申购上限': 'apply_limit',
                    '发行市盈率': 'pe',
                    '行业市盈率': 'industry_pe',
                    '申购日期': 'apply_date',
                    '中签号公布日': 'lottery_date',
                    '中签缴款日': 'payment_date',
                    '上市日期': 'listing_date'
                }, inplace=True)
                
                # 筛选今天及未来的申购信息
                today = datetime.now().strftime('%Y-%m-%d')
                df = df[df['apply_date'] >= today]
                
                # 标记市场类型
                df['market'] = df['code'].apply(lambda x: 
                    '沪市主板' if x.startswith(('600', '601', '603')) else 
                    '深市主板' if x.startswith('000') else 
                    '创业板' if x.startswith(('300', '301')) else 
                    '科创板' if x.startswith('688') else '未知'
                )
                
                # 只保留需要的列
                df = df[['market', 'code', 'name', 'apply_code', 'price', 'apply_limit', 
                        'pe', 'industry_pe', 'apply_date', 'payment_date']]
                
                logger.info(f"成功爬取A股新股信息，共{len(df)}条")
                return df
                
        except Exception as e:
            logger.error(f"爬取A股新股信息失败: {str(e)}")
            return pd.DataFrame()

    def scrape_convertible_bond(self):
        """爬取可转债申购信息"""
        try:
            url = self.data_sources['convertible_bond']
            response = requests.get(url, headers=self.headers)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 查找表格数据
            table = soup.find('table', {'id': 'dtTable'})
            if not table:
                logger.error("未找到可转债表格")
                return pd.DataFrame()
                
            # 解析表头
            headers = [th.text.strip() for th in table.find_all('th')]
            
            # 解析行数据
            rows = []
            for tr in table.find_all('tr')[1:]:  # 跳过表头
                tds = tr.find_all('td')
                if len(tds) >= len(headers):
                    row = [td.text.strip() for td in tds]
                    rows.append(row)
            
            # 创建DataFrame
            df = pd.DataFrame(rows, columns=headers)
            
            if not df.empty:
                # 重命名列
                df.rename(columns={
                    '债券代码': 'code',
                    '债券名称': 'name',
                    '申购代码': 'apply_code',
                    '正股代码': 'stock_code',
                    '正股名称': 'stock_name',
                    '发行规模(亿元)': 'issue_size',
                    '申购日期': 'apply_date',
                    '中签号公布日': 'lottery_date',
                    '上市日期': 'listing_date'
                }, inplace=True)
                
                # 筛选今天及未来的申购信息
                today = datetime.now().strftime('%Y-%m-%d')
                df = df[df['apply_date'] >= today]
                
                # 标记市场类型
                df['market'] = '可转债'
                
                # 只保留需要的列
                df = df[['market', 'code', 'name', 'apply_code', 'stock_code', 
                        'stock_name', 'issue_size', 'apply_date']]
                
                logger.info(f"成功爬取可转债信息，共{len(df)}条")
                return df
                
        except Exception as e:
            logger.error(f"爬取可转债信息失败: {str(e)}")
            return pd.DataFrame()

    def scrape_hk_stock_ipo(self):
        """爬取港股IPO申购信息"""
        try:
            url = self.data_sources['hk_stock']
            response = requests.get(url, headers=self.headers)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 查找表格数据
            table = soup.find('table', {'class': 'ipoTable'})
            if not table:
                logger.error("未找到港股IPO表格")
                return pd.DataFrame()
                
            # 解析表头
            headers = [th.text.strip() for th in table.find_all('th')]
            
            # 解析行数据
            rows = []
            for tr in table.find_all('tr')[1:]:  # 跳过表头
                tds = tr.find_all('td')
                if len(tds) >= len(headers):
                    row = [td.text.strip() for td in tds]
                    rows.append(row)
            
            # 创建DataFrame
            df = pd.DataFrame(rows, columns=headers)
            
            if not df.empty:
                # 重命名列（根据实际网页调整）
                df.rename(columns={
                    '证券代号': 'code',
                    '证券名称': 'name',
                    '招股日期': 'offer_period',
                    '定价日期': 'pricing_date',
                    '上市日期': 'listing_date',
                    '入场费(港元)': 'entry_fee',
                    '发行价(港元)': 'price_range',
                    '每手股数': 'lots',
                    '保荐人': 'sponsor',
                    '状态': 'status'
                }, inplace=True)
                
                # 处理日期格式
                df['market'] = '港股'
                
                # 只保留需要的列
                df = df[['market', 'code', 'name', 'offer_period', 'listing_date', 
                        'price_range', 'entry_fee', 'lots', 'status']]
                
                logger.info(f"成功爬取港股IPO信息，共{len(df)}条")
                return df
                
        except Exception as e:
            logger.error(f"爬取港股IPO信息失败: {str(e)}")
            return pd.DataFrame()

    def get_ipo_info(self):
        """获取所有市场的新股申购信息"""
        # 判断是否为交易日
        if not self._get_trading_dates():
            logger.info("今天不是交易日，不推送新股信息")
            return None
            
        # 爬取各市场数据
        a_stock_df = self.scrape_a_stock_ipo()
        cb_df = self.scrape_convertible_bond()
        hk_df = self.scrape_hk_stock_ipo()
        
        # 合并数据
        all_ipo_df = pd.concat([a_stock_df, cb_df, hk_df], ignore_index=True)
        
        # 保存数据
        if not all_ipo_df.empty:
            date_str = datetime.now().strftime('%Y%m%d')
            save_path = os.path.join(self.data_dir, f"ipo_info_{date_str}.csv")
            all_ipo_df.to_csv(save_path, index=False, encoding='utf-8-sig')
            logger.info(f"新股申购信息已保存至{save_path}")
            
            # 按市场分类
            market_groups = all_ipo_df.groupby('market')
            return {market: group.to_dict('records') for market, group in market_groups}
        else:
            logger.info("未获取到新股申购信息")
            return None

    def format_ipo_message(self, ipo_info):
        """格式化新股申购信息为推送消息"""
        if not ipo_info:
            return []
            
        messages = []
        system_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for market, ipo_list in ipo_info.items():
            for ipo in ipo_list:
                # 构建消息内容
                msg_lines = [f"CF系统时间：{system_time}"]
                msg_lines.append(f"【{market}新股申购】")
                msg_lines.append(f"名称：{ipo.get('name', '未知')}")
                msg_lines.append(f"代码：{ipo.get('code', '未知')}")
                
                # 根据市场类型添加不同字段
                if market in ['沪市主板', '深市主板', '创业板', '科创板']:
                    msg_lines.append(f"申购代码：{ipo.get('apply_code', '未知')}")
                    msg_lines.append(f"发行价格：{ipo.get('price', '未知')}元")
                    msg_lines.append(f"申购上限：{ipo.get('apply_limit', '未知')}股")
                    msg_lines.append(f"申购日期：{ipo.get('apply_date', '未知')}")
                    msg_lines.append(f"缴款日期：{ipo.get('payment_date', '未知')}")
                elif market == '可转债':
                    msg_lines.append(f"申购代码：{ipo.get('apply_code', '未知')}")
                    msg_lines.append(f"正股名称：{ipo.get('stock_name', '未知')}({ipo.get('stock_code', '未知')})")
                    msg_lines.append(f"发行规模：{ipo.get('issue_size', '未知')}亿元")
                    msg_lines.append(f"申购日期：{ipo.get('apply_date', '未知')}")
                elif market == '港股':
                    msg_lines.append(f"招股日期：{ipo.get('offer_period', '未知')}")
                    msg_lines.append(f"上市日期：{ipo.get('listing_date', '未知')}")
                    msg_lines.append(f"发行价：{ipo.get('price_range', '未知')}港元")
                    msg_lines.append(f"入场费：{ipo.get('entry_fee', '未知')}")
                    msg_lines.append(f"每手股数：{ipo.get('lots', '未知')}股")
                
                # 添加风险提示
                msg_lines.append("\n风险提示：以上信息仅供参考，投资需谨慎")
                
                messages.append('\n'.join(msg_lines))
                
        return messages

    def run(self):
        """执行爬取并返回格式化消息"""
        try:
            ipo_info = self.get_ipo_info()
            if not ipo_info:
                return []
                
            return self.format_ipo_message(ipo_info)
        except Exception as e:
            logger.error(f"新股信息爬取主流程失败: {str(e)}")
            return []

# 测试代码
if __name__ == "__main__":
    scraper = IPOInfoScraper()
    messages = scraper.run()
    
    if messages:
        logger.info("今日新股申购信息：")
        for i, msg in enumerate(messages):
            logger.info(f"消息{i+1}:\n{msg}\n")
        # 模拟推送
        logger.info(f"准备推送{len(messages)}条新股申购信息")
    else:
        logger.info("无新股申购信息需要推送")