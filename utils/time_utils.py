from datetime import datetime
import pytz

def get_beijing_time():
    """获取当前北京时间（带时区信息）"""
    beijing_tz = pytz.timezone('Asia/Shanghai')
    return datetime.now(beijing_tz)

def get_beijing_time_str(format='%Y-%m-%d %H:%M:%S'):
    """获取当前北京时间字符串"""
    return get_beijing_time().strftime(format)

def is_trading_day():
    """判断当前是否为交易日（北京时间）"""
    today = get_beijing_time()
    weekday = today.weekday()  # 0=周一, 4=周五, 5=周六, 6=周日
    
    # 判断是否为周末
    if weekday >= 5:
        return False
    
    # 节假日列表（格式：(月, 日)）
    holidays = [
        (1, 1), (1, 2), (1, 3),  # 元旦
        (4, 4), (4, 5),          # 清明
        (5, 1), (5, 2), (5, 3),  # 劳动节
        (10, 1), (10, 2), (10, 3), (10, 4), (10, 5)  # 国庆
    ]
    
    # 检查是否为节假日（不考虑年份，适用于每年重复的节假日）
    today_date = (today.month, today.day)
    if today_date in holidays:
        return False
        
    return True
