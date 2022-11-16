import tushare as ts
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
import time
import logging


def getLogging():
    # 创建logger对象
    logger = logging.getLogger('daily_logger')
    # 设置日志等级
    logger.setLevel(logging.DEBUG)
    # 追加写入文件a ，设置utf-8编码防止中文写入乱码
    test_log = logging.FileHandler('../logs/get_daily.log', 'a', encoding='utf-8')
    # 向文件输出的日志级别
    test_log.setLevel(logging.DEBUG)
    # 向文件输出的日志信息格式
    formatter = logging.Formatter('%(asctime)s - %(filename)s - line:%(lineno)d - %(levelname)s - %(message)s -%(process)s')
    test_log.setFormatter(formatter)
    # 加载文件到logger对象中
    logger.addHandler(test_log)
    return logger

logger = getLogging()

def get_pro_api():
    mytoken = "f254ac9dc6e1df931994440e111c052577610c94d99ea957d13f9055"
    ts.set_token(mytoken)
    pro = ts.pro_api()
    return pro
pro = get_pro_api()

def get_daily(ts_code,start_date=date.today().strftime("%Y%m%d"),end_date=date.today().strftime("%Y%m%d")):
    # A股日线行情
    # 数据说明：交易日每天15点～16点之间入库。本接口是未复权行情，停牌期间不提供数据
    # 描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据
    data = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    return data

def updateData(ts_code,start_date=date.today().strftime("%Y%m%d"),end_date=date.today().strftime("%Y%m%d"),mode='s'):
    # model = s 增量更新
    #         f 全量更新
    try:
        if mode == 'f':a = int('a')
        df_hist = pd.read_csv('../Data/Dynamic/daily/' + ts_code + '.csv')
        data = get_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        data['trade_date'] = pd.to_numeric(data['trade_date'])
        data = data.append(df_hist)
        data = data.drop_duplicates(['trade_date'])
        data.set_index('trade_date', inplace=True)
        data.to_csv('../Data/Dynamic/daily/' + ts_code+'.csv')
    except:
        logger.error('{0} 股票不存在,重新拉取全量数据!'.format(ts_code))
        # print('{0} 股票不存在,重新拉取全量数据!'.format(ts_code))
        data = get_daily(ts_code=ts_code,start_date=(date.today() - timedelta(365*30)).strftime("%Y%m%d"))
        data.set_index('trade_date', inplace=True)
        data.to_csv('../Data/Dynamic/daily/' + ts_code + '.csv')


if __name__ == '__main__':
    logger.info('读取股票基本数据')
    df_basic = pd.read_csv('../Data/Static/basic.csv')
    ts_codes = np.array(df_basic['ts_code'])
    start_date = (date.today() - timedelta(365*30)).strftime("%Y%m%d")
    yesterday = (date.today() - timedelta(2)).strftime("%Y%m%d")
    end_date = date.today().strftime("%Y%m%d")

    ##### 每日更新数据 #####
    logger.info('开始更新daily {0} 数据'.format(date.today().strftime("%Y%m%d")))
    count = 0
    start_time = time.time()
    for ts_code in ts_codes:
        updateData(ts_code,yesterday,end_date,mode='s')
        count += 1
        if count % 100 == 0:
            logger.info('数据更新进度{0}/{1}，耗时:{2:.2f}s'.format(count,len(ts_codes), time.time() - start_time))
    end_time = time.time()
    logger.info('{0}数据更新完毕,更新{1}只股票数据,共耗时:{2:.2f}s'.format(date.today().strftime("%Y%m%d"),count,end_time - start_time))
    ##### 每日更新数据 #####


