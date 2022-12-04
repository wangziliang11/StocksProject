# 整合股票行情数据
import tushare as ts
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
import time
import logging
import argparse
import threading
import os

def getLogging():
    # 创建logger对象
    logger = logging.getLogger('general_logger')
    # 设置日志等级
    logger.setLevel(logging.DEBUG)
    # 追加写入文件a ，设置utf-8编码防止中文写入乱码
    test_log = logging.FileHandler('../logs/mergeData.log', 'a', encoding='utf-8')
    # 向文件输出的日志级别
    test_log.setLevel(logging.DEBUG)
    # 向文件输出的日志信息格式
    formatter = logging.Formatter('%(asctime)s - %(filename)s - line:%(lineno)d - %(levelname)s - %(message)s -%(process)s')
    test_log.setFormatter(formatter)
    # 加载文件到logger对象中
    logger.addHandler(test_log)
    return logger

logger = getLogging()

def multi_thread(ts_codes,daily_path):
    global start_time
    global count
    global n
    for ts_code in ts_codes:
        try:
            daily_basic_path = daily_path + ts_code + '.csv'
            df_daily_basic = pd.read_csv(daily_basic_path)
            df_daily_basic.set_index('ts_code', inplace=True)
            df = df_basic.loc[df_basic['ts_code'].isin([ts_code])]
            df.set_index('ts_code', inplace=True)
            df = df_daily_basic.join(df, on='ts_code')
            df.to_csv('../data/MergeData/quotation/' + ts_code + '.csv')
            count += 1
            if count % 100 == 0:
                logger.info('行情数据合并进度{0}/{1}，耗时:{2:.2f}s'.format(count, n, time.time() - start_time))
        except:
            continue
    end_time = time.time()
    logger.info(
        '成功合并 {} 只股票行情数据,耗时:{:.2f}s'.format(count,end_time - start_time))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date",default=(date.today()-timedelta(365*30)).strftime("%Y%m%d"))
    parser.add_argument("--end-date",default=((date.today()).strftime("%Y%m%d")))
    parser.add_argument("--freq", default=('D'))
    parser.add_argument("--adj", default=('qfq'))
    parser.add_argument("--ma", default=([3,5,10,20,30,60,90,120]))
    parser.add_argument("--threads", default=(20))
    args = parser.parse_args()

    threads = args.threads

    static_path = '../Data/Static/basic.csv'
    df_basic = pd.read_csv(static_path)
    global n
    n = len(df_basic)
    k = n // threads
    ts_list = []
    for i in range(threads - 1):
        ts_list.append(df_basic.iloc[i * k:(i + 1) * k])
    ts_list.append(df_basic.iloc[(threads - 1) * k:])
    ts_codes_list = []
    for ts_ in ts_list:
        ts_codes_list.append(np.array(ts_['ts_code']))
    logger.info('读取到 {} 只股票基本数据'.format(n))

    daily_path = '../Data/Dynamic/daily_basic/'
    thread_list = []
    for i in range(len(ts_codes_list)):
        name = 't'+str(i)
        thread_example = threading.Thread(name=name, target=multi_thread,
                                          args=(ts_codes_list[i],daily_path))
        thread_list.append(thread_example)
    global start_time
    global count
    start_time = time.time()
    count = 0
    for th in thread_list:
        th.start()


