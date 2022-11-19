# 获取上市公司财务利润表数据
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
    test_log = logging.FileHandler('../logs/get_income.log', 'a', encoding='utf-8')
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

def get_income(ts_code):
    fields = 'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps'
    data = pro.income(ts_code=ts_code)
    return data

def updateData(ts_code,data_path):
    try:
        file_path = data_path + ts_code + '.csv'
        data = get_income(ts_code=ts_code)
        data.to_csv(file_path)
    except IOError as e:
        logger.info('IOError -1')
        return -1
    except Exception as e:
        if (str(e).count('接口')>0):
            logger.info('IOError -1')
            return -1
        else:
            logger.info('Error -2, tscode : {}'.format(ts_code))
            return -2
    return 0

def multi_thread(ts_codes,*args):
    global count
    global n
    global start_time
    data_path = args[0]
    for ts_code in ts_codes[:]:
        status = updateData(ts_code=ts_code,data_path=data_path)
        while status == -1:
            time.sleep(10)
            status = updateData(ts_code=ts_code,data_path=data_path)
        if status == 0:
            count += 1
        if count % 100 == 0:
            logger.info('数据更新进度{0}/{1}，耗时:{2:.2f}s'.format(count, n, time.time() - start_time))
    end_time = time.time()
    logger.info(
        '{0} 成功更新{1}只股票数据,耗时:{2:.2f}s'.format(date.today().strftime("%Y%m%d"), count, end_time - start_time))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
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

    ##### 获取数据 #####
    data_path = '../Data/Dynamic/income/'
    mode = 'f'
    global count
    global start_time
    count = 0
    start_time = time.time()
    logger.info('开始更新 {} income 数据'.format(date.today().strftime("%Y%m%d")))
    logger.info('数据存储路径 : {}'.format(data_path))
    thread_list = []
    for i in range(len(ts_codes_list)):
        name = 't' + str(i)
        thread_example = threading.Thread(name=name, target=multi_thread,
                                          args=(ts_codes_list[i], data_path))
        thread_list.append(thread_example)
    start_time = time.time()
    for th in thread_list:
        th.start()
