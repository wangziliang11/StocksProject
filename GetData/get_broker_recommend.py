# 每月初获取券商月度金股
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
    test_log = logging.FileHandler('../logs/get_broker_recommend.log', 'a', encoding='utf-8')
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

def get_broker_recommend(month):
    data = pro.broker_recommend(month=month)
    return data

def updateData(month,data_path):
    try:
        file_path = data_path + month + '.csv'
        data = get_broker_recommend(month=month)
        data.to_csv(file_path)
    except IOError as e:
        logger.info('IOError -1')
        return -1
    except Exception as e:
        if (str(e).count('接口')>0):
            logger.info('IOError -1')
            return -1
        else:
            logger.info('Error -2, month : {}'.format(month))
            return -2
    return 0

def multi_thread(month,*args):
    global count
    global n
    global start_time
    data_path = args[0]

    status = updateData(month=month,data_path=data_path)
    end_time = time.time()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", default='202211')

    args = parser.parse_args()
    month = args.month

    ##### 获取数据 #####
    data_path = '../Data/Dynamic/broker_recommend/'
    mode = 'f'
    global count
    global start_time
    count = 0
    start_time = time.time()
    logger.info('开始更新 {} broker_recommend 数据'.format(date.today().strftime("%Y%m%d")))
    logger.info('数据存储路径 : {}'.format(data_path))
    multi_thread(month,data_path)


