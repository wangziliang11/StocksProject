#!/usr/bin/python
# -*- coding: UTF-8 -*-
import tushare as ts
import numpy as np
import pandas as pd
import logging
from datetime import date, datetime, timedelta

def getLogging():
    # 创建logger对象
    logger = logging.getLogger('static_logger')
    # 设置日志等级
    logger.setLevel(logging.DEBUG)
    # 追加写入文件a ，设置utf-8编码防止中文写入乱码
    test_log = logging.FileHandler('../logs/get_static.log', 'a', encoding='utf-8')
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

def get_pro_api():
    mytoken = "f254ac9dc6e1df931994440e111c052577610c94d99ea957d13f9055"
    ts.set_token(mytoken)
    pro = ts.pro_api()
    return pro
pro = get_pro_api()
def get_stock_basic(fields=None):
    # 获取基础信息数据，包括股票代码、名称、上市日期、退市日期等
    fields='ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,curr_type,is_hs,list_date'
    data = pro.stock_basic( list_status='L', fields=fields)
    return data

def get_trade_cal():
    # 获取各大交易所交易日历数据,默认提取的是上交所
    data = pro.trade_cal(exchange='', start_date='20221101', end_date='20221105')
    return data

def get_namechange(ts_code):
    # 历史名称变更记录
    data = pro.namechange(ts_code=ts_code, fields='ts_code,name,start_date,end_date,change_reason')
    return data

def get_hs_const(hs_type=['SH','SZ']):
    # 获取沪股通、深股通成分数据
    data = []
    for hstype in hs_type:
        if len(data) == 0:
            data = pro.hs_const(hs_type=hstype)
        else:
            data = data.append(pro.hs_const(hs_type=hstype))
    return data

def get_stock_company():
    # 获取上市公司基础信息，单次提取4500条，可以根据交易所分批提取
    exchanges = ['SSE','SZSE']
    fields = 'ts_code,chairman,manager,secretary,reg_capital,setup_date,province,city,introduction,website,email,office,employees,main_business,business_scope'
    data = []
    for exchange in exchanges:
        if len(data) == 0:
            data = pro.stock_company(exchange=exchange, fields=fields)
        else:
            data = data.append(pro.stock_company(exchange=exchange, fields=fields))
    return data

def get_stk_managers(ts_list):
    # 获取上市公司管理层, 用户需要2000积分才可以调取，5000积分以上频次相对较高
    ts_code = ','.join(ts_list)
    data = pro.stk_managers(ts_code=ts_code)
    return data

def get_stk_rewards(ts_list):
    # 获取上市公司管理层薪酬和持股,用户需要2000积分才可以调取，5000积分以上频次相对较高
    ts_code = ','.join(ts_list)
    data = pro.stk_rewards(ts_code=ts_code)
    return data

def get_new_share(start_date='20221101',end_date='20221105'):
    # 获取新股上市列表数据 单次最大2000条，总量不限制
    data = pro.new_share(start_date=start_date, end_date=end_date)
    return data

if __name__ == '__main__':
    logger.info('开始获取股票静态信息 {}'.format(date.today().strftime("%Y%m%d")))
    df_basic = get_stock_basic()
    df_trade_cal = get_trade_cal()
    df_hs_const = get_hs_const()
    df_stock_company = get_stock_company()

    ### 获取公司管理层信息 ###
    # ts_list = np.array(df_basic['ts_code'].loc[:10])
    # df_stk_managers = get_stk_managers(ts_list)
    # df_stk_rewards = get_stk_rewards(ts_list)
    ### 获取公司管理层信息 ###

    df_basic.set_index('ts_code', inplace=True)
    df_hs_const.set_index('ts_code',inplace=True)
    df_stock_company.set_index('ts_code',inplace=True)
    # df_basic['symbol'].astype('str')

    df = df_basic.join(df_hs_const,on='ts_code')
    df = df.join(df_stock_company,on='ts_code')
    df.to_csv('../data/Static/basic.csv')
    logger.info('成功获取 {0} 只股票静态信息'.format(len(df)))