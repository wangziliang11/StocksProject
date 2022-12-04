import tushare as ts
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
import time
import logging
import argparse
import threading
import os
import json


def get_pro_api():
    mytoken = "f254ac9dc6e1df931994440e111c052577610c94d99ea957d13f9055"
    ts.set_token(mytoken)
    pro = ts.pro_api()
    return pro


pro = get_pro_api()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default=(date.today() - timedelta(50)).strftime("%Y%m%d"))
    parser.add_argument("--end-date", default=((date.today()).strftime("%Y%m%d")))
    parser.add_argument("--freq", default=('D'))
    parser.add_argument("--adj", default=('qfq'))
    parser.add_argument("--ma", default=([3, 5, 10, 15, 20, 30, 50, 60, 75, 90, 120, 150, 200]))
    parser.add_argument("--threads", default=(20))
    args = parser.parse_args()

    today = pd.datetime.today()
    if (today - pd.tseries.offsets.BDay(0)) > today:
        end_date = today - pd.tseries.offsets.BDay(1)
    else:
        end_date = today - pd.tseries.offsets.BDay(0)
    end_date = end_date.strftime("%Y%m%d")
    # day = np.where((today - pd.tseries.offsets.BDay(0)) > today,
    #          (today - pd.tseries.offsets.BDay(1)),
    #          (today - pd.tseries.offsets.BDay(0)))
    print(end_date)
    # print(type(day))
    # print(len(day))
    start_date = args.start_date
    # end_date = args.end_date
    freq = args.freq
    adj = args.adj
    ma = args.ma
    threads = int(args.threads)
    # end_date = '20221201'

    static_path = '../Data/Static/basic.csv'
    df_basic = pd.read_csv(static_path)
    print('共读取 {} 只股票信息'.format(len(df_basic)))
    # print(df_basic.columns)
    # 行业
    # df_basic = df_basic[['ts_code', 'name', 'industry']]
    industrys = df_basic['industry'].unique()
    df_basic = df_basic[['ts_code', 'market']]

    price_path = '../data/prices/price_{}.csv'.format(end_date)
    turnover_rate_path = '../data/turnover_rate/turnover_rate_{}.csv'.format(end_date)

    df_price = pd.read_csv(price_path)
    df_turn = pd.read_csv(turnover_rate_path)
    df_price = df_price[df_price['score'].isin([7])]
    # print(df_price.columns)
    for i in range(len(industrys)):
        industry = industrys[i]
        if i == 0:
            df_turnover = df_turn[df_turn['industry'].isin([industry])].sort_values(by=['turnover_rate_score'],
                                                                                    ascending=[False]).iloc[:10]
        else:
            df_tmp = df_turn[df_turn['industry'].isin([industry])].sort_values(by=['turnover_rate_score'],
                                                                               ascending=[False]).iloc[:10]
            df_turnover = pd.concat([df_turnover, df_tmp])
    # print(df_turnover.columns)
    price_codes = set(df_price['ts_code'].unique())
    turn_codes = set(df_turnover['ts_code'].unique())
    codes = price_codes - (price_codes - turn_codes)
    df_price.set_index('ts_code', inplace=True)
    df_turnover.set_index('ts_code', inplace=True)
    # print(df_turnover.columns)
    df = df_basic[df_basic['ts_code'].isin(list(codes))]
    df.set_index('ts_code', inplace=True)
    # print(df.columns)
    df = df.join(df_price[
                     ['name', 'industry', 'trade_date', 'close', 'ma3', 'ma5', 'ma10', 'ma15', 'ma20', 'ma30', 'ma50',
                      'ma60', 'ma75', 'ma90', 'ma120', 'ma150', 'ma200']], on='ts_code')

    df = df.join(df_turnover[['score1_3', 'score1_5', 'score1_10', 'score3_5', 'score3_10', 'score3_15', 'score3_20',
                              'score3_30', 'score5_10', 'score5_20', 'score5_30', 'week_score', 'month_score']],
                 on='ts_code')
    # print(df.columns)
    df.to_csv('../data/result/codes_{}.csv'.format(end_date))
    print('done')
