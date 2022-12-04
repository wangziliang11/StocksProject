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

def Compare(x,y):
    if x >= y:
        return 1
    return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date",default=(date.today()-timedelta(1)).strftime("%Y%m%d"))
    parser.add_argument("--end-date",default=((date.today()).strftime("%Y%m%d")))
    parser.add_argument("--freq", default=('D'))
    parser.add_argument("--adj", default=('qfq'))
    parser.add_argument("--ma", default=([3,5,10,15,20,30,50,60,75,90,120,150,200]))
    parser.add_argument("--threads", default=(20))
    args = parser.parse_args()

    start_date1 = int(args.start_date)
    end_date1 = int(args.end_date)
    freq = args.freq
    adj = args.adj
    ma = args.ma
    threads = int(args.threads)

    static_path = '../Data/Static/basic.csv'
    df_basic = pd.read_csv(static_path)
    print('共读取 {} 只股票信息'.format(len(df_basic)))
    # 行业
    df_basic = df_basic[['ts_code', 'name', 'industry']]
    industrys = df_basic['industry'].unique()
    industry_dict = dict()
    for industry in industrys:
        industry_dict[industry] = df_basic[df_basic['industry'].isin([industry])]['ts_code'].values
    df_basic.set_index('ts_code', inplace=True)
    n = 1
    for i in range(n):
        start_date = (datetime.strptime(str(start_date1), "%Y%m%d") - timedelta(i)).strftime("%Y%m%d")
        end_date = (datetime.strptime(str(end_date1), "%Y%m%d") - timedelta(i)).strftime("%Y%m%d")
        start_date = int(start_date)
        end_date = int(end_date)

        prices_dict = {'industry': [], 'ts_code': [], 'name': [], 'trade_date': [], 'ma1': [], 'ma3': [], 'ma5': [],
                              'ma10': [], 'ma15': [], 'ma20': [], 'ma30': [], 'ma50': [], 'ma60': [],'ma75': [],'ma90': [],
                              'ma120': [],'ma150': [],'ma200': [],'ratio_min':[],'ratio_max':[],'score':[]}
        start_time = time.time()
        df_price = None
        start_flag = True
        ts_max_min = dict()
        for industry in industry_dict:
            for i in range(len(industry_dict[industry])):
                try:
                    ts_code = industry_dict[industry][i]
                    code_path = '../data/Dynamic/Daily/qfq/{}.csv'.format(ts_code)
                    columns = ['ts_code','trade_date','close','ma3','ma5','ma10','ma15','ma20','ma30','ma50','ma60','ma75','ma90','ma120','ma150','ma200']
                    df_code = pd.read_csv(code_path)[columns].iloc[:270]
                    ts_max_min[ts_code] = {'max':df_code['close'].max(),'min':df_code['close'].min()}
                    if start_flag:
                        df_price = df_code
                        start_flag = False
                    else:
                        df_price = pd.concat([df_price,df_code])
                except:
                    continue
        print(len(df_price))
        df_price.fillna(value=0.0)
        df_price['price_1_150'] = df_price.apply(lambda x: Compare(x.close, x.ma150),axis=1)
        df_price['price_1_200'] = df_price.apply(lambda x:Compare(x.close,x.ma200),axis=1)
        df_price['price_150_200'] = df_price.apply(lambda x: Compare(x.ma150, x.ma200),axis=1)
        df_price['price_50_150'] = df_price.apply(lambda x: Compare(x.ma50, x.ma150),axis=1)
        df_price['price_50_200'] = df_price.apply(lambda x: Compare(x.ma50, x.ma200),axis=1)
        df_price['price_max_25'] = df_price.apply(lambda x: Compare(x.close,ts_max_min[x.ts_code]['max']*0.75),axis=1)
        df_price['price_min_25'] = df_price.apply(lambda x: Compare(x.close,ts_max_min[x.ts_code]['min'] * 1.25),axis=1)
        df_price.fillna(value=0.0)
        df_price['score'] = df_price.apply(lambda x: sum([x.price_1_150, x.price_1_200,x.price_150_200,x.price_50_150,x.price_50_200,x.price_max_25,x.price_min_25]), axis=1)
        # df_price.eval('score = close + ma150', inplace=True)
        df_price.set_index('ts_code', inplace=True)
        df_price = df_basic.join(df_price, on='ts_code')
        print('len(df_price):',len(df_price))
        # for d in range(start_date, end_date + 1):
        for d in range(5):
            early_day = (datetime.strptime(str(end_date), "%Y%m%d") - timedelta(d)).strftime("%Y%m%d")
            data = df_price[df_price['trade_date'].isin([int(early_day)])]
            print(early_day,len(data))
            if len(data) == 0:continue
            data = data.sort_values(by=['score'],ascending=[False])
            data.to_csv('../data/prices/price_{}.csv'.format(str(int(data.head(1)['trade_date'].values[0]))))
        print('done',time.time()-start_time)

