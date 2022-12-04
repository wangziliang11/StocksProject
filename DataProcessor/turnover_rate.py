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

def calc_ma_x(df,x):
    return df.head(x)['turnover_rate'].agg([np.mean]).values[0]

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date",default=(date.today()-timedelta(100)).strftime("%Y%m%d"))
    parser.add_argument("--end-date",default=((date.today()).strftime("%Y%m%d")))
    parser.add_argument("--freq", default=('D'))
    parser.add_argument("--adj", default=('qfq'))
    parser.add_argument("--ma", default=([3,5,10,20,30,60,90,120]))
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
    n = 3
    for i in range(n):
        start_date = (datetime.strptime(str(start_date1), "%Y%m%d") - timedelta(i)).strftime("%Y%m%d")
        end_date = (datetime.strptime(str(end_date1), "%Y%m%d") - timedelta(i)).strftime("%Y%m%d")
        start_date = int(start_date)
        end_date = int(end_date)
        print(end_date)

        for d in range(start_date, end_date + 1):
            if d == start_date:
                df_daily_basic = pro.daily_basic(trade_date=d, fields='ts_code,trade_date,pe,turnover_rate,total_mv')
            else:
                df = pro.daily_basic(trade_date=d, fields='ts_code,trade_date,pe,turnover_rate,total_mv')
                df_daily_basic = pd.concat([df_daily_basic,df])
        df_daily_basic = df_daily_basic.sort_values(by=['trade_date'],ascending=[False])
        print('data lens : ',len(df_daily_basic))
        trade_dates = df_daily_basic['trade_date'].unique()
        print('trade_dates : ',len(trade_dates))

        turnover_rate_dict = {'industry': [], 'ts_code': [], 'name': [], 'trade_date': [], 'ma1': [], 'ma3': [], 'ma5': [],
                              'ma10': [], 'ma15': [], 'ma20': [], 'ma30': [], 'ma45': [], 'ma60': [], 'score1_3': [],
                              'score1_5': [], 'score1_10': [], 'score3_5': [], 'score3_10': [],
                              'score3_15': [], 'score3_20': [], 'score3_30': [], 'score5_10': [], 'score5_20': [],
                              'score5_30': [], 'week_score':[],'month_score': [],
                              'turnover_rate_score': []}
        start_time = time.time()

        for industry in industry_dict:
            df_daily = df_daily_basic[df_daily_basic['trade_date'].isin(['20221118'])]
            df_daily = df_daily[df_daily['ts_code'].isin(industry_dict[industry])]
            count = 0
            for ts_code in industry_dict[industry]:
                ### 换手率 ###
                try:
                    df_turnover_rate = df_daily_basic[df_daily_basic['ts_code'].isin([ts_code])]
                    df_turnover_rate = df_turnover_rate.sort_values(by=['trade_date'],ascending=[False])
                    turnover_rate_today = df_turnover_rate.head(1)['turnover_rate'].values[0]
                    turnover_rate_month = df_turnover_rate.head(20)['turnover_rate'].agg([np.mean]).values[0]
                    turnover_rate_lastmonth = df_turnover_rate.iloc[20:40]['turnover_rate'].agg([np.mean]).values[0]
                    turnover_rate_week = df_turnover_rate.head(5)['turnover_rate'].agg([np.mean]).values[0]
                    turnover_rate_lastweek = df_turnover_rate.iloc[5:10]['turnover_rate'].agg([np.mean]).values[0]
                    turnover_rate_dict['ma1'].append(turnover_rate_today)
                    days = [3,5,10,15,20,30,45,60]
                    turnover_day_mean = []
                    ma = dict()
                    for day in days:
                        ma[day] = df_turnover_rate.head(day)['turnover_rate'].agg([np.mean]).values[0]
                except:
                    continue
                score = 0
                for i in range(len(days)-1):
                    for j in range(i+1,len(days)):
                        score += ma[days[i]]/ma[days[j]]
                for day in days:
                    turnover_rate_dict['ma' + str(day)].append(ma[day])
                ma[1] = turnover_rate_today
                turnover_rate_dict['score1_3'].append(ma[1] / ma[3])
                turnover_rate_dict['score1_5'].append(ma[1] / ma[5])
                turnover_rate_dict['score1_10'].append(ma[1] / ma[10])
                turnover_rate_dict['score3_5'].append(ma[3]/ma[5])
                turnover_rate_dict['score3_10'].append(ma[3] / ma[10])
                turnover_rate_dict['score3_15'].append(ma[3] / ma[15])
                turnover_rate_dict['score3_20'].append(ma[3] / ma[20])
                turnover_rate_dict['score3_30'].append(ma[3] / ma[30])
                turnover_rate_dict['score5_10'].append(ma[5] / ma[10])
                turnover_rate_dict['score5_20'].append(ma[5] / ma[20])
                turnover_rate_dict['score5_30'].append(ma[5] / ma[30])
                week_score = turnover_rate_week / turnover_rate_lastweek
                turnover_rate_dict['week_score'].append(round(week_score, 4))
                month_score = turnover_rate_month / turnover_rate_lastmonth
                turnover_rate_dict['month_score'].append(round(month_score,4))
                name = df_basic[df_basic['ts_code'].isin([ts_code])]['name'].values[0]
                trade_date = df_turnover_rate.head(1)['trade_date'].values[0]
                turnover_rate_dict['trade_date'].append(trade_date)
                turnover_rate_dict['name'].append(name)
                turnover_rate_dict['ts_code'].append(ts_code)
                turnover_rate_dict['industry'].append(industry)
                turnover_rate_dict['turnover_rate_score'].append(round(score,4))
                # break
            # break
        data = pd.DataFrame(turnover_rate_dict)
        data = data.sort_values(by=['turnover_rate_score'], ascending=[False])
        data.set_index('ts_code',inplace=True)
        data.to_csv('../data/turnover_rate/turnover_rate_{}.csv'.format(df_turnover_rate.head(1)['trade_date'].values[0]))
        print('done',time.time()-start_time)

