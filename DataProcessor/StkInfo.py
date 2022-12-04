import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta
import json

class StkInfo():
    def __init__(self,ts_code):
        '''
        ts_code 信息，包括基本静态信息，行情信息，基本面信息等
        :param ts_code:
        '''
        self.ts_code = ts_code
        self.static_info = self.get_static_info(ts_code)
        self.daily_info = self.get_daily_info()
        self.fundamentals_info = self.get_fundamentals_info(ts_code)

    def get_static_info(self,ts_code):
        '''
        可获取ts_code 静态信息
        :param ts_code:
        :return: result
        包括 股票名字(name)、行业(industry)、市场类型(market)、地域(area)、沪深港通(is_hs)、上市日期(list_date)、主营业务产品(main_business)、经营领域(business_scope)等字段
        '''
        static_path = '../data/Static/basic.csv'
        df_basic = pd.read_csv(static_path)
        res = dict()
        df = df_basic.loc[df_basic['ts_code'].isin([ts_code])]
        industry = df['industry'].values[0]
        market = df['market'].values[0]
        name = df['name'].values[0]
        area = df['area'].values[0]
        is_hs = df['is_hs'].values[0]
        list_date = df['list_date'].values[0]
        main_business = df['main_business'].values[0]
        business_scope = df['business_scope'].values[0]
        res = {'ts_code': ts_code, 'name': name, 'industry': industry, 'market': market, 'area': area,
               'list_date': str(list_date), 'is_hs': is_hs, 'main_business': main_business, 'business_scope': business_scope}
        # return json.dumps(res,ensure_ascii=False)
        return res

    def get_daily_info(self,trade_date=None,start_date=date.today().strftime("%Y%m%d"),end_date=date.today().strftime("%Y%m%d")):
        '''
        可获取股票日行情信息
        :param ts_code:
        :param trade_date: 交易日期(二选一，优先级高，默认当天)
        :param start_date: 开始日期(二选一，优先级低，默认当天)
        :param end_date: 结束日期(二选一，优先级低，默认当天)
        :return:
        '''
        result = []
        ts_code = self.ts_code
        daily_path = '../data/Dynamic/Daily/qfq/' + ts_code + '.csv'
        daily_basic_path = '../data/Dynamic/daily_basic/' + ts_code + '.csv'
        df_daily = pd.read_csv(daily_path)
        df_daily_basic = pd.read_csv(daily_basic_path)
        if trade_date:
            res = dict()
            trade_date = int(trade_date)
            df_daily = df_daily.loc[df_daily['trade_date'].isin([trade_date])]
            df_daily_basic = df_daily_basic.loc[df_daily_basic['trade_date'].isin([trade_date])]
            df_daily.set_index('trade_date',inplace=True)
            df_daily_basic.set_index('trade_date',inplace=True)
            if len(df_daily) == 0 or len(df_daily_basic) == 0:
                return result
            df = df_daily.join(df_daily_basic,on='trade_date')
            name = self.static_info.get('name','')
            res = {
                'ts_code':ts_code,'name':name,'trade_date':trade_date,'open':df['open'].values[0],'high':df['high'].values[0],'low':df['low'].values[0],
                'close':df['close'].values[0],'change':round(df['change'].values[0],4),'pct_chg':df['pct_chg'].values[0],'vol':df['vol'].values[0],
                'volume_ratio':df['volume_ratio'].values[0],'amount':df['amount'].values[0],'turnover_rate':df['turnover_rate'].values[0],
                'pe':df['pe'].values[0],'pb':df['pb'].values[0],'ps':df['ps'].values[0],'total_mv':df['total_mv'].values[0]
            }
            ma_dict = {mas: df[mas].values[0] for mas in ['ma' + str(k) for k in [3, 5, 10, 20, 30, 60, 90, 120]]}
            mav_dict = {mav: df[mav].values[0] for mav in ['ma_v_' + str(k) for k in [3, 5, 10, 20, 30, 60, 90, 120]]}
            res.update(ma_dict)
            res.update(mav_dict)
            return [res]
        else:
            start_date = int(start_date)
            end_date = int(end_date)
            for date in range(start_date,end_date+1):
                res = dict()
                df_daily = df_daily.loc[df_daily['trade_date'].isin([date])]
                df_daily_basic = df_daily_basic.loc[df_daily_basic['trade_date'].isin([date])]
                df_daily.set_index('trade_date', inplace=True)
                df_daily_basic.set_index('trade_date', inplace=True)
                if len(df_daily) == 0 or len(df_daily_basic) == 0:
                    continue
                df = df_daily.join(df_daily_basic, on='trade_date')
                name = self.static_info.get('name', '')

                result.append(res)


        return json.dumps(res,ensure_ascii=False)

    def get_fundamentals_info(self,ts_code=None):
        res = dict()
        return json.dumps(res,ensure_ascii=False)

    def __call__(self, *args, **kwargs):
        return self.static_info
