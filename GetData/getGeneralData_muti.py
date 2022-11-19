# 目前整合了股票（未复权、前复权、后复权）、指数、数字货币、ETF基金、期货、期权的行情数据
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
    test_log = logging.FileHandler('../logs/get_general.log', 'a', encoding='utf-8')
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

def get_general(ts_code,start_date=date.today().strftime("%Y%m%d"),end_date=date.today().strftime("%Y%m%d"),**kwargs):
    # 通用行情接口
    # 数据说明：交易日每天15点～16点之间入库
    adj = kwargs.get('adj')
    freq = kwargs.get('freq','D')
    asset = kwargs.get('asset','E')
    factors = kwargs.get('factors',[])
    adjfactor = kwargs.get('adjfactor',False)
    data = ts.pro_bar(ts_code=ts_code, adj=adj, freq=freq,start_date=start_date, end_date=end_date,ma=ma,asset=asset,factors=factors,adjfactor=adjfactor)
    return data

def updateData(ts_code,start_date=date.today().strftime("%Y%m%d"),end_date=date.today().strftime("%Y%m%d"),data_path='',mode='s',**kwargs):
    # model = s 增量更新
    #         f 全量更新
    try:
        if mode == 'f':a = int('a')
        # 增量
        file_path = data_path + ts_code + '.csv'
        if os.path.exists(file_path):
            df_hist = pd.read_csv(file_path)
            data = get_general(ts_code=ts_code, start_date=start_date, end_date=end_date,**kwargs)
            data['trade_date'] = pd.to_numeric(data['trade_date'])
            data = data.append(df_hist)
            data = data.drop_duplicates(['trade_date'])
        else:
            data = get_general(ts_code=ts_code, start_date=start_date, end_date=end_date, **kwargs)
        data.set_index('trade_date', inplace=True)
        data.to_csv(data_path + ts_code+'.csv')
    except IOError as e:
        logger.info('IOError -1')
        return -1
    except:
        try:
            data = get_general(ts_code=ts_code,start_date=start_date,end_date=end_date,**kwargs)
            data.set_index('trade_date', inplace=True)
            data.to_csv(data_path + ts_code + '.csv')
        except IOError as e:
            logger.info('IOError -1')
            return -1
        except:
            logger.info('Error -2, tscode : {}'.format(ts_code))
            return -2
    return 0

def multi_thread(ts_codes,*args):
    global count
    global n
    global start_time
    start_date,end_date,data_path,mode,adj,freq,ma = args
    for ts_code in ts_codes[:]:
        status = updateData(ts_code=ts_code, start_date=start_date, end_date=end_date, data_path=data_path, mode=mode, adj=adj,
                   freq=freq, ma=ma, asset='E', factors=['tor', 'vr'], adjfactor=True)
        while status == -1:
            time.sleep(10)
            status = updateData(ts_code=ts_code, start_date=start_date, end_date=end_date, data_path=data_path,
                                mode=mode, adj=adj,
                                freq=freq, ma=ma, asset='E', factors=['tor', 'vr'], adjfactor=True)
        if status == 0:
            count += 1
        if count % 100 == 0:
            logger.info('数据更新进度{0}/{1}，耗时:{2:.2f}s'.format(count, n, time.time() - start_time))
    end_time = time.time()
    logger.info('{0},{1} {2} 成功更新{3}只股票数据,耗时:{4:.2f}s'.format(freq,adj,date.today().strftime("%Y%m%d"), count,
                                                                           end_time - start_time))



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date",default=(date.today()-timedelta(365*30)).strftime("%Y%m%d"))
    parser.add_argument("--end-date",default=((date.today()).strftime("%Y%m%d")))
    parser.add_argument("--freq", default=('D'))
    parser.add_argument("--adj", default=('qfq'))
    parser.add_argument("--ma", default=([3,5,10,20,30,60,90,120]))
    parser.add_argument("--threads", default=(20))
    args = parser.parse_args()

    start_date = args.start_date
    end_date = args.end_date
    freq = args.freq
    adj = args.adj
    ma = args.ma
    threads = int(args.threads)

    logger.info('获取股票 {} 行情数据'.format(freq))
    logger.info('复权方式: {}'.format(adj))
    logger.info('均线统计: {}'.format(','.join(list(map(str,ma)))))

    static_path = '../Data/Static/basic.csv'
    df_basic = pd.read_csv(static_path)
    global n
    n = len(df_basic)
    k = n // threads
    ts_list = []
    for i in range(threads-1):
        ts_list.append(df_basic.iloc[i*k:(i+1)*k])
    ts_list.append(df_basic.iloc[(threads-1)*k:])
    ts_codes_list = []
    for ts_ in ts_list:
        ts_codes_list.append(np.array(ts_['ts_code']))
    logger.info('读取到 {} 只股票基本数据'.format(n))

    ##### 获取数据 #####
    base_path = '../Data/Dynamic/'
    freq_path_mapping = {'D':'daily/','W':'weekly/','M':'monthly/'}
    adj_path_mapping = {'qfq':'qfq/','hfq':'hfq/','wfq':'wfq/'}
    data_path = base_path + freq_path_mapping[freq] + adj_path_mapping[adj]

    mode = 'f'
    if adj == 'wfq':
        # start_date = (date.today()-timedelta(1)).strftime("%Y%m%d")
        mode = 'f'
        adj = None
    global count
    count = 0
    logger.info('开始更新 {0} : {1} 数据'.format(freq,adj))
    logger.info('数据存储路径 : {}'.format(data_path))
    thread_list = []
    for i in range(len(ts_codes_list)):
        name = 't'+str(i)
        thread_example = threading.Thread(name=name, target=multi_thread,
                                          args=(ts_codes_list[i], start_date, end_date, data_path, mode, adj, freq, ma))
        thread_list.append(thread_example)
    global start_time
    start_time = time.time()
    for th in thread_list:
        th.start()


