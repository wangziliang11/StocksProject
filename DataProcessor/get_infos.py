# 股票所属行业分析
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
from StkInfo import StkInfo


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--ts-code",default=['000001.SZ','000002.SZ'])
    parser.add_argument("--trade-date", default=date.today().strftime("%Y%m%d"))
    args = parser.parse_args()

    ts_code = args.ts_code
    trade_date = args.trade_date
    trade_date = 20221118

    stkinfo = StkInfo(ts_code[0])
    # print(stkinfo.ts_code)
    # print(stkinfo.static_info)
    # print('***')
    # print(stkinfo())
    print(stkinfo.get_daily_info(trade_date))
