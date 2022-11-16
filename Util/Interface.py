import codecs
import math
import numpy as np
import matplotlib.pyplot as plt
import tushare as ts
import pandas as pd

def pro():
    mytoken = "f254ac9dc6e1df931994440e111c052577610c94d99ea957d13f9055"
    ts.set_token(mytoken)
    pro = ts.pro_api()
    return pro
if __name__ == '__main__':
    print('PyCharm')
