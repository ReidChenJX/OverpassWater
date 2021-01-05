#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2020/12/29 13:37
# @Author   : ReidChen

import pandas as pd
import numpy as np


'''进行格式转化，减少内存使用。'''
def otMenory(data):
    # 将数据读取到内存后，进行格式转化，减少内存使用。
    data[data.select_dtypes('int64').columns] = data.select_dtypes('int64').apply(pd.to_numeric, downcast='unsigned')
    data[data.select_dtypes('float').columns] = data.select_dtypes('float').apply(pd.to_numeric, downcast='float')
    
    obj = data.select_dtypes('object')
    for col in obj.columns:
        if len(obj[col].unique()) / len(obj[col]) < 0.4:
            data[col] = data[col].astype('category')
    
    return data
        
