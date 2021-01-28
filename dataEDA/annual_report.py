#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/1/27 14:54
# @Author   : ReidChen
# Document  ：Annual report on overpass water.

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

column_list = ['S_NO', 'T_SYSTIME', 'N_VALUE']
# 下立交所有积水数据
# all_data = pd.read_csv('../data/EDAdata/EDAoverpass.csv', encoding='gbk', usecols=column_list)
all_data = pd.read_csv('../data/data/2020overpass.csv', encoding='gbk', usecols=column_list)

# 下立交对应的积水点与区域信息
sno_stainId = pd.read_csv('../data/data/SNO_STATIONID.csv', encoding='gbk')

all_data['T_SYSTIME'] = pd.to_datetime(all_data['T_SYSTIME'])
all_data.set_index(keys='T_SYSTIME', inplace=True)

'''任务一：按年份划分数据，统计S_NO的数量'''

def snostainId(x):
    y = sno_stainId[sno_stainId['S_NO'] == x]['S_DIST']
    return y


# 13-20 8年数据
year_nums = list()
dist_sno_ = pd.DataFrame()

# list = ['2020-01','2020-02','2020-03','2020-04','2020-05','2020-06']
# for i in list:
for i in range(2013, 2021, 1):
    # 统计每年不重复的积水点
    one_year_data = all_data.loc[str(i)]
    one_year_sno = one_year_data.drop_duplicates('S_NO')
    year_number = len(one_year_sno)
    year_nums.append([i, year_number])
    
    # 按地区统计当年的积水点
    this_year_data = one_year_sno.copy(deep=True)
    this_year_data['S_DIST'] = this_year_data['S_NO']
    this_year_data['S_DIST'].apply(lambda x: snostainId(x) )
    dist_sno = one_year_sno.guoupby('S_DIST').count()
    dist_sno['YEAR'] = i
    dist_sno_ = pd.concat([dist_sno_,dist_sno])
    

year_nums = pd.DataFrame(year_nums, columns=['YEAR', 'NUMS'])


# 对S_NO 进行处理，保留积水数据
