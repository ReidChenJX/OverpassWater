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
# 以时间为数据索引
all_data.set_index(keys='T_SYSTIME', inplace=True)

'''任务一：按年份, 区域划分数据，统计S_NO的数量'''

def snostainId(x):
    # 将S_DIST编码换为具体地区名称
    y = sno_stainId[sno_stainId['S_NO'] == x]['S_DIST'].values[0]
    return y

year_nums = list()
dist_sno_ = list()

for i in range(2013, 2021, 1):
    # 统计每年不重复的积水点
    one_year_data = all_data.loc[str(i)]
    one_year_sno = one_year_data.drop_duplicates('S_NO')
    year_number = len(one_year_sno)
    year_nums.append([i, year_number])
    
    # 按地区统计当年的积水点
    this_year_data = one_year_sno.copy(deep=True)
    this_year_data['S_DIST'] = this_year_data['S_NO']
    this_year_data['S_DIST'] = this_year_data['S_DIST'].apply(lambda x: snostainId(x))
    dist_sno = this_year_data.groupby('S_DIST').count()
    for dist in dist_sno.itertuples(index=True):
        a = [i, dist.Index, dist.S_NO]
        dist_sno_.append(a)
    

year_nums = pd.DataFrame(year_nums, columns=['YEAR', 'NUMS'])
dist_sno_ = pd.DataFrame(dist_sno_, columns=['YEAR', 'S_DIST', 'NUMS'])


'''任务二：以10为界限，统计各地区每年的积水次数'''


def hydrops_data(pos_index):
    # 积水级别：（10-15）一级，（15-25）二级，（25-50）三级，（50--）四级
    hydrops_data = pd.DataFrame(columns=['S_NO', 'START_TIME', 'END_TIME', 'DEEP', 'JSRANK'])
    hydrops_data_index = 0
    
    for JSD_NO in pos_index:
        # 根据S_NO获取积水点的数据
        JSD_value = all_data[all_data['S_NO'] == JSD_NO]
        JSD_value_time = JSD_value.sort_index()  # 按照时间排序
        
        # 依次读取监测数据，记录积水开始时间，结束时间，积水深度
        log = 0  # 标志位，1代表正在积水
        start_time = np.nan
        water_deep = 0.0
        jsrank = 0
        
        # 获取每一段监测数值，判断当前积水情况，并写入hydrops_data
        for value in JSD_value_time.itertuples(index=False):
            if value.N_VALUE >= 10 and log == 1:  # 正在积水并还在积水
                water_deep = max(water_deep, value.N_VALUE)
            if value.N_VALUE < 10 and log == 1:  # 正在积水但在此时退出积水
                if start_time == np.nan : start_time = value.Index
                end_time = value.Index
                if 10 <= water_deep < 15:  # 判断积水深度等级
                    jsrank = 1
                elif 15 <= water_deep < 25:
                    jsrank = 2
                elif 25 <= water_deep < 50:
                    jsrank = 3
                elif water_deep >= 50:
                    jsrank = 4
                hydrops_data.loc[hydrops_data_index] = [value.S_NO, start_time, end_time, water_deep, jsrank]
                log, water_deep = 0, 0.0
                hydrops_data_index += 1
            if value.N_VALUE >= 10 and log == 0:  # 不积水，此时开始积水
                start_time = value.Index
                log = 1
                water_deep = max(water_deep, value.N_VALUE)
            if value.N_VALUE < 10 and log == 0: continue  # 不积水
    
    hydrops_data.to_csv('../data/EDAdata/hydrops_data.csv', encoding='gbk', index=False)
    return hydrops_data

s_no_index = all_data['S_NO'].drop_duplicates().values.tolist()
hydrops_data = hydrops_data(s_no_index)

