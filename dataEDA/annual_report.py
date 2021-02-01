#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/1/27 14:54
# @Author   : ReidChen
# Document  ：Annual report on overpass water.

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 下立交对应的积水点与区域信息
sno_stainId = pd.read_csv('../data/data/SNO_STATIONID.csv', encoding='gbk')


def snostainId(x):
    # 将S_DIST编码换为具体地区名称
    y = sno_stainId[sno_stainId['S_NO'] == x]['S_DIST'].values[0]
    return y


'''任务一：按年份, 区域划分数据，统计S_NO的数量'''


def annual_sno():
    # 按照年度来区分统计数据
    
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
    # 每年在运行的积水点个数
    year_nums.to_csv('../data/EDAdata/每年在运行的积水点个数.csv', encoding='gbk', index=False)
    
    dist_sno_ = pd.DataFrame(dist_sno_, columns=['YEAR', 'S_DIST', 'NUMS'])
    # 各地区每年在运行的积水点个数
    dist_sno_.to_csv('../data/EDAdata/各地区每年在运行的积水点个数.csv', encoding='gbk', index=False)


try:
    f = open('../data/EDAdata/各地区每年在运行的积水点个数.csv')
    f.close()
    print("The first task has been completed.")
except IOError:
    print("Begin the first task.")
    column_list = ['S_NO', 'T_SYSTIME', 'N_VALUE']
    # 下立交所有积水数据
    all_data = pd.read_csv('../data/EDAdata/EDAoverpass.csv', encoding='gbk', usecols=column_list)
    all_data['T_SYSTIME'] = pd.to_datetime(all_data['T_SYSTIME'])
    # 以时间为数据索引
    all_data.set_index(keys='T_SYSTIME', inplace=True)
    annual_sno()

'''任务二：统计各地区每年的积水时间与积水深度'''


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
        js_rank = 0
        
        # 获取每一段监测数值，判断当前积水情况，并写入hydrops_data
        ## 若用10直接判断是否积水，在震荡点会产生大量相似数据，降低判断标准，但一级积水标准不变
        base_line = 6  # 退出积水的标准
        flood_line = 10  # 开始积水的标准
        end_time = pd.to_datetime('2013-01-01 00:00:00')
        for value in JSD_value_time.itertuples(index=True):
            if value.N_VALUE >= base_line and log == 1:  # 正在积水并还在积水
                water_deep = max(water_deep, value.N_VALUE)
                end_time = value.Index
            if value.N_VALUE < base_line and log == 1:  # 正在积水但在此时退出积水
                if start_time == np.nan:
                    start_time = value.Index
                if end_time <= start_time:
                    continue
                
                if 10 <= water_deep < 15:  # 判断积水深度等级
                    js_rank = 1
                elif 15 <= water_deep < 25:
                    js_rank = 2
                elif 25 <= water_deep < 50:
                    js_rank = 3
                elif water_deep >= 50:
                    js_rank = 4
                
                hydrops_data.loc[hydrops_data_index] = [value.S_NO, start_time, end_time, water_deep, js_rank]
                log, water_deep = 0, 0.0
                hydrops_data_index += 1
            if value.N_VALUE >= flood_line and log == 0:  # 不积水，此时开始积水
                start_time = value.Index
                log = 1
                water_deep = max(water_deep, value.N_VALUE)
            if value.N_VALUE < flood_line and log == 0:
                continue  # 不积水
    
    hydrops_data.to_csv('../data/EDAdata/每一次积水的时间与深度.csv', encoding='gbk', index=False)
    return hydrops_data


try:
    f = open('../data/EDAdata/每一次积水的时间与深度.csv')
    f.close()
    print("The second task has been completed, using existing data.")
    time_deep = pd.read_csv('../data/EDAdata/每一次积水的时间与深度.csv', encoding='gbk')
except IOError:
    print("Begin the second task.")
    # 每个积水点的积水时间、深度、等级表
    s_no_index = all_data['S_NO'].drop_duplicates().values.tolist()
    time_deep = hydrops_data(s_no_index)


def sec_dy_data():
    # 增加积水点的地区信息
    time_deep['S_DIST'] = time_deep['S_NO']
    time_deep['S_DIST'] = time_deep['S_DIST'].apply(lambda x: snostainId(x))
    time_deep['START_TIME'] = pd.to_datetime(time_deep['START_TIME'])
    time_deep['END_TIME'] = pd.to_datetime(time_deep['END_TIME'])
    time_deep['DURATION'] = time_deep['END_TIME'] - time_deep['START_TIME']
    time_deep['DURATION'] = time_deep['DURATION'].apply(lambda x: x.seconds / 3600)  # seconds为秒
    
    time_deep.set_index(keys='START_TIME', inplace=True)  # 由积水开始时间作为数据索引
    
    year_dist_nums = list()
    year_freq_nums = list()
    year_dist_dura = list()
    
    # 各地区每年积水数量大于等于10的积水点个数
    for i in range(2013, 2021, 1):
        one_year_data = time_deep.loc[str(i)]
        one_year_deep = one_year_data.drop_duplicates('S_NO')
        # 按照地区进行归类，各地区每年积水点个数
        dist_sno = one_year_deep.groupby('S_DIST').count()
        for dist in dist_sno.itertuples(index=True):
            year_dist_nums.append([i, dist.Index, dist.S_NO])
        
        # 各地区每年的积水次数
        dist_freq = one_year_data.groupby('S_DIST').count()
        for dist in dist_freq.itertuples(index=True):
            year_freq_nums.append([i, dist.Index, dist.S_NO])
        
        # 各地区总的积水时长
        dist_duration = one_year_data.groupby('S_DIST').sum()
        for dist in dist_duration.itertuples(index=True):
            year_dist_dura.append([i, dist.Index, dist.DURATION])
    
    year_dist_nums = pd.DataFrame(year_dist_nums, columns=['YEAR', 'DIST', 'NUMS'])
    year_freq_nums = pd.DataFrame(year_freq_nums, columns=['YEAR', 'DIST', 'NUMS'])
    year_dist_dura = pd.DataFrame(year_dist_dura, columns=['YEAR', 'DIST', 'DURATION'])
    
    year_dist_nums.to_csv('../data/EDAdata/各地区每年积水大于10的积水点个数.csv', encoding='gbk', index=False)
    year_freq_nums.to_csv('../data/EDAdata/各地区每年积水大于10的积水次数.csv', encoding='gbk', index=False)
    year_dist_dura.to_csv('../data/EDAdata/各地区每年总的积水时长.csv', encoding='gbk', index=False)


try:
    f = open('../data/EDAdata/各地区每年总的积水时长.csv')
    f.close()
    print("Data on regional water has been processed.")
except IOError:
    print("Using hydrops data create section flood data.")
    # 每个积水点的积水时间、深度、等级表
    sec_dy_data()


'''任务三：统计积水对应的降雨，降雨量，降雨时长，预测积水的持续时间，最大深度'''
# 每一次积水的时间与深度
EDArain_data = pd.read_csv('../data/EDAdata/EDAraindata.csv',encoding='gbk')

time_deep = time_deep
path_sno = 2015060043

