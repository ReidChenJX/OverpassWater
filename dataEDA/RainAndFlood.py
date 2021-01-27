#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/1/25 9:32
# @Author   : ReidChen
# Document  ：An exploratory analysis of the relationship between rainfall and stagnant water.

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
'''
任务一：绘制全年特定地区点的降雨与积水时序对比图

'''
Path_SNO = 2015060043


def get_one(SNO):
    # 将特定地区的积水与降雨单独提取出来
    observe_abute = pd.read_csv('../data/data/overpass_abute.csv', encoding='gbk')
    S_STATIONID = observe_abute[observe_abute.S_NO == SNO]['S_STATIONID'].values[0]
    
    rain_data_all = pd.read_csv('../data/data/2020rain_data.csv', encoding='gbk')
    one_rain_data = rain_data_all[rain_data_all.S_STATIONID == S_STATIONID].copy(deep=True)
    one_rain_data.drop_duplicates('D_TIME', inplace=True)
    one_rain_data.to_csv('../data/EDAdata/rain_data{SNO}.csv'.format(SNO=SNO), encoding='gbk', index=False)
    del rain_data_all
    
    overpass = pd.read_csv('../data/data/2020overpass.csv', encoding='gbk')
    one_overpass = overpass[overpass.S_NO == SNO].copy(deep=True)
    one_overpass.drop_duplicates('T_SYSTIME', inplace=True)
    one_overpass.to_csv('../data/EDAdata/overpass{SNO}.csv'.format(SNO=SNO), encoding='gbk', index=False)


# 判断当前采样点是否有数据，若无则生成新的数据
try:
    f = open('../data/EDAdata/overpass{SNO}.csv'.format(SNO=Path_SNO))
    f.close()
    print("Sample data already exists.")
except IOError:
    print("Create new sample data.")
    get_one(Path_SNO)


def RainAndFlood(SNO):
    # 读取积水与降雨，并按照时间进行排序
    rain_data = pd.read_csv('../data/EDAdata/rain_data{SNO}.csv'.format(SNO=SNO), encoding='gbk')
    overpass_data = pd.read_csv('../data/EDAdata/overpass{SNO}.csv'.format(SNO=SNO), encoding='gbk')
    
    rain_data['D_TIME'] = pd.to_datetime(rain_data['D_TIME'])
    rain_data.sort_values(by='D_TIME', inplace=True)
    rain_data.reset_index(inplace=True)
    rain_data['N_RAINVALUE'] = rain_data['N_RAINVALUE'].apply(lambda x: 0 if x < 0 else x)
    
    overpass_data['T_SYSTIME'] = pd.to_datetime(overpass_data['T_SYSTIME'])
    overpass_data.sort_values(by='T_SYSTIME', inplace=True)
    overpass_data.reset_index(inplace=True)
    
    # 重采样，同步数据时间
    rain_len, overpass_len = len(rain_data) - 1, len(overpass_data) - 1
    rain_index, overpass_index, rain_over_index = 0, 0, 0
    
    # 设计新的DataFrame，包括时间，降雨，积水
    rain_over_data = pd.DataFrame(columns=['TIME', 'N_RAINVALUE', 'N_VALUE'])
    
    # 当双指针均走到数据集最后，退出
    while rain_index < rain_len and overpass_index < overpass_len:
        rain_time = rain_data.loc[rain_index, 'D_TIME']
        overpass_time = overpass_data.loc[overpass_index, 'T_SYSTIME']
        
        if rain_time < overpass_time:
            time = rain_time
            # 当前时间的降雨数据与积水数据
            N_RAINVALUE = rain_data.loc[rain_index].N_RAINVALUE
            
            # 将此时降雨时间作为基础时间，积水值需向前取一位
            tem_index = overpass_index if overpass_index == 0 else overpass_index - 1
            N_VALUE = overpass_data.loc[tem_index].N_VALUE
            
            rain_over_data.loc[rain_over_index] = time, N_RAINVALUE, N_VALUE
            
            rain_index += 1
            rain_over_index += 1
        
        elif overpass_time < rain_time:
            time = overpass_time
            
            # 将此时积水时间作为基础时间，降雨数据向前取一位
            tem_index = rain_index if rain_index == 0 else rain_index - 1
            
            # 当前时间的降雨数据与积水数据
            N_RAINVALUE = rain_data.loc[tem_index].N_RAINVALUE
            N_VALUE = overpass_data.loc[overpass_index].N_VALUE
            
            rain_over_data.loc[rain_over_index] = time, N_RAINVALUE, N_VALUE
            
            overpass_index += 1
            rain_over_index += 1
        else:
            print('Not in the consider')
    
    rain_over_data.to_csv('../data/EDAdata/RainAndFlood{SNO}.csv'.format(SNO=SNO), encoding='gbk', index=False)


try:
    f = open('../data/EDAdata/RainAndFlood{SNO}.csv'.format(SNO=Path_SNO))
    f.close()
    print("Sample rain and flood data already exists.")
except IOError:
    print("Create new rain and flood data.")
    RainAndFlood(Path_SNO)

rain_over_data = pd.read_csv('../data/EDAdata/RainAndFlood{SNO}.csv'.format(SNO=Path_SNO), encoding='gbk')
rain_over_data = rain_over_data[['TIME', 'N_RAINVALUE', 'N_VALUE']]
rain_over_data['TIME'] = pd.to_datetime(rain_over_data['TIME'])
rain_over_data.set_index(keys='TIME', inplace=True)
rain_over_data.sort_index(inplace=True)

plt.figure(figsize=(15, 10), dpi=200)
plt.plot(rain_over_data['N_RAINVALUE'])
plt.plot(rain_over_data['N_VALUE'])
plt.title('The rain and flood data order by time.')
plt.ylabel('Value')
plt.xlabel('Time')
plt.legend(['Rain_value', 'Flood_value'], loc='upper right')
plt.show()
rain_over_data.reset_index()