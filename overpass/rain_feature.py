#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2020/12/29
# @Author   : ReidChen

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import sys
sys.path.append('./')
import OPTools

'''数据表2020rain_data，属性表overpass_abute'''
'''结果：降雨数据表 rain_data 降雨时间，持续时间，降雨量；观测站表 observe_abute 观测站属性与降雨总量，降雨时长'''

'''根据S_STATIONID，推算降雨开始时间，结束时间，降雨量'''
def rain_data(observe_rain_data, observe_abute):
    # 记录雨量站，降雨时间，降雨量，雨量评级。默认0.2/小时为降雨
    rain_data = pd.DataFrame(columns=['S_STATIONID', 'START_TIME', 'END_TIME',
                                      'DURATION', 'N_RAINVALUE', 'RARANK', 'S_DIST'])
    rain_data_index = 0
    
    observe_index = observe_abute.index
    for stationID in observe_index:
        # 根据S_STATIONID获取观察点的数据
        rain_value = observe_rain_data[observe_rain_data['S_STATIONID'] == stationID][[
                    'S_STATIONID', 'D_TIME', 'N_RAINVALUE', 'S_DIST']]
        rain_value.sort_values('D_TIME', inplace=True)  # 按照时间排序
    
        # 依次读取监测数据，记录降雨开始时间，结束时间，降雨量
        log = 0             # 标志位，1代表正在下雨
        rain_fall = 0.0     # 总降雨量
        rain_time = 0       # 降雨持续时间，只有大于1（10分钟）才为有效降雨
        
    
        for value in rain_value.itertuples(index=False):
            if value.N_RAINVALUE >= 0.2 and log == 0:       # 开始降雨
                start_time = value.D_TIME
                # start_time += timedelta(hours=-1)           # 时间前移一小时
                rain_fall += value.N_RAINVALUE
                log = 1
                rain_time += 1
            elif value.N_RAINVALUE < 0.2 and log == 0:      # 未降雨
                continue
            elif value.N_RAINVALUE >= 0.2 and log == 1:     #正在降雨
                rain_fall += value.N_RAINVALUE
                rain_time += 1
            elif value.N_RAINVALUE < 0.2 and log == 1:      # 雨停
                # 雨停，只有rain_time>2为有效降雨
                if rain_time == 1:
                    rain_time = 0
                    rain_fall = 0
                    log = 0
                    continue
                else:
                    end_time = value.D_TIME
                    # end_time += timedelta(hours=-1)  # 时间前移一小时
                    duration = end_time - start_time
                    duration = duration.seconds / 60    # 持续多少分钟
                    # 根据降雨时间与降雨量判断降雨级别：小雨，中雨，大雨，暴雨，大暴雨，特大暴雨
                    if (duration <= 12 and rain_fall <= 5) or (duration > 12 and rain_fall <= 10):
                        rarank = 1
                    elif (duration <= 12 and rain_fall <= 15) or (duration > 12 and rain_fall <= 25):
                        rarank = 2
                    elif (duration <= 12 and rain_fall <= 30) or (duration > 12 and rain_fall <= 50):
                        rarank = 3
                    elif rain_fall <= 100:
                        rarank = 4
                    elif rain_fall <= 250:
                        rarank = 5
                    else: rarank = 6
                    if value.S_DIST == '<Null>':
                        s_dist = '无'
                    else: s_dist = value.S_DIST
                    rain_data.loc[rain_data_index] = [value.S_STATIONID, start_time, end_time,
                                                      duration, rain_fall, rarank, s_dist]
                    log, rain_fall, rarank, rain_time= 0, 0, 0, 0
                    rain_data_index += 1

    rain_data.to_csv('../data/data/rain_data.csv', encoding='gbk', index=False)
    return rain_data

'''统计降雨信息，并将统计数据插入observe_abute属性表'''
def observe_abute_table(rain_data,observe_abute):
    # 统计降雨总量
    rainTimeFall = rain_data[['S_STATIONID', 'DURATION', 'N_RAINVALUE']]
    
    # 统计 降雨总量，降雨总时长
    rainTimeFall_sum = rainTimeFall.groupby('S_STATIONID').sum()
    
    observe_index = rainTimeFall_sum.index
    observe_frq = list(rain_data['S_STATIONID'])
    
    observe_abute[['RAINFALL', 'DURATION', 'FREQU']] = ''
    for index in observe_index:
        observe_abute.loc[index,'RAINFALL'] =rainTimeFall_sum.loc[index,'N_RAINVALUE']
        observe_abute.loc[index, 'DURATION'] = rainTimeFall_sum.loc[index, 'DURATION']
        observe_abute.loc[index, 'FREQU'] = observe_frq.count(index)
    
    observe_abute.to_csv('../data/data/observe_abute.csv', encoding='gbk')
    
'''统计地区降雨信息：降雨量，降雨时长，降雨次数'''
def region_rain(rain_data, observe_abute):
    
    # 创建地区表，维护地区性降雨时长，降雨rank（求最大）
    all_dict = set(rain_data['S_DIST'])
    region_rain = pd.DataFrame(columns=['S_DIST', 'START_TIME', 'END_TIME', 'DURATION', 'MAXRANK', 'DI_OB', 'RA_OB'])
    region_rain_index = 0
    
    for s_dict in all_dict:
        # 该地区的所有降雨按时间排序
        rainFoTime = rain_data[rain_data['S_DIST'] == s_dict].sort_values('START_TIME')
        # 获得该地区的观察点数量
        dict_observe = observe_abute[observe_abute['S_DIST'] == s_dict]['S_STATIONNAME'].count()
        # 根据降雨时间表，统计本次有记录降雨数量的观察站数
        rain_observe = len(set(rainFoTime['S_STATIONID']))
        
        # 依次判断每一行降雨数据
        rain_index =list()      # 用于存储本次地区影响的所有降雨
        log = 0         # 用于跳过第一次初始化的写入，直接转变为1
        for value in rainFoTime.itertuples(index=True):
            if log == 0:    # 初始化
                start_time = value.START_TIME
                end_time = value.END_TIME
                rain_index.append(value.Index)      # 记录当前降雨行为本次降雨
                log += 1
            else:
                the_start = value.START_TIME
                the_end = value.END_TIME
                
                if start_time <= the_start <= end_time:     # 本行降雨依旧在本次降雨内
                    end_time = max(end_time, the_end)
                    rain_index.append(value.Index)
                else:       # 本行降雨不在本次降雨，需将本次降雨存入region_rain，并开始新的降雨
                    # 本次降雨的时长
                    duration = end_time - start_time
                    duration = duration.seconds / 3600
                    # 本次降雨的等级
                    the_rain = rainFoTime.loc[rain_index]
                    maxrank = max(the_rain['RARANK'])
                    
                    region_rain.loc[region_rain_index] = [s_dict, start_time, end_time,
                                                          duration, maxrank, dict_observe, rain_observe]
                    region_rain_index += 1
                    # 开始新的降雨计数
                    start_time = value.START_TIME
                    end_time = value.END_TIME
                    rain_index = []
                    rain_index.append(value.Index)

    region_rain.to_csv('../data/data/region_rain.csv', encoding='gbk', index=False)
    return region_rain



if __name__ == '__main__':
    path = '../data/data/rain_data_test.csv'
    observe_rain_data = pd.read_csv(path, encoding='gbk')
    
    # 对属性缺失值进行中文“无”的填充
    columns = ['S_STATIONNAME', 'S_DIST', 'S_XIANGZHEN']
    for column in columns:
        observe_rain_data[column].fillna(value='无', inplace=True)
    # 时间格式转化
    observe_rain_data['D_TIME'] = pd.to_datetime(observe_rain_data['D_TIME'])
        
    # 减少内存使用
    observe_rain_data = OPTools.otMenory(observe_rain_data)

    # 维护S_STATIONID与观测站属性关联
    observe_abute = observe_rain_data[['S_STATIONID', 'S_STATIONNAME', 'S_DIST', 'S_XIANGZHEN']].copy(deep=True)
    observe_abute.drop_duplicates('S_STATIONID', inplace=True)
    observe_abute.set_index(keys='S_STATIONID', inplace=True)

    rain_data = rain_data(observe_rain_data, observe_abute)     # 记录每一场雨
    observe_abute_table(rain_data,observe_abute)                      # 观察点属性表
    region_rain = region_rain(rain_data, observe_abute)         # 按地区区分降雨
    
