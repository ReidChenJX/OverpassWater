#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2020/12/29
# @Author   : ReidChen

import pandas as pd
import numpy as np
import sys,os
file_dir = os.path.split(os.path.realpath(__file__))[0]
sys.path.append(file_dir)
import OPTools

"""
数据来源：2020overpass.csv，为数据库获取表
程序返回：overpass_abute，包含各下立交点属性内容
结果：异常积水点 overpass_neg，积水时间与深度 hydrops_data，积水属性与频次 overpass_table
"""

'''查找数据中的异常值，并记录在 overpass_neg 文件中'''


def anomaly_data(overpass_abute):
    # 查出异常积水点：数值为负，离线状态，无地址，无对应降雨观察点
    overpass_neg = pd.DataFrame(columns=['S_NO', 'NEG_VALUE', 'LINE_Off', 'NO_ADDR', 'NO_RAIN'])
    
    S_NO, NEG_VALUE, LINE_Off, NO_ADDR, NO_RAIN = list(), list(), list(), list(), list()
    
    for row in overpass_abute.itertuples(index=True):
        if row.N_VALUE < 0 or row.S_STATENAME != '正常' or row.S_ADDR == '无' or row.S_STATIONID == np.nan:
            # 若为异常情况，需要记录到overpass_neg内
            if row.Index not in S_NO:
                S_NO.append(row.Index)
                if row.N_VALUE < 0:
                    NEG_VALUE.append(row.N_VALUE)
                else:
                    NEG_VALUE.append('正值')
                # 若离线则设置为1
                if row.S_STATENAME != '正常':
                    LINE_Off.append(row.S_STATENAME)
                else:
                    LINE_Off.append('正常')
                # 若无地址，设置为1
                if row.S_ADDR == '无':
                    NO_ADDR.append('无')
                else:
                    NO_ADDR.append(row.S_ADDR)
                # 若无降雨站，设置为1
                if row.S_STATIONID == np.nan:
                    NO_RAIN.append('无')
                else:
                    NO_RAIN.append(row.S_STATIONID)
        # 若为正常情况，继续循环
        else:
            continue
    # 将数据整合至overpass_neg
    overpass_neg['S_NO'] = S_NO
    overpass_neg['NEG_VALUE'] = NEG_VALUE
    overpass_neg['LINE_Off'] = LINE_Off
    overpass_neg['NO_ADDR'] = NO_ADDR
    overpass_neg['NO_RAIN'] = NO_RAIN
    
    # 将数据写入csv文件
    neg_path = '../data/data/overpass_neg.csv'
    overpass_neg.to_csv(neg_path, index=False, encoding='gbk')
    
    return overpass_neg


'''排除异常点后的下立交观察点列表'''


def overpassPosData(all_index, neg_index):
    # 排除异常点后的下立交观察点
    neg_index = neg_index
    all_index = all_index
    pos_index = list()
    
    for XLJ in all_index:
        if XLJ not in neg_index:
            pos_index.append(XLJ)
    return pos_index


'''根据可用的S_NO，推算积水开始时间，积水结束时间，积水深度，积水级别，并记录在 hydrops_data 文件中'''


def hydrops_data(pos_index):
    # 积水级别：（10-15）一级，（15-25）二级，（25-50）三级，（50--）四级
    hydrops_data = pd.DataFrame(columns=['S_NO', 'START_TIME', 'END_TIME', 'DEEP', 'JSRANK'])
    hydrops_data_index = 0
    
    for JSD_NO in pos_index:
        # 根据S_NO获取积水点的数据
        JSD_value = overpass_data[overpass_data['S_NO'] == JSD_NO][['S_NO', 'T_SYSTIME', 'N_VALUE']]
        JSD_value.sort_values('T_SYSTIME', inplace=True)  # 按照时间排序
        
        # 依次读取监测数据，记录积水开始时间，结束时间，积水深度
        log = 0  # 标志位，1代表正在积水
        start_time = '2010-12-22 00:00:00'
        end_time = '2010-12-22 00:00:00'
        water_deep = 0.0
        jsrank = 0
        
        # 获取每一段监测数值，判断当前积水情况，并写入hydrops_data
        for value in JSD_value.itertuples(index=False):
            if value.N_VALUE >= 10 and log == 1:  # 正在积水并还在积水
                water_deep = max(water_deep, value.N_VALUE)
            if value.N_VALUE < 10 and log == 1:  # 正在积水但在此时退出积水
                if start_time == '2010-12-22 00:00:00': start_time = value.T_SYSTIME
                end_time = value.T_SYSTIME
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
                start_time = value.T_SYSTIME
                log = 1
                water_deep = max(water_deep, value.N_VALUE)
            if value.N_VALUE < 10 and log == 0: continue  # 不积水
    
    hydrops_data.to_csv('../data/data/hydrops_data.csv', encoding='gbk', index=False)
    return hydrops_data


'''向 overpass_abute 属性表插入积水次数，平均积水深度，最大积水深度'''


def overpass_table(hydrops_data):
    # 计算积水次数，平均深度，最大深度，并合并到属性表中
    hydrops_deep = hydrops_data[['S_NO', 'DEEP']]
    # 统计 积水平均深度
    deep_mean = hydrops_deep.groupby('S_NO').mean()
    # 统计 积水最大深度
    deep_max = hydrops_deep.groupby('S_NO').max()
    
    # 索引 积水点的S_NO
    deep_SNO = hydrops_deep['S_NO']
    deep_SNO_list = list(deep_SNO)
    overpass_abute[['MEAN_DEEP', 'MAX_DEEP', 'FREQU']] = ''
    for index in deep_SNO:
        # 记录平均深度
        overpass_abute.loc[index, 'MEAN_DEEP'] = deep_mean.loc[index, 'DEEP']
        overpass_abute.loc[index, 'MAX_DEEP'] = deep_max.loc[index, 'DEEP']
        overpass_abute.loc[index, 'FREQU'] = deep_SNO_list.count(index)
    
    return overpass_abute


'''向 overpass_abute 属性表中，插入对应雨量观察站的位置信息，并生成overpass_abute文件'''


def dict_overpass_table(overpass_abute):
    overpass_abute['S_DIST'] = ''
    # 从降雨观察站属性表中获取位置信息
    rain_data = pd.read_csv('../data/data/observe_abute.csv', encoding='gbk')
    rain_data.set_index('S_STATIONID', inplace=True)
    
    for s_no in overpass_abute.index:
        
        # 获取对应的STATIONID
        S_STATIONID = overpass_abute.loc[s_no, 'S_STATIONID']
        # 获取STATIONID对应的S_DIST
        if S_STATIONID not in rain_data.index:
            continue
        S_DIST = rain_data.loc[S_STATIONID, 'S_DIST']
        overpass_abute.loc[s_no, 'S_DIST'] = S_DIST
    
    # 删除数值列。
    overpass_abute.drop(columns=['N_VALUE'], inplace=True)
    overpass_abute.to_csv('../data/data./overpass_abute.csv', encoding='gbk',)



if __name__ == '__main__':
    path = '../data/data/2020overpass.csv'
    overpass_data = pd.read_csv(path, encoding='gbk')
    
    # 对属性缺失值进行中文“无”的填充
    columns = ['S_ADDR', 'S_BUILDDATE', 'S_PROUNIT', 'S_MANAGE_UNIT', 'S_MAINTAIN_UNIT', 'S_STATIONNAME']
    for column in columns:
        overpass_data[column].fillna(value='无', inplace=True)
    # 时间格式转化
    overpass_data['T_SYSTIME'] = pd.to_datetime(overpass_data['T_SYSTIME'])
    
    # 减少内存使用
    overpass_data = OPTools.otMenory(overpass_data)
    
    # 维护S_NO与下立交属性关联
    overpass_abute = overpass_data[['S_NO', 'S_HASMONITOR', 'N_VALUE', 'S_STATENAME', 'S_ADDR',
                                      'S_BUILDDATE', 'S_PROUNIT', 'S_MANAGE_UNIT',
                                      'S_MAINTAIN_UNIT', 'S_STATIONID', 'S_STATIONNAME']].copy(deep=True)
    overpass_abute.drop_duplicates('S_NO', inplace=True)
    overpass_abute.set_index(keys='S_NO', inplace=True)
    
    overpass_neg = anomaly_data(overpass_abute)  # 异常值数据
    neg_index = list(overpass_neg['S_NO'])  # 异常积水点
    all_index = overpass_abute.index  # 所有观察点的S_NO
    pos_index = overpassPosData(all_index, neg_index)  # 正常可分析积水点
    hydrops_data = hydrops_data(pos_index)  # 根据可用S_NO,获取积水数据(时间，深度，级别）
    overpass_abute = overpass_table(hydrops_data)  # 整理最初的属性表，包括积水次数，平均深度，最大深度
    overpass_abute = dict_overpass_table(overpass_abute)  # 向overpass_abute中插入降雨观察站的位置信息
