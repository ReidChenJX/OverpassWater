#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2020/12/29
# @Author   : ReidChen

import pandas as pd
import numpy as np

rain_flood = pd.read_csv('./data/hydrops_data.csv', encoding='gbk')
overpass_abute = pd.read_csv('./data/overpass_abute.csv', encoding='gbk')
rain_data = pd.read_csv('./data/rain_data.csv', encoding='gbk')

snoTostation = overpass_abute[['S_NO', 'S_STATIONID']].copy(deep=True)
del overpass_abute

'''rain_flood: 一次积水，对应降雨观测点最符合的降雨数据，并判断是雨中积水还是雨后积水'''
# 建立S_NO与S_STATIONID的联系
snoTostation.set_index('S_NO',inplace=True)

rain_flood_index = rain_flood['S_NO'].values

rain_flood[['S_STATIONID','START_RAIN','END_RAIN','DURATION','N_RAINVALUE','RANK', 'DEFINE']] = ''

# 在rain_flood中，每一行数据都去匹配最佳的降雨数据
rain_flood_len = len(rain_flood)

for i in range(rain_flood_len):
    # 第 i 行的s_no
    rain_flood_sno = rain_flood.loc[i,'S_NO']
    # 对应的观察点ID
    stationID = snoTostation.loc[rain_flood_sno,'S_STATIONID']
    # 该观察点所有的降雨情况
    one_allRain = rain_data[rain_data['S_STATIONID'] == stationID]

    for one_rain in one_allRain.itertuples(index=True):
        # rain_flood 第 i 行的开始时间在one_rain的时间范围内，即可
        if one_rain.START_TIME <=rain_flood.loc[i,'START_TIME'] <= one_rain.END_TIME :
            # 满足 雨中积水 情况，开始赋值
            rain_flood.loc[i, 'S_STATIONID'] = stationID
            rain_flood.loc[i, 'START_RAIN'] = one_rain.START_TIME
            rain_flood.loc[i, 'END_RAIN'] = one_rain.END_TIME
            rain_flood.loc[i, 'DURATION'] = one_rain.DURATION
            rain_flood.loc[i, 'N_RAINVALUE'] = one_rain.N_RAINVALUE
            rain_flood.loc[i, 'RANK'] = one_rain.RANK
            rain_flood.loc[i, 'DEFINE'] = '雨中积水'
            
            # 赋值结束，跳出本次循环，继续上级循环
            break
        #
        elif (one_rain.END_TIME <= rain_flood.loc[i,'START_TIME'])  \
                and ( (one_rain.Index == max(one_allRain.index)) or rain_flood.loc[i,'START_TIME'] <= one_allRain.loc[one_rain.Index+1,'START_TIME']):
            # 下雨过后，产生积水
            rain_flood.loc[i, 'S_STATIONID'] = stationID
            rain_flood.loc[i, 'START_RAIN'] = one_rain.START_TIME
            rain_flood.loc[i, 'END_RAIN'] = one_rain.END_TIME
            rain_flood.loc[i, 'DURATION'] = one_rain.DURATION
            rain_flood.loc[i, 'N_RAINVALUE'] = one_rain.N_RAINVALUE
            rain_flood.loc[i, 'RANK'] = one_rain.RANK
            rain_flood.loc[i, 'DEFINE'] = '停雨积水'
        
rain_flood.to_csv('./data/rain_flood.csv',index=False,encoding='gbk')
