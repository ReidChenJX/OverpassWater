# -*- coding:utf-8 -*-

import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号


# 降雨数据，包括地区和降雨量
rain_data = pd.read_csv('./data/observe_abute.csv',encoding='gbk')
rain_data.set_index('S_STATIONID', inplace=True)
# 积水数据，包括下立交点和积水深度
overpass_data = pd.read_csv('./data/overpass_abute.csv',encoding='gbk')
overpass_data.set_index('S_NO', inplace=True)
# 每一次积水对应的降雨情况
rain_flood = pd.read_csv('./data/rain_flood.csv',encoding='gbk')

'''任务一：绘制地区--总降雨    地区--降雨频率   地区--降雨量'''

rain_data_dra = rain_data[['S_DIST','RAINFALL','DURATION','FREQU']].copy()
rain_data_dra.dropna(inplace=True)
rain_data_dra = rain_data_dra.groupby('S_DIST').mean()
rain_data_dra.sort_values('RAINFALL', inplace=True,ascending=False)

'''任务二：地区--积水次数  地区--平均积水深度  地区--最大积水深度'''
# 找出下立交所对应点地区
overpass_data['S_DIST'] = ''
# 排除无积水次数的积水点
# overpass_data = overpass_data[overpass_data['FREQU'] > 0 ]
# 将积水点对应的地区查找出来

for s_no in overpass_data.index:
    if overpass_data.loc[s_no, 'FREQU'] == np.nan:
        continue
    # 获取对应的STATIONID
    S_STATIONID = overpass_data.loc[s_no, 'S_STATIONID']
    # 获取STATIONID对应的S_DIST
    if S_STATIONID not in rain_data.index:
        continue
    S_DIST = rain_data.loc[S_STATIONID,'S_DIST']
    overpass_data.loc[s_no, 'S_DIST'] = S_DIST

overpass_data.to_csv('./data/overpass_abute.csv',encoding='gbk')
