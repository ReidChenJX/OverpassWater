#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2020/12/29
# @Author   : ReidChen

import pandas as pd



'''从 overpass_abute 获取所有下立交积水点对应的S_STATIONID，在observe_abute验证是否存在'''
def observe_neg():
    
    # 加载下立交属性表，获取对应降雨观察点的 STATIONID
    overpass_abute = pd.read_csv('../data/data/overpass_abute.csv', encoding='gbk')
    # 加载降雨属性表，从其中进行匹配查找
    observe_abute = pd.read_csv('../data/data/observe_abute.csv',encoding='gbk')
    observe_stainId = observe_abute['S_STATIONID'].values
    
    # 构建 observe_neg，存放异常的观察点
    observe_neg = pd.DataFrame(columns=['S_NO', 'S_ADDR', 'S_STATIONID', 'S_STATIONNAME'])
    index = 0
    stationIF = overpass_abute[['S_NO', 'S_ADDR', 'S_STATIONID', 'S_STATIONNAME']]
    
    for id in stationIF.itertuples(index=True):
        if id.S_STATIONID not in observe_stainId:
            observe_neg.loc[index, 'S_NO'] = stationIF.loc[id.Index, 'S_NO']
            observe_neg.loc[index, 'S_ADDR'] = stationIF.loc[id.Index, 'S_ADDR']
            observe_neg.loc[index, 'S_STATIONID'] = stationIF.loc[id.Index, 'S_STATIONID']
            observe_neg.loc[index, 'S_STATIONNAME'] = stationIF.loc[id.Index, 'S_STATIONNAME']
            index += 1
            
    observe_neg.to_csv('../data/data/observe_neg.csv', index=False, encoding='gbk')

observe_neg = observe_neg()
