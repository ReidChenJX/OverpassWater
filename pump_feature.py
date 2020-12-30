#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2020/12/30 16:56
# @Author   : ReidChen

import pandas as pd
import numpy as np


'''JISHUI表中含有大量无积水监测的积水点'''
def selNegSno(S_P_List):
    # 降雨与积水,下立交S_NO来源于积水监测数据。
    overpass_abute = pd.read_csv('./data/overpass_abute.csv', encoding='gbk')
    snoFrAbute = overpass_abute['S_NO'].values
    # S_P_List中有数据库所有的S_NO
    snoFrPump = S_P_List['S_NO'].values
    sNotList = []
    for sno in snoFrPump:
        if sno not in snoFrAbute:
            sNotList.append(sno)
    
    del overpass_abute
    OpNotInJiShui = S_P_List.set_index('S_NO')
    OpNotInJiShui = OpNotInJiShui.loc[sNotList]
    OpNotInJiShui.to_csv('./data/OpNotInJiShui.csv', index=True, encoding='gbk')

'''积水与泵张开启时间对应关系'''
# 积水点的积水时序
hydrops_data = pd.read_csv('./data/hydrops_data.csv', encoding='gbk')




if __name__ == '__main__':
    
    pump_run_data = pd.read_csv('./data/PumpHis.csv', encoding='gbk')
    S_P_List = pd.read_csv('./data/SnoPump.csv', encoding='gbk')
    
    selNegSno(S_P_List)




