#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/1/13 13:40
# @Author   : ReidChen
# Document  ：Get the data from June to October.

import pandas as pd

overpass_2020 = pd.read_csv('../data/data/2020overpass.csv',encoding='gbk')
rain2020 = pd.read_csv('../data/data/2020rain_data.csv',encoding='gbk')
pump_2020 = pd.read_csv('../data/data/2020pump_his.csv',encoding='gbk')

overpass_2020['T_SYSTIME'] = pd.to_datetime(overpass_2020['T_SYSTIME'])
rain2020['D_TIME'] = pd.to_datetime(rain2020['D_TIME'])
pump_2020['T_KBTIME'] = pd.to_datetime(pump_2020['T_KBTIME'])

start_data = pd.to_datetime('2020-06-01')
end_data = pd.to_datetime('2020-09-01')


overpass_2020_6 = overpass_2020[overpass_2020.T_SYSTIME >= start_data]
overpass_2020_6 = overpass_2020_6[overpass_2020_6.T_SYSTIME < end_data]
overpass_2020_6.to_csv('../data/data/2020-06overpass.csv',index=False,encoding='gbk')

rain2020_6 = rain2020[rain2020.D_TIME >= start_data]
rain2020_6 = rain2020_6[rain2020_6.D_TIME < end_data]
rain2020_6.to_csv('../data/data/2020-6rain_data.csv',index=False,encoding='gbk')

pump_2020_6 = pump_2020[pump_2020.T_KBTIME >= start_data]
pump_2020_6 = pump_2020_6[pump_2020_6.T_KBTIME< end_data]
pump_2020_6.to_csv('../data/data/2020-06pump_his.csv',index=False,encoding='gbk')




