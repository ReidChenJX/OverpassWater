#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/1/5 16:57
# @Author   : ReidChen

import pandas as pd
import numpy as np

'''数据类'''


class ModelData:
    def __init__(self, name, start_time, end_time, data_path=None):
        """
        用于提供可进行建模的数据
        param  name: 积水点的S_NO
        param  startTime: 数据的开始时间
        param  endTime: 数据的结束时间
        """
        self.name = name
        
        if data_path is None:
            self.data_path = '../data/model_data/{name}.csv'.format(name=name)
        else:
            self.data_path = data_path
        
        self.time = start_time
        self.end = end_time
        
        self.abute = None
        self.data = None
        self.size = None
    
    def _get_data(self):
        # 按照时间格式获取数据，包括积水监测，降雨监测，泵站运行数据
        overpass_abute = pd.read_csv('../data/data/overpass_abute.csv', encoding='gbk')
        original_overpass_data = pd.read_csv('../data/data/mouth8-9Overpass.csv', encoding='gbk', low_memory=False)
        original_rain_data = pd.read_csv('../data/data/mouth8-9rain_data.csv', encoding='gbk', low_memory=False)
        original_pump_data = pd.read_csv('../data/data/PumpHis.csv', encoding='gbk')
        
        # 根据 S_NO 提取数据，设置self.abute
        self.abute = overpass_abute[overpass_abute.S_NO == self.name].copy(deep=True)
        del overpass_abute
        
        flood_time = original_overpass_data[original_overpass_data.S_NO == self.name].sort_values('T_SYSTIME')
        flood_tmp = flood_time[self.time <= flood_time.T_SYSTIME]
        flood_data = flood_tmp[flood_tmp.T_SYSTIME <= self.end].copy(deep=True)
        del original_overpass_data
        
        rain_time = original_rain_data[original_rain_data.S_STATIONID == self.abute.S_STATIONID].sort_values('D_TIME')
        rain_tmp = rain_time[self.time <= rain_time.D_TIME]
        rain_data = rain_tmp[rain_tmp.D_TIME <= self.end].copy(deep=True)
        del original_rain_data
        
        pump_time = original_pump_data[original_pump_data.S_STID == self.abute.S_STID].sort_values('T_KBTIME')
        pump_tmp = pump_time[self.time <= pump_time.T_KBTIME]
        pump_data = pump_tmp[pump_tmp.T_KBTIME <= self.end].copy(deep=True)
        del original_pump_data
        
        return flood_data, rain_data, pump_data
    
    def transform(self):
        # 将积水，降雨，泵站情况合并到一张表中
        flood_data, rain_data, pump_data = self._get_data()
        
        transform = flood_data[['S_NO', 'T_SYSTIME', 'N_VALUE']].copy(deep=True)
        transform['T_SYSTIME'] = pd.to_datetime(transform['T_SYSTIME'])
        transform.set_index('T_SYSTIME')
        
        rain_data = rain_data[['D_TIME', 'N_RAINVALUE']]
        rain_data['D_TIME'] = pd.to_datetime(rain_data['D_TIME'])
        rain_data.set_index('D_TIME')
        
        pump_data = pump_data[['S_BJBH', 'S_KTBZT', 'T_TBTIME', 'N_KBSW', 'N_TBSW']]
        pump_data['S_KTBZT'] = pd.to_datetime(pump_data['S_KTBZT'])
        pump_data['T_TBTIME'] = pd.to_datetime(pump_data['T_TBTIME'])
        
        rain_index = 0
        rain_length = len(rain_data)
        def get_rain(one_flood) -> list:
            # 根据时间，寻找当时最贴近的降雨数据
            res = 0
            time = one_flood.T.SYSTIME
            station_id = one_flood.S_STATIONID
            # 降雨反馈时间：D_TIME 唯一
            for D_TIME in rain_data.index:
                if time > D_TIME and time < D_TIME[0+1]:
                    res = rain_data.loc[D_TIME]
                rain_index = D_TIME
            
            return res
        
        pump_index = 0
        pump_length = len(pump_data)
        def get_pump(one_flood) -> list:
            # 根据时间，寻找当时最贴近的泵站运行数据
            time = one_flood.T.SYSTIME
            st_id = one_flood.S_STID
            
            res = [0, 0, 0]
            return res
        
        for one_flood in transform.itertuples():
            transform.loc[one_flood.Index, 'N_RAINVALUE'] = get_rain(one_flood)
            transform.loc[one_flood.Index, 'KBZT', 'KBNUM', 'KBBH'] = get_pump(one_flood)


s_no = '2015060020'
model_data = ModelData(s_no, start_time='2020-07-01 09:00:00', end_time='2020-09-01 09:00:00')
flood_data, rain_data, pump_data = model_data._get_data()
