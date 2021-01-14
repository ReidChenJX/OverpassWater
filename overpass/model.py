#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/1/5 16:57
# @Author   : ReidChen

import pandas as pd
import datetime
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
        overpass_abute = pd.read_csv('../data/data/overpass_abute.csv', encoding='gbk', index_col='S_NO')
        original_overpass_data = pd.read_csv('../data/data/2020overpass.csv', encoding='gbk')
        original_rain_data = pd.read_csv('../data/data/2020rain_data.csv', encoding='gbk')
        original_pump_data = pd.read_csv('../data/data/2020pump_his.csv', encoding='gbk')
        rain_time_val = pd.read_csv('../data/data/rain_data.csv', encoding='gbk')
        
        # 根据 S_NO 提取数据，设置self.abute
        self.abute = overpass_abute.loc[self.name].copy(deep=True)
        del overpass_abute
        
        # 处理积水监测数据，并在其中补充降雨与泵站数据
        flood_time = original_overpass_data[original_overpass_data.S_NO == self.name].sort_values('T_SYSTIME')
        flood_tmp = flood_time[self.time <= flood_time.T_SYSTIME]
        flood_data = flood_tmp[flood_tmp.T_SYSTIME <= self.end].copy(deep=True)
        flood_data.reset_index(inplace=True)
        del original_overpass_data
        
        # 处理降雨数据，获取特定时间段的监测地点的监测数据
        rain_time = original_rain_data[original_rain_data.S_STATIONID == self.abute.S_STATIONID].sort_values('D_TIME')
        rain_tmp = rain_time[self.time <= rain_time.D_TIME]
        rain_data = rain_tmp[rain_tmp.D_TIME <= self.end].copy(deep=True)
        # 降雨监测数据，如果小于0为异常值，清除为0
        rain_data['N_RAINVALUE'] = rain_data['N_RAINVALUE'].apply(lambda x: x if x >= 0 else 0)
        rain_data.reset_index(inplace=True)
        del original_rain_data
        
        # 处理泵站数据，获取特定时间，特定监测点的泵站数据
        pump_time = original_pump_data[original_pump_data.S_STID == self.abute.S_STID].sort_values('T_KBTIME')
        pump_tmp = pump_time[self.time <= pump_time.T_KBTIME]
        pump_data = pump_tmp[pump_tmp.T_KBTIME <= self.end].copy(deep=True)
        pump_data.reset_index(inplace=True)
        del original_pump_data
        
        # 处理降雨时间表
        rain_time = rain_time_val[rain_time_val.S_STATIONID == self.abute.S_STATIONID].sort_values('START_TIME')
        rain_tmp = rain_time[self.time <= rain_time.START_TIME]
        rain_time = rain_tmp[rain_tmp.START_TIME <= self.end].copy(deep=True)
        rain_time.reset_index(inplace=True)
        del rain_time_val
        
        return flood_data, rain_data, pump_data, rain_time
    
    def transform(self):
        # 将积水，降雨，泵站情况合并到一张表中
        flood_data, rain_data, pump_data, rain_time = self._get_data()
        
        transform = flood_data[['S_NO', 'T_SYSTIME', 'N_VALUE']].copy(deep=True)
        transform['T_SYSTIME'] = pd.to_datetime(transform['T_SYSTIME'])
        
        rain_data = rain_data[['D_TIME', 'N_RAINVALUE']]
        rain_data['D_TIME'] = pd.to_datetime(rain_data['D_TIME'])
        
        pump_data = pump_data[['S_BJBH', 'T_KBTIME', 'T_TBTIME', 'N_KBSW', 'N_TBSW']]
        pump_data['T_KBTIME'] = pd.to_datetime(pump_data['T_KBTIME'])
        pump_data['T_TBTIME'] = pd.to_datetime(pump_data['T_TBTIME'])
        pump_data['pump_inter'] = pump_data['T_KBTIME'] - pump_data['T_TBTIME']
        pump_data['pump_inter'] = pump_data['pump_inter'].apply(lambda x: x.seconds / 60)
        
        
        rain_time['START_TIME'] = pd.to_datetime(rain_time['START_TIME'])
        rain_time['END_TIME'] = pd.to_datetime(rain_time['END_TIME'])
        
        def get_rain(one_flood) -> list:
            # 根据时间，统计前12小时，前6小时，前3小时，前1小时，前5分钟以及此时的降雨数据
            time = one_flood.T_SYSTIME
            time_12 = time + datetime.timedelta(hours=-12)
            time_6 = time + datetime.timedelta(hours=-6)
            time_3 = time + datetime.timedelta(hours=-3)
            time_1 = time + datetime.timedelta(hours=-1)
            
            # 前向寻找到最接近的的降雨记录
            rain_run_intime = rain_data[rain_data.D_TIME <= time]
            if len(rain_run_intime) == 0: return [0, 0, 0, 0, 0, 0]
            rain_tail = rain_run_intime.iloc[-1]
            
            # 当前降雨量
            rain_now = rain_tail.N_RAINVALUE
            # 前一次记录降雨量
            rain_last = rain_run_intime.loc[rain_tail.name - 1, 'N_RAINVALUE']
            # 前一小时降雨量合计
            rain_run_1 = rain_run_intime[rain_run_intime.D_TIME >= time_1]
            rain_1h = sum(rain_run_1.N_RAINVALUE)
            # 前3小时降雨量合计
            rain_run_3 = rain_run_intime[rain_run_intime.D_TIME >= time_3]
            rain_3h = sum(rain_run_3.N_RAINVALUE)
            # 前6小时降雨合计
            rain_run_6 = rain_run_intime[rain_run_intime.D_TIME >= time_6]
            rain_6h = sum(rain_run_6.N_RAINVALUE)
            # 前12小时降雨合计
            rain_run_12 = rain_run_intime[rain_run_intime.D_TIME >= time_12]
            rain_12h = sum(rain_run_12.N_RAINVALUE)
            
            return rain_12h, rain_6h, rain_3h, rain_1h, rain_last, rain_now
        
        def interval(one_flood) -> list:
            # 以六个小时前未停雨为记录标准，六小时内的雨均会照成影响
            now_time = one_flood.T_SYSTIME
            time_pro6 = now_time + datetime.timedelta(hours=-6)
            # 以停雨时间为判断标准，前六小时内未停雨的
            flood_raTime = rain_time[rain_time['END_TIME'] >= time_pro6]
            # 排除之后再降雨的数据
            flood_raTime = flood_raTime[flood_raTime['START_TIME'] <= now_time]
            
            if len(flood_raTime) == 0:
                # 没有降雨记录
                start_inter, end_inter = -1, -1
            else:
                start_inter = (flood_raTime.iloc[0].START_TIME - now_time).seconds / 60  # 开始到当前时间间隔
                end_inter = (flood_raTime.iloc[-1].END_TIME - now_time).seconds / 60  # 结束到当前时间间隔
                if flood_raTime.iloc[-1].END_TIME > now_time: end_inter = -1
            
            return start_inter, end_inter
        
        def get_pump(one_flood) -> list:
            # 根据时间，寻找当时最贴近的泵站运行数据
            time = one_flood.T_SYSTIME
            time_12 = time + datetime.timedelta(hours=-12)
            time_6 = time + datetime.timedelta(hours=-6)
            time_3 = time + datetime.timedelta(hours=-3)
            time_1 = time + datetime.timedelta(hours=-1)
            
            # 根据time确定开关泵时间包含time的数据
            pump_run_in_time = pump_data[pump_data.T_KBTIME <= time]
            pump_run_in_time = pump_run_in_time[pump_run_in_time.T_TBTIME >= time]
            
            # 前12h之内泵站的运行数据
            pump_run_in_12h = pump_data[pump_data.T_KBTIME <= time]
            pump_run_in_12h = pump_run_in_12h[pump_run_in_12h.T_TBTIME >= time_12]
            pump_dur_12 = sum(pump_run_in_12h['pump_inter'])
            
            # 前6h之内泵站的运行数据
            pump_run_in_6h = pump_data[pump_data.T_KBTIME <= time]
            pump_run_in_6h = pump_run_in_6h[pump_run_in_6h.T_TBTIME >= time_6]
            pump_dur_6 = sum(pump_run_in_6h['pump_inter'])
            # 前3h之内的泵站运行数据
            pump_run_in_3h = pump_data[pump_data.T_KBTIME <= time]
            pump_run_in_3h = pump_run_in_3h[pump_run_in_3h.T_TBTIME >= time_3]
            pump_dur_3 = sum(pump_run_in_3h['pump_inter'])
            # 前1h之内的泵站运行数据
            pump_run_in_1h = pump_data[pump_data.T_KBTIME <= time]
            pump_run_in_1h = pump_run_in_1h[pump_run_in_1h.T_TBTIME >= time_1]
            pump_dur_1 = sum(pump_run_in_1h['pump_inter'])
            
            
            
            if len(pump_run_in_time) == 0:  # 判断此时有多少泵站运行数据，若为0则无泵
                pump_zt, pump_num, pump_kbbh = 0, 0, 0
            elif len(pump_run_in_time) == 1:  # 单泵运行
                pump_zt = 1
                pump_num = 1
                _pump_kbbh = pump_run_in_time.S_BJBH.to_list()
                pump_kbbh = ','.join([str(x) for x in _pump_kbbh])
            else:  # 多泵运行
                pump_zt = 1
                pump_num = len(pump_run_in_time)
                _pump_kbbh = pump_run_in_time.S_BJBH.to_list()
                pump_kbbh = ','.join([str(x) for x in _pump_kbbh])
            
            return pump_dur_12, pump_dur_6, pump_dur_3, pump_dur_1, pump_zt, pump_num, pump_kbbh
        
        for one_flood in transform.itertuples():
            # try:
            # 降雨数据录入
            transform.loc[one_flood.Index, ['rain_12h', 'rain_6h', 'rain_3h', 'rain_1h',
                                            'rain_last', 'rain_now']] = get_rain(one_flood)
            # 时间间隔数据录入
            transform.loc[one_flood.Index, ['start_inter', 'end_inter']] = interval(one_flood)
            # 泵站数据录入
            transform.loc[one_flood.Index, ['pump_dur_12', 'pump_dur_6', 'pump_dur_3', 'pump_dur_1',
                                            'kbzt', 'kbnum', 'kbbh']] = get_pump(one_flood)
            if one_flood.Index == 0: val_last = 0
            else: val_last = transform.loc[one_flood.Index-1, 'N_VALUE']
            # 前一下立交积水点的监测值数据
            transform.loc[one_flood.Index,'val_last'] = val_last
                
            # except:
            #     print('Mistake issues:')
            #     print(get_rain(one_flood))
            #     print(get_pump(one_flood))
        
        # 积水数据中，
        self.data = transform
        self.size = transform.shape

def main():
    
    s_no = 2015060020
    model_data = ModelData(s_no, start_time='2020-01-01 00:00:00', end_time='2020-12-31 00:00:00')
    model_data.transform()
    model_data.data.to_csv(model_data.data_path, encoding='gbk', index=False)


if __name__ == '__main__':
    main()