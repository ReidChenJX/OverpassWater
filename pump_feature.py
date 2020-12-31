#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2020/12/30 16:56
# @Author   : ReidChen

import pandas as pd
import numpy as np

'''JISHUI表中含有大量无积水监测的积水点'''


def selNegSno(S_P_List, overpass_abute):
    # 降雨与积水,下立交S_NO来源于积水监测数据。
    snoFrAbute = overpass_abute['S_NO'].values
    # S_P_List中有数据库所有的S_NO
    snoFrPump = S_P_List['S_NO'].values
    sNotList = []
    for sno in snoFrPump:
        if sno not in snoFrAbute:
            sNotList.append(sno)
    
    OpNotInJiShui = S_P_List.set_index('S_NO')
    OpNotInJiShui = OpNotInJiShui.loc[sNotList]
    OpNotInJiShui.to_csv('./data/OpNotInJiShui.csv', index=True, encoding='gbk')


'''积水与泵站个数，泵站开启时长的统计'''


def PumpDF(S_P_List, overpass_abute, pump_run_data):
    # 在 overpass_abute 中维护泵站个数，泵站排量
    oaData = overpass_abute.set_index('S_NO')
    SPData = S_P_List.drop_duplicates('S_NO')
    SPData = SPData.set_index('S_NO')
    
    for oa in oaData.itertuples(index=True):
        if oa.Index not in SPData.index:
            continue
        s_xtbm = SPData.loc[oa.Index, 'S_XTBM']  # 获取系统编码
        oaData.loc[oa.Index, 'S_XTBM'] = s_xtbm
        oaData.loc[oa.Index, 'S_PUMPS_TYPE'] = SPData.loc[oa.Index, 'S_PUMPS_TYPE']
        oaData.loc[oa.Index, 'N_PUMPS_NUM'] = SPData.loc[oa.Index, 'N_PUMPS_NUM']
        oaData.loc[oa.Index, 'N_PUMPS_FLOW'] = SPData.loc[oa.Index, 'N_PUMPS_FLOW']
        s_stid = SPData.loc[oa.Index, 'S_STID']
        oaData.loc[oa.Index, 'S_STID'] = s_stid
        
        # 统计每个泵的开泵时长
        pump_rdata = pump_run_data[pump_run_data['S_STID'] == s_stid]
        duration = pd.Timedelta(0, unit='seconds')
        for rdata in pump_rdata.itertuples():
            duration += rdata.T_TBTIME - rdata.T_KBTIME
        oaData.loc[oa.Index, 'DURATION'] = round(duration.seconds / 3600, 2)
    
    oaData.to_csv('./data/overpass_abute.csv', encoding='gbk')
    return oaData


'''向rain_flood 表中插入泵站开停情况：开启时间，停止时间，开启个数'''


def rain_flood_pump(rain_flood, oaData, pump_run_data):
    # 获取一段 rain_flood 数据，S_NO 对应的S_STID ，S_STID的pump_run_data，在S_STID的pump_run_data中寻找合适的范围
    for ra_fo in rain_flood.itertuples(index=True):
        s_stid = oaData.loc[str(ra_fo.S_NO), 'S_STID']
        pump_run = pump_run_data[pump_run_data['S_STID'] == s_stid].sort_values('T_KBTIME')
        
        pump_start, pump_end = pd.Timedelta(0, unit='seconds'), pd.Timedelta(0, unit='seconds')
        pump_ksw, pump_tsw = 0, 0
        
        for p_run in pump_run.itertuples(index=True):  # 某一个泵所有的时序
            # 积水时间前，最近的开泵；积水时间之后，最近的停泵。
            # ra_fo.START_TIME 积水开  ra_fo.END_TIME 积水停
            if p_run.T_KBTIME <= ra_fo.START_TIME and (np.array(p_run.Index) == pump_run.tail(1).index.values
                                                       or pump_run.loc[p_run.Index + 1, 'T_KBTIME'] > ra_fo.START_TIME):
                pump_start = p_run.T_KBTIME
                pump_ksw = p_run.N_KBSW
            if p_run.T_TBTIME > pump_start and ra_fo.END_TIME <= p_run.T_TBTIME:
                pump_end = p_run.T_TBTIME
                pump_tsw = p_run.N_TBSW
            else:
                pass
        rain_flood.loc[ra_fo.Index, ['T_KBTIME', 'T_TBTIME', 'N_KBSW', 'N_TBSW']] = [pump_start, pump_end, pump_ksw,
                                                                                     pump_tsw]


if __name__ == '__main__':
    # pump_run_data 泵站开停时间表。
    pump_run_data = pd.read_csv('./data/PumpHis.csv', encoding='gbk')
    pump_run_data['T_KBTIME'] = pd.to_datetime(pump_run_data['T_KBTIME'])
    pump_run_data['T_TBTIME'] = pd.to_datetime(pump_run_data['T_TBTIME'])
    # 泵站与积水点对应表，包含相关属性。
    S_P_List = pd.read_csv('./data/SnoPump.csv', encoding='gbk')
    # 积水点属性表。
    overpass_abute = pd.read_csv('./data/overpass_abute.csv', encoding='gbk')
    overpass_abute.drop_duplicates()
    # 积水——降雨，时间序列表。
    rain_flood = pd.read_csv('./data/rain_flood.csv', encoding='gbk')
    rain_flood['START_TIME'] = pd.to_datetime(rain_flood['START_TIME'])
    rain_flood['END_TIME'] = pd.to_datetime(rain_flood['END_TIME'])
    # 将无水位监测但在JISHUI表中存在的积水点存档。
    '''selNegSno(S_P_List)'''
    # 处理泵站属性，合并到积水属性表中：overpass_abute
    oaData = PumpDF(S_P_List, overpass_abute, pump_run_data)
    # ra_fo_pm = rain_flood_pump(rain_flood, oaData, pump_run_data)
    
    for ra_fo in rain_flood.itertuples(index=True):
        s_stid = oaData.loc[str(ra_fo.S_NO), 'S_STID']
        pump_run = pump_run_data[pump_run_data['S_STID'] == s_stid].sort_values('T_KBTIME')
        pump_run.reset_index(inplace=True)
        
        pump_start, pump_end = pd.Timestamp(0, unit='s'), pd.Timestamp(0, unit='s')
        pump_ksw, pump_tsw = 0, 0
        
        for p_run in pump_run.itertuples(index=True):  # 某一个泵所有的时序
            # 积水时间前，最近的开泵；积水时间之后，最近的停泵。
            # ra_fo.START_TIME 积水开  ra_fo.END_TIME 积水停
            if p_run.T_KBTIME <= ra_fo.START_TIME and (np.array(p_run.Index) == pump_run.tail(1).index.values
                                                       or pump_run.loc[p_run.Index + 1, 'T_KBTIME'] > ra_fo.START_TIME):
                pump_start = p_run.T_KBTIME
                pump_ksw = p_run.N_KBSW
            if p_run.T_TBTIME > pump_start and ra_fo.END_TIME <= p_run.T_TBTIME:
                pump_end = p_run.T_TBTIME
                pump_tsw = p_run.N_TBSW
            else:
                pass
        
        rain_flood.loc[ra_fo.Index, ['T_KBTIME', 'T_TBTIME', 'N_KBSW', 'N_TBSW']] = [pump_start, pump_end, pump_ksw,
                                                                                     pump_tsw]
