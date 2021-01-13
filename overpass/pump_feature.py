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
    OpNotInJiShui.to_csv('../data/data/OpNotInJiShui.csv', index=True, encoding='gbk')


'''积水与泵站个数，泵站开启时长的统计'''


def PumpDF(S_P_List, overpass_abute, pump_run_data):
    # 在 overpass_abute 中维护泵站个数，泵站排量
    oaData = overpass_abute.set_index('S_NO')
    SPData = S_P_List.drop_duplicates('S_NO')
    SPData = SPData.set_index('S_NO')
    
    for oa in oaData.itertuples(index=True):
        if str(oa.Index) not in SPData.index:
            continue
        s_xtbm = SPData.loc[str(oa.Index), 'S_XTBM']  # 获取系统编码
        oaData.loc[oa.Index, 'S_XTBM'] = s_xtbm
        oaData.loc[oa.Index, 'S_PUMPS_TYPE'] = SPData.loc[str(oa.Index), 'S_PUMPS_TYPE']
        oaData.loc[oa.Index, 'N_PUMPS_NUM'] = SPData.loc[str(oa.Index), 'N_PUMPS_NUM']
        oaData.loc[oa.Index, 'N_PUMPS_FLOW'] = SPData.loc[str(oa.Index), 'N_PUMPS_FLOW']
        s_stid = SPData.loc[str(oa.Index), 'S_STID']
        oaData.loc[oa.Index, 'S_STID'] = s_stid
        
        # 统计每个泵的开泵时长
        pump_rdata = pump_run_data[pump_run_data['S_STID'] == s_stid]
        duration = pd.Timedelta(0, unit='seconds')
        for rdata in pump_rdata.itertuples():
            duration += rdata.T_TBTIME - rdata.T_KBTIME
        oaData.loc[oa.Index, 'DURATION'] = round(duration.seconds / 3600, 2)
    
    oaData.to_csv('../data/data/overpass_abute.csv', encoding='gbk')
    return oaData


'''向rain_flood 表中插入泵站开停情况：开启时间，停止时间，开启个数'''


def rain_flood_pump(rain_flood, oaData, pump_run_data):
    # 获取一段 rain_flood 数据，S_NO 对应的S_STID ，S_STID的pump_run_data，在S_STID的pump_run_data中寻找合适的范围
    for ra_fo in rain_flood.itertuples(index=True):
        # 初始化泵站开启结束时间
        pump_start, pump_end = np.nan, np.nan
        # 获取积水点的泵站运行ID
        s_stid = oaData.loc[ra_fo.S_NO, 'S_STID']
        # 获取该泵站的运行数据，并按照时间排序
        pump_run = pump_run_data[pump_run_data['S_STID'] == s_stid].sort_values('T_KBTIME')
        pump_run.reset_index(inplace=True)
        if len(pump_run) == 0:
            pump_start, pump_end, pump_ksw, pump_tsw = np.nan, np.nan, np.nan, np.nan
        else:
            # 比较得到开泵时间
            pump_run_stime = pump_run.T_KBTIME
            pump_start = max(pump_run_stime[pump_run_stime <= ra_fo.START_TIME])
            pump_ksw = pump_run[pump_run['T_KBTIME'] == pump_start]['N_KBSW'].values
            # 停泵时间
            pump_run_end = pump_run.T_TBTIME
            pump_end = min(pump_run_end[pump_run_end >= ra_fo.END_TIME])
            pump_tsw = pump_run[pump_run['T_TBTIME'] == pump_end]['N_TBSW'].values
        rain_flood.loc[ra_fo.Index, 'T_KBTIME'] = pump_start
        rain_flood.loc[ra_fo.Index, 'T_TBTIME'] = pump_end
        rain_flood.loc[ra_fo.Index, 'N_KBSW'] = pump_ksw
        # rain_flood.loc[ra_fo.Index, 'N_TBSW'] = pump_tsw
    rain_flood.to_csv('../data/data/rain_flood.csv', encoding='gbk', index=False)
    

if __name__ == '__main__':
    # pump_run_data 泵站开停时间表。
    pump_run_data = pd.read_csv('../data/data/2020pump_his.csv', encoding='gbk')
    pump_run_data['T_KBTIME'] = pd.to_datetime(pump_run_data['T_KBTIME'])
    pump_run_data['T_TBTIME'] = pd.to_datetime(pump_run_data['T_TBTIME'])
    # 泵站与积水点对应表，包含相关属性。
    S_P_List = pd.read_csv('../data/data/SnoPump.csv', encoding='gbk')
    # 积水点属性表。
    overpass_abute = pd.read_csv('../data/data/overpass_abute.csv', encoding='gbk')
    overpass_abute.drop_duplicates()
    # 积水——降雨，时间序列表。
    rain_flood = pd.read_csv('../data/data/rain_flood.csv', encoding='gbk')
    rain_flood['START_TIME'] = pd.to_datetime(rain_flood['START_TIME'])
    rain_flood['END_TIME'] = pd.to_datetime(rain_flood['END_TIME'])
    # 将无水位监测但在JISHUI表中存在的积水点存档。
    '''selNegSno(S_P_List)'''
    # 处理泵站属性，合并到积水属性表中：overpass_abute
    oaData = PumpDF(S_P_List, overpass_abute, pump_run_data)
    ra_fo_pm = rain_flood_pump(rain_flood, oaData, pump_run_data)
