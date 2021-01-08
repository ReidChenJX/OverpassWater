#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2020/12/29
# @Author   : ReidChen
# document  : Get From Database.

import numpy as np
import pandas as pd
import cx_Oracle
import pymssql
import os
from datetime import datetime, timedelta
from multiprocessing import Process
from dateutil.relativedelta import relativedelta

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def overpassData(start_time, end_time, log, conn):
    # 根据时间获取下立交积水数据
    start_time = start_time
    end_time = end_time
    
    overpass_sql = "SELECT S_JISHUI_MONITOR_HIS.S_NO, S_JISHUI_MONITOR_HIS.T_SYSTIME, S_JISHUI_MONITOR_HIS.N_VALUE, " \
                   "S_JISHUI_MONITOR_HIS.S_HASMONITOR, S_JISHUI_MONITOR_HIS.S_STATENAME,T_JISHUI.S_ADDR, " \
                   "T_JISHUI.S_BUILDDATE, T_JISHUI.S_PROUNIT, T_JISHUI.S_MANAGE_UNIT, T_JISHUI.S_MAINTAIN_UNIT, " \
                   "T_JISHUI.S_STATIONID, T_JISHUI.S_STATIONNAME " \
                   "FROM S_JISHUI_MONITOR_HIS LEFT JOIN T_JISHUI ON S_JISHUI_MONITOR_HIS.S_NO = T_JISHUI.S_NO  " \
                    "WHERE S_JISHUI_MONITOR_HIS.T_SYSTIME >= TO_DATE('{start_time}','yyyy-MM-dd')" \
                   " AND S_JISHUI_MONITOR_HIS.T_SYSTIME < TO_DATE('{end_time}','yyyy-MM-dd')".format(
                start_time=start_time,end_time=end_time)
    
    data = pd.read_sql(overpass_sql, con=conn)
    if log == 0:
        data.to_csv('../data/data/2020overpass.csv',index=False, encoding='gbk')
    else:
        data.to_csv('../data/data/2020overpass.csv', mode='a', header=False, index=False, encoding='gbk')

def rainData(start_time, end_time, log, conn):
    start_time = start_time
    end_time = end_time

    rain_sql = "SELECT S_YULIANG_MIN_HIS.S_STATIONID, S_YULIANG_MIN_HIS.D_TIME, " \
                "S_YULIANG_MIN_HIS.N_RAINVALUE, S_YULIANG_MIN_HIS.S_STATIONNAME, " \
                "T_DRAIN_STATION.S_DIST, T_DRAIN_STATION.S_XIANGZHEN " \
                "FROM S_YULIANG_MIN_HIS LEFT JOIN T_DRAIN_STATION ON " \
                "S_YULIANG_MIN_HIS.S_STATIONID = T_DRAIN_STATION.S_STATIONID " \
                "WHERE D_TIME >= TO_DATE('{start_time}', 'yyyy-MM-dd') AND D_TIME < TO_DATE('{end_time}', 'yyyy-MM-dd') ".format(
            start_time=start_time, end_time=end_time)

    rain_data = pd.read_sql(rain_sql, con=conn)
    if log == 0:
        rain_data.to_csv('../data/data/2020rain_data.csv', index=False, encoding='gbk')
    else:
        rain_data.to_csv('../data/data/2020rain_data.csv', mode='a', header=False, index=False, encoding='gbk')

def pump_his(start_time, end_time, log, conn):
    start_time = start_time
    end_time = end_time
    pump_his_sql = "SELECT " \
                   "S_STID, " \
                   "S_BJBH, " \
                   "S_BJTYPE, " \
                   "S_PUMPTYPE, " \
                   "S_KTBZT," \
                   "T_KBTIME," \
                   "T_TBTIME " \
                   "N_KBSW," \
                   "N_TBSW " \
                   "FROM " \
                   "T_KTBMX_HIS "\
                   "WHERE T_KBTIME >= TO_DATE('{start_time}', 'yyyy-MM-dd') AND T_KBTIME < TO_DATE('{end_time}', 'yyyy-MM-dd') ".format(
            start_time=start_time, end_time=end_time)
    pump_his = pd.read_sql(pump_his_sql, con=conn)
    if log == 0:
        pump_his.to_csv('../data/data/2020pump_his.csv', index=False, encoding='gbk')
    else:
        pump_his.to_csv('../data/data/2020pump_his.csv', index=False, encoding='gbk', mode='a', header=False)

def ETforST(time):
    start_time = datetime.strptime(time, '%Y-%m-%d')    # 转化为时间格式
    end_time = start_time + relativedelta(days=+15)    # 时序推迟，产生多段时间拼接
    end_time = datetime.strftime(end_time, '%Y-%m-%d')  # 转化为字符格式
    return  end_time

def overpass_get():
    # 下立交数据多时段批次获取
    conn_overpass = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.1.203:1521/ORCL')
    start_time = '2020-01-01'  # 2020年1月 开始到 2020年12月31号
    log = 0  # 分批次,0代表第一次，后续增加为1
    for i in range(23):
        end_time = ETforST(start_time)
        overpassData(start_time, end_time, log, conn_overpass)
        log += 1
        start_time = end_time
    # 最后一次获取当前时间到12月31号的数据
    overpassData(start_time, '2020-12-31', log, conn_overpass)
    # 关闭连接
    conn_overpass.close()

def rain_get():
    # 降雨数据多时段批次获取
    conn_rain = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.1.203:1521/ORCL')
    start_time = '2020-01-01'  # 2020年1月 开始到 2020年12月31号
    log = 0  # 分批次,0代表第一次，后续增加为1
    for i in range(23):
        end_time = ETforST(start_time)
        rainData(start_time, end_time, log, conn_rain)
        log += 1
        start_time = end_time
    # 最后一次获取当前时间到12月31号的数据
    rainData(start_time, '2020-12-31', log, conn_rain)
    # 关闭连接
    conn_rain.close()

def pump_get():
    # 泵站数据阶段性获取
    conn_pump = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.1.203:1521/ORCL')
    start_time = '2020-01-01'  # 2020年1月 开始到 2020年12月31号
    log = 0  # 分批次,0代表第一次，后续增加为1
    for i in range(23):
        end_time = ETforST(start_time)
        pump_his(start_time, end_time, log, conn_pump)
        log += 1
        start_time = end_time
    # 最后一次获取当前时间到12月31号的数据
    pump_his(start_time, '2020-12-31', log, conn_pump)
    # 关闭连接
    conn_pump.close()

def sno_pump_data():
    pump_sql = "SELECT " \
                   "JI.S_NO, " \
                   "JI.S_ADDR, " \
                   "JI.S_PSXT, " \
                   "JI.S_DIRECTTO, " \
                   "JI.S_JSGGJ," \
                   "JI.S_JSGYX," \
                   "JI.S_CSGGJ," \
                   "JI.S_CSGYX," \
                   "JI.S_SBTS," \
                   "JI.S_SBLL," \
                   "JI.S_XTBM," \
                   "JI.S_STATIONID," \
                   "PM.S_DRAI_PUMP_NAME," \
                   "PM.S_DRAI_PUMP_ADD," \
                   "PM.N_DRAI_PUMP_FLOW_FACT_Y," \
                   "PM.N_DRAI_PUMP_FLOW_FACT_W," \
                   "PM.N_DRAI_PUMP_POW_SUM_Y," \
                   "PM.N_DRAI_PUMP_POW_SUM_W," \
                   "PM.N_DISTRICT," \
                   "PM.N_DRAI_PUMP_TYPE," \
                   "PM.N_DRAI_PUMP_TYPE_FEAT," \
                   "PM.S_PUMPS_TYPE," \
                   "PM.N_PUMPS_NUM, " \
                   "PM.N_PUMPS_FLOW, " \
                   "DA.S_STID, " \
                   "DA.S_ZONE_ID " \
                   "FROM " \
                   "T_JISHUI  JI " \
                   "LEFT JOIN V_OVERPUMP  PM ON JI.S_XTBM = PM.S_XTBM " \
                   "LEFT JOIN T_DRAINPUMP_ADD  DA ON JI.S_XTBM = DA.S_XTBM"

    conn = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.1.203:1521/ORCL')
    data = pd.read_sql(pump_sql, con=conn)
    data.to_csv('../data/data/SnoPump.csv', index=False, encoding='gbk')
    conn.close()

if __name__ == '__main__':
    
    p1 = Process(target=overpass_get)
    p2 = Process(target=rain_get)
    p3 = Process(target=pump_get)
    p1.start()
    p2.start()
    p3.start()
    # sno_pump_data()

    

    
