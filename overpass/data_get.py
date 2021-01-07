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
        data.to_csv('../data/data/mouth8-9Overpass.csv',index=False, encoding='gbk')
    else:
        data.to_csv('../data/data/mouth8-9Overpass.csv', mode='a', header=False, index=False, encoding='gbk')

def rainData(start_time, end_time, log, conn):
    start_time = start_time
    end_time = end_time

    rain_sql = "SELECT S_YULIANG_1HOUR.S_STATIONID, S_YULIANG_1HOUR.D_TIME, " \
                "S_YULIANG_1HOUR.N_RAINVALUE, S_YULIANG_1HOUR.S_STATIONNAME, " \
                "T_DRAIN_STATION.S_DIST, T_DRAIN_STATION.S_XIANGZHEN " \
                "FROM S_YULIANG_1HOUR LEFT JOIN T_DRAIN_STATION ON " \
                "S_YULIANG_1HOUR.S_STATIONID = T_DRAIN_STATION.S_STATIONID " \
                "WHERE D_TIME >= TO_DATE('{start_time}', 'yyyy-MM-dd') AND D_TIME < TO_DATE('{end_time}', 'yyyy-MM-dd') ".format(
            start_time=start_time, end_time=end_time)

    rain_data = pd.read_sql(rain_sql, con=conn)
    if log == 0:
        rain_data.to_csv('../data/data/mouth8-9rain_data.csv', index=False, encoding='gbk')
    else:
        rain_data.to_csv('../data/data/mouth8-9rain_data.csv', mode='a', header=False, index=False, encoding='gbk')

def ETforST(time):
    start_time = datetime.strptime(time, '%Y-%m-%d')    # 转化为时间格式
    end_time = start_time + relativedelta(days=+15)    # 时间推迟一个月
    end_time = datetime.strftime(end_time, '%Y-%m-%d')  # 转化为字符格式
    return  end_time

def overpass_get():
    # 下立交数据多时段批次获取
    conn_overpass = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.0.201:1521/yfzx')
    start_time = '2020-06-01'  # 六月份开始，后面五个月
    log = 0  # 分批次,0代表第一次，后续增加为1
    for i in range(9):
        end_time = ETforST(start_time)
        overpassData(start_time, end_time, log, conn_overpass)
        log += 1
        start_time = end_time
    # 最后一次获取当前时间到11月1号的数据
    overpassData(start_time, '2020-11-01', log, conn_overpass)
    # 关闭连接
    conn_overpass.close()

def rain_get():
    # 降雨数据多时段批次获取
    conn_rain = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.0.201:1521/yfzx')
    start_time = '2020-06-01'  # 六月份开始，后面五个月
    log = 0  # 分批次,0代表第一次，后续增加为1
    for i in range(9):
        end_time = ETforST(start_time)
        rainData(start_time, end_time, log, conn_rain)
        log += 1
        start_time = end_time
    # 最后一次获取当前时间到11月1号的数据
    rainData(start_time, '2020-11-01', log, conn_rain)
    # 关闭连接
    conn_rain.close()


def pump_data():
    overpass_sql = "SELECT " \
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
    data = pd.read_sql(overpass_sql, con=conn)
    data.to_csv('../data/data/SnoPump.csv', index=False, encoding='gbk')
    conn.close()

if __name__ == '__main__':
    
    p1 = Process(target=overpass_get)
    p2 = Process(target=rain_get)
    p1.start()
    p2.start()
    pump_data()
    
    
    

    
