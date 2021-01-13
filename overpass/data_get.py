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
    
    # 在SQL 文件中读取查询语句
    overpass_sql = open(file='./sql/overpass.sql',)
    list_text = overpass_sql.readlines()
    overpass_sql.close()
    # 将list类型的查询语句装换为str类型，并插入值
    sql_text = "".join(list_text)
    sql = sql_text.format(start_time=start_time, end_time=end_time)
    
    
    data = pd.read_sql(sql, con=conn)
    if log == 0:
        data.to_csv('../data/data/2020overpass.csv',index=False, encoding='gbk')
    else:
        data.to_csv('../data/data/2020overpass.csv', mode='a', header=False, index=False, encoding='gbk')

def overpassData2(start_time, end_time, conn):
    # 根据时间获取下立交积水数据
    start_time = start_time
    end_time = end_time
    
    # 在SQL 文件中读取查询语句
    overpass_sql = open(file='./sql/overpass2.sql', )
    list_text = overpass_sql.readlines()
    overpass_sql.close()
    # 将list类型的查询语句装换为str类型，并插入值
    sql_text = "".join(list_text)
    sql = sql_text.format(start_time=start_time, end_time=end_time)
    
    # overpassData2 只进行数据的增加
    data = pd.read_sql(sql, con=conn)
    data.to_csv('../data/data/2020overpass.csv', mode='a', header=False, index=False, encoding='gbk')

def rainData(start_time, end_time, log, conn):
    start_time = start_time
    end_time = end_time
    
    # 在SQL 文件中读取查询语句
    rain_sql = open(file='./sql/rain.sql')
    list_text = rain_sql.readlines()
    rain_sql.close()
    # 将list类型的查询语句装换为str类型，并插入值
    sql_text = "".join(list_text)
    sql = sql_text.format(start_time=start_time, end_time=end_time)

    rain_data = pd.read_sql(sql, con=conn)
    if log == 0:
        rain_data.to_csv('../data/data/2020rain_data.csv', index=False, encoding='gbk')
    else:
        rain_data.to_csv('../data/data/2020rain_data.csv', mode='a', header=False, index=False, encoding='gbk')

def pump_his(start_time, end_time, log, conn):
    start_time = start_time
    end_time = end_time
    
    pump_his_sql = open(file='./sql/pump.sql')
    list_text = pump_his_sql.readlines()
    pump_his_sql.close()
    
    sql_text = "".join(list_text)
    sql = sql_text.format(start_time=start_time, end_time=end_time)
    
    pump_his = pd.read_sql(sql, con=conn)
    if log == 0:
        pump_his.to_csv('../data/data/2020pump_his.csv', index=False, encoding='gbk')
    else:
        pump_his.to_csv('../data/data/2020pump_his.csv', index=False, encoding='gbk', mode='a', header=False)

def ETforST(time):
    start_time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')    # 转化为时间格式
    end_time = start_time + relativedelta(days=+15)    # 时序推迟，产生多段时间拼接
    end_time = datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S')  # 转化为字符格式
    return  end_time

def overpass_get():
    # 下立交数据多时段批次获取
    # 第一次获取，从SQLServer中获取1月到5月的数据
    conn_overpass = pymssql.connect(server='172.18.0.201', user='datareader',
                                    password='WAVENET@1', database='PSCXLJ')
    start_time = '2020-01-01 00:00:00'  # 2020年1月 开始到 2020年6月01号
    log = 0  # 分批次,0代表第一次，后续增加为1
    for i in range(23):
        end_time = ETforST(start_time)
        overpassData(start_time, end_time, log, conn_overpass)
        log += 1
        start_time = end_time
    # 最后一次获取当前时间到12月31号的数据
    overpassData(start_time, '2020-06-02 0:22:49', log, conn_overpass)
    # 关闭连接
    conn_overpass.close()

    # 第二次，从Oracle中获取6月到12月的数据
    conn_overpass = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.1.203:1521/ORCL')
    start_time = '2020-06-01 20:22:50'  # 2020年6月 开始到 2020年12月31号
    log = 0  # 分批次,0代表第一次，后续增加为1
    for i in range(14):
        end_time = ETforST(start_time)
        overpassData2(start_time, end_time, conn_overpass)
        log += 1
        start_time = end_time
    overpassData2(start_time, '2020-12-31 23:59:59', conn_overpass)
    # 关闭连接
    conn_overpass.close()
    

def rain_get():
    # 降雨数据多时段批次获取
    conn_rain = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.1.203:1521/ORCL')
    start_time = '2020-01-01 00:00:00'  # 2020年1月 开始到 2020年12月31号
    log = 0  # 分批次,0代表第一次，后续增加为1
    for i in range(23):
        end_time = ETforST(start_time)
        rainData(start_time, end_time, log, conn_rain)
        log += 1
        start_time = end_time
    # 最后一次获取当前时间到12月31号的数据
    rainData(start_time, '2020-12-31 23:59:59', log, conn_rain)
    # 关闭连接
    conn_rain.close()

def pump_get():
    # 泵站数据阶段性获取
    conn_pump = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.1.203:1521/ORCL')
    start_time = '2020-01-01 00:00:00'  # 2020年1月 开始到 2020年12月31号
    log = 0  # 分批次,0代表第一次，后续增加为1
    for i in range(23):
        end_time = ETforST(start_time)
        pump_his(start_time, end_time, log, conn_pump)
        log += 1
        start_time = end_time
    # 最后一次获取当前时间到12月31号的数据
    pump_his(start_time, '2020-12-31 23:59:59', log, conn_pump)
    # 关闭连接
    conn_pump.close()

def sno_pump_data():
    pump_sql = open(file='./sql/snopump.sql')
    list_text = pump_sql.readlines()
    pump_sql.close()

    conn = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.1.203:1521/ORCL')
    data = pd.read_sql(list_text, con=conn)
    data.to_csv('../data/data/SnoPump.csv', index=False, encoding='gbk')
    conn.close()

if __name__ == '__main__':
    pump_get()
    # overpass_get()
    # overpass_get = Process(target=overpass_get)
    # rain_get = Process(target=rain_get)
    # pump_get = Process(target=pump_get)
    # overpass_get.start()
    # rain_get.start()
    # pump_get.start()
    
    # sno_pump_data()

    

    
