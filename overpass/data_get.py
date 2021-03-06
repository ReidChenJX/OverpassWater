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
from datetime import datetime
from dateutil.relativedelta import relativedelta

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def overpassData(start_time, end_time, log, conn):
    # 根据时间获取下立交积水数据   数据来源：Oracle
    start_time = start_time
    end_time = end_time
    
    # 在SQL 文件中读取查询语句
    overpass_sql = open(file='./sql/overpass.sql', )
    list_text = overpass_sql.readlines()
    overpass_sql.close()
    # 将list类型的查询语句装换为str类型，并插入值
    sql_text = "".join(list_text)
    sql = sql_text.format(start_time=start_time, end_time=end_time)
    
    data = pd.read_sql(sql, con=conn)
    if log == 0:
        data.to_csv('../data/data/2020overpass.csv', index=False, encoding='gbk')
    else:
        data.to_csv('../data/data/2020overpass.csv', mode='a', header=False, index=False, encoding='gbk')


def overpassData2(start_time, end_time, conn):
    # 根据时间获取下立交积水数据  数据来源：SQLServer
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
    start_time = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')  # 转化为时间格式
    end_time = start_time + relativedelta(days=+90)  # 时序推迟，产生多段时间拼接
    end_time = datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S')  # 转化为字符格式
    return end_time


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


'''并行获取训练集数据'''


def org_train_data():
    # 设置多进程，并行获取积水、降雨、泵站数据
    sno_pump_data()
    overpass_get()
    rain_get()
    pump_get()


'''并行获取测试集数据'''


def org_test_data():
    start_time = '2019-07-05 00:00:00'
    end_time = '2019-12-31 23:59:59'
    
    '''获取下立交积水数据'''
    
    def overpass_data(start_time, end_time):
        
        def getDa_fromDB(start_time, last_time, log, conn):
            # overpass 下立交积水数据，来自SQLServer，采用overpass.sql
            overpass_sql = open(file='./sql/overpass.sql', )
            list_text = overpass_sql.readlines()
            overpass_sql.close()
            # 将list类型的查询语句装换为str类型，并插入值
            sql_text = "".join(list_text)
            test_overpass_sql = sql_text.format(start_time=start_time, end_time=last_time)
            
            data = pd.read_sql(test_overpass_sql, con=conn)
            if log == 0:
                data.to_csv('../data/datatest/overpass_test.csv', index=False, encoding='gbk')
            else:
                data.to_csv('../data/datatest/overpass_test.csv', mode='a', header=False, index=False, encoding='gbk')
        
        conn_overpass = pymssql.connect(server='172.18.0.201', user='datareader',
                                        password='WAVENET@1', database='PSCXLJ')
        start_time = start_time
        log = 0  # 分批次,0代表第一次，后续增加为1
        for i in range(30):
            last_time = ETforST(start_time)  # 时间日期延后2月
            if last_time < end_time:
                getDa_fromDB(start_time, last_time, log, conn_overpass)
                log += 1
                start_time = last_time
            else:
                # 最后一次获取当前时间到12月31号的数据
                getDa_fromDB(start_time, end_time, log, conn_overpass)
                break
        # 关闭连接
        conn_overpass.close()
    
    '''获取降雨积水数据'''
    
    def rain_data(start_time, end_time):
        
        def getRa_fromDB(start_time, last_time, log, conn):
            # rain 降雨数据，来自Oracle数据库，采用 testRainData.sql
            rain_sql = open(file='./sql/testRainData.sql')
            list_text = rain_sql.readlines()
            rain_sql.close()
            # 将list类型的查询语句装换为str类型，并插入值
            sql_text = "".join(list_text)
            test_rain_sql = sql_text.format(start_time=start_time, end_time=last_time)
            
            rain_data = pd.read_sql(test_rain_sql, con=conn)
            if log == 0:
                rain_data.to_csv('../data/datatest/rain_data_test.csv', index=False, encoding='gbk')
            else:
                rain_data.to_csv('../data/datatest/rain_data_test.csv', mode='a',
                                 header=False, index=False, encoding='gbk')
        
        conn_rain = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.0.201:1521/yfzx')
        start_time = start_time
        log = 0  # 分批次,0代表第一次，后续增加为1
        for i in range(23):
            last_time = ETforST(start_time)
            if last_time < end_time:
                getRa_fromDB(start_time, last_time, log, conn_rain)
                log += 1
                start_time = last_time
            else:
                # 最后一次获取当前时间到12月31号的数据
                getRa_fromDB(start_time, end_time, log, conn_rain)
                break
        # 关闭连接
        conn_rain.close()
    
    '''获取泵站运行数据'''
    
    def pump_data(start_time, end_time):
        
        def get_PMP_fromDB(start_time, last_time, log, conn):
            # pump 泵站数据，来自Oracle数据库，采用 pump.sql
            pump_his_sql = open(file='./sql/pump.sql')
            list_text = pump_his_sql.readlines()
            pump_his_sql.close()
            sql_text = "".join(list_text)
            test_pump_sql = sql_text.format(start_time=start_time, end_time=last_time)
            
            pump_his = pd.read_sql(test_pump_sql, con=conn)
            if log == 0:
                pump_his.to_csv('../data/datatest/pump_his_test.csv', index=False, encoding='gbk')
            else:
                pump_his.to_csv('../data/datatest/pump_his_test.csv', index=False,
                                encoding='gbk', mode='a', header=False)
        
        # 泵站数据阶段性获取
        conn_pump = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.0.201:1521/yfzx')
        start_time = start_time
        log = 0  # 分批次,0代表第一次，后续增加为1
        for i in range(23):
            last_time = ETforST(start_time)
            if last_time < end_time:
                get_PMP_fromDB(start_time, last_time, log, conn_pump)
                log += 1
                start_time = last_time
            else:
                # 最后一次获取当前时间到12月31号的数据
                get_PMP_fromDB(start_time, end_time, log, conn_pump)
                break
        # 关闭连接
        conn_pump.close()
    
    overpass_data(start_time, end_time)
    rain_data(start_time, end_time)
    pump_data(start_time, end_time)


'''获取下立交积水点的历史掉线数据'''


def offline_overpass():
    # 开启连接
    conn_overpass = pymssql.connect(server='172.18.0.201', user='datareader',
                                    password='WAVENET@1', database='PSCXLJ')
    
    offline_sql = open(file='./sql/offline_overpass.sql')
    
    list_text = offline_sql.readlines()
    offline_sql.close()
    sql_text = "".join(list_text)
    
    offline_data = pd.read_sql(sql_text, con=conn_overpass)
    offline_data.to_csv('../data/data/offline_data.csv', index=False, encoding='gbk')
    
    # 关闭连接
    conn_overpass.close()


'''获取下立交S_NO与降雨点S_STATIONID的对应'''


def sno_stationid():
    # 从Oracle中获取S_NO 与 S_STATIONID 的对应，积水数据写入csv前需补充S_STATIONID的值
    
    conn_oracle = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.0.201:1521/yfzx')
    
    offline_sql = open(file='./sql/sno_stationid.sql')
    
    list_text = offline_sql.readlines()
    offline_sql.close()
    sql_text = "".join(list_text)
    
    sno_stationid_data = pd.read_sql(sql_text, con=conn_oracle)
    sno_stationid_data.to_csv('../data/data/SNO_STATIONID.csv', index=False, encoding='gbk')
    
    # 关闭连接
    conn_oracle.close()


'''获取EDA数据，所有的积水数据与降雨数据'''


def EDA_data():
    # 积水数据 ------------------
    start_time = '2013-01-01 00:00:00'
    end_time = '2020-12-31 23:59:59'
    
    def EDA_hydrop_data(start_time, end_time):
        conn_overpass = pymssql.connect(server='172.18.0.201', user='datareader',
                                        password='WAVENET@1', database='PSCXLJ')
        # 读取SQL文件，修改为可读的SQL语句
        offline_sql = open(file='./sql/overpass.sql')
        list_text = offline_sql.readlines()
        offline_sql.close()
        # 将list类型的查询语句装换为str类型，
        sql_text = "".join(list_text)
        
        log = 0  # 分批次,0代表第一次，后续增加为1
        for i in range(10000):
            
            last_time = ETforST(start_time)  # 结束时间为向后推迟三个月
            # 未到结束时间
            if last_time < end_time:
                
                sql = sql_text.format(start_time=start_time, end_time=last_time)
                data = pd.read_sql(sql, con=conn_overpass)
                if log == 0:
                    data.to_csv('../data/EDAdata/EDAoverpass.csv', index=False, encoding='gbk')
                else:
                    data.to_csv('../data/EDAdata/EDAoverpass.csv', mode='a', header=False, index=False, encoding='gbk')
                log += 1
                start_time = last_time
            else:
                sql = sql_text.format(start_time=start_time, end_time=end_time)
                data = pd.read_sql(sql, con=conn_overpass)
                data.to_csv('../data/EDAdata/EDAoverpass.csv', mode='a', header=False, index=False, encoding='gbk')
                break
        
        conn_overpass.close()
    
    # # 获取积水数据
    # EDA_hydrop_data(start_time, end_time)
    
    # 降雨数据 ------------------
    def EDA_rain_data(start_time, end_time):
        
        # 降雨数据量过大，采用单个积水点的对应点进行数据获取
        PATH_SNO = 2015060043
        sno_stainId = pd.read_csv('../data/data/SNO_STATIONID.csv', encoding='gbk')
        # S_NO 对应的降雨监测点 S_STATIONID
        s_stationid = sno_stainId[sno_stainId['S_NO'] == PATH_SNO]['S_STATIONID'].values[0]
        
        conn_rain = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.0.201:1521/yfzx')
        # 读取SQL文件，修改为可读的SQL语句
        offline_sql = open(file='./sql/EDArain.sql')
        list_text = offline_sql.readlines()
        offline_sql.close()
        # 将list类型的查询语句装换为str类型，
        sql_text = "".join(list_text)
        
        log = 0  # 分批次,0代表第一次，后续增加为1
        for i in range(10000):
            
            last_time = ETforST(start_time)  # 结束时间为向后推迟三个月
            # 未到结束时间
            if last_time < end_time:
                
                sql = sql_text.format(start_time=start_time, end_time=last_time, s_stationid=s_stationid)
                data = pd.read_sql(sql, con=conn_rain)
                if log == 0:
                    data.to_csv('../data/EDAdata/EDAraindata.csv', index=False, encoding='gbk')
                else:
                    data.to_csv('../data/EDAdata/EDAraindata.csv', mode='a', header=False, index=False, encoding='gbk')
                log += 1
                start_time = last_time
            else:
                sql = sql_text.format(start_time=start_time, end_time=end_time, s_stationid=s_stationid)
                data = pd.read_sql(sql, con=conn_rain)
                data.to_csv('../data/EDAdata/EDAraindata.csv', mode='a', header=False, index=False, encoding='gbk')
                break
        
        conn_rain.close()
    
    # 获取降雨数据
    EDA_rain_data(start_time, end_time)


if __name__ == '__main__':
    # # 获取训练集数据
    # org_train_data()
    
    # # 获取测试集数据
    # org_test_data()
    
    # # 获取下立交掉线数据
    # offline_overpass()
    
    # 获取 S_NO与S_STATIONID的维护
    # sno_stationid()
    
    # 获取EDA数据
    EDA_data()
