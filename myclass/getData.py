#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2020/12/29
# @Author   : ReidChen

import pandas as pd
import cx_Oracle
import pymssql
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

class linkBase(object):
    '''
    调用getData方法获取数据
    选择不同的数据库，type 可选择 oracle，mysql，sqlserver
    '''
    def __init__(self, type='oracle'):
        self.type = type
    
    def oracle_data(self, sql):
        # 选择连接Oracle数据库
        conn = cx_Oracle.connect('YXJG_Wavenet','YXJG_Wavenet','172.18.1.203:1521/ORCL')
        data = pd.read_sql(sql, con=conn, chunksize=100000)
        conn.close()
        return data
    
    def mysql_data(self, sql):
        # 选择连接MySQL数据库
        conn = pymssql.connect(server="172.18.0.201", port="3306", user='',
                               password='@1', database='')
        data = pd.read_sql(sql, con=conn)
        conn.close()
        return data
        pass
    
    def sqlserver_data(self, sql):
        # 选择连接SQlServer数据库
        conn = pymssql.connect(server="172.18.0.201", port="1433", user='datareader',
                               password='WAVENET@1', database='PSCXLJ')
        data = pd.read_sql(sql, con=conn)
        conn.close()
        return data
    
    def getData(self,sql):
        if self.type == 'oracle':
            return self.oracle_data(sql=sql)
        elif self.type == 'mysql':
            return self.mysql_data(sql=sql)
        elif self.type == 'sqlserver':
            return self.sqlserver_data(sql=sql)
        else: raise Exception('未定义的数据库类型，可选 oracle，mysql，sqlserver')
        