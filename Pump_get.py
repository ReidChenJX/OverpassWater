#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2020/12/30 15:54
# @Author   : ReidChen

import numpy as np
import pandas as pd
import cx_Oracle
import pymssql
import os
from datetime import datetime, timedelta
from multiprocessing import Process
from dateutil.relativedelta import relativedelta

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
	"PM.S_DRAI_PUMP_ADD,"\
	"PM.N_DRAI_PUMP_FLOW_FACT_Y,"\
	"PM.N_DRAI_PUMP_FLOW_FACT_W,"\
	"PM.N_DRAI_PUMP_POW_SUM_Y,"\
	"PM.N_DRAI_PUMP_POW_SUM_W,"\
	"PM.N_DISTRICT,"\
	"PM.N_DRAI_PUMP_TYPE,"\
	"PM.N_DRAI_PUMP_TYPE_FEAT,"\
	"PM.S_PUMPS_TYPE,"\
	"PM.N_PUMPS_NUM, "\
	"PM.N_PUMPS_FLOW "\
	"FROM " \
	"T_JISHUI  JI " \
	"LEFT JOIN V_OVERPUMP  PM ON JI.S_XTBM = PM.S_XTBM"


conn = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.1.203:1521/ORCL')
data = pd.read_sql(overpass_sql, con=conn)

data.to_csv('./data/PumpData.csv', index=False, encoding='gbk')
conn.close()


pump_sql = "SELECT S_STID,S_BJBH,S_BJTYPE,S_PUMPTYPE,S_KTBZT,T_KBTIME,T_TBTIME,N_KBSW,N_TBSW " \
		   "FROM T_KTBMX_HIS WHERE T_KBTIME >= TO_DATE( '2020-06-01', 'yyyy-MM-dd' ) " \
		   "AND T_KBTIME < TO_DATE( '2020-11-01', 'yyyy-MM-dd' )"

conn = cx_Oracle.connect('YXJG_Wavenet', 'YXJG_Wavenet', '172.18.1.203:1521/ORCL')
data = pd.read_sql(pump_sql, con=conn)

data.to_csv('./data/PumpHis.csv', index=False, encoding='gbk')
conn.close()