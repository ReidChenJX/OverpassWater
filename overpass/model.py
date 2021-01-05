#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/1/5 16:57
# @Author   : ReidChen

class HoldData:
    """
    用于提供可进行建模的数据。
    parameters:
    name：积水点的S_NO
    list：各种数据表
    
    """
    
    def __init__(self, name):
        self.name = name
        self.data_path = '../data/model_data/{name}.csv'.format(name=name)
        self.data = None
        self.size = None
        self.time = None
        self.end = None
    
    def clear_data(self, name, list):
        
        # 清洗数据，按照时间格式合并数据
        data = None
        
        # overpass_abute = overpass_abute
        # original_overpass_data = mouth8-9Overpass
        # original_rain_data = mouth8-9rain_data
        # original_pump_data = PumpHis
        
        
        
        
        
        
        self.data = data
        


name = 2019
list = list()
model_data = HoldData(name)
