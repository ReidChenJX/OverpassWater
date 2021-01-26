#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/1/19 10:10
# @Author   : ReidChen
# Document  ：依次处理数据

import os
import numpy as np

def main():
    
    os.system("python3 ../overpass/rain_feature.py")
    os.system("python3 ../overpass/overpass_feature.py")
    os.system("python3 ../overpass/OpassToRain.py")
    os.system("python3 ../overpass/pump_feature.py")
    os.system("python3 ../overpass/detection.py")
    

if __name__ == '__main__':
    main()