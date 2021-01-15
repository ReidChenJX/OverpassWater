#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/1/14 17:44
# @Author   : ReidChen
# Document  ：生成模型训练测试数据

import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from keras.models import Sequential
from keras.layers import Dense, Activation,Bidirectional,LSTM,TimeDistributed

# 加载数据，对单个下立交积水点进行模型预测

'''训练数据类:从基础文件中梳理整理出可用于分析的数据'''


class TrainData:
    def __init__(self, s_no, IOF):
        """
        对建模数据进行时序处理，转变为训练数据与测试数据
        param  s_no: 下立交积水点的s_no
        param  data: 训练数据
        param  abute: 数据点的属性
        param  IOF -> List：如[30, 7, 12]，代表前30条数据预测后7条数据,每条数据有12个特征
        """
        self.s_no = s_no
        self.X = None
        self.y = None
        self.IOF = IOF
    
    def _get_data(self):
        # 对原始数据进行变化
        path = '../data/model_data/{s_no}.csv'.format(s_no=self.s_no)
        org_data = pd.read_csv(path, encoding='gbk')
        
        # 将时间特征置为索引并进行排序
        org_data['T_SYSTIME'] = pd.to_datetime(org_data['T_SYSTIME'])
        org_data.set_index(keys='T_SYSTIME', inplace=True)
        org_data.sort_index(inplace=True)
        
        train_y = org_data['N_VALUE']
        train_x = org_data.drop(columns=['S_NO', 'val_last', 'N_VALUE'])
        
        return train_x, train_y
    
    def _transform(self, n_input, n_out, n_features):
        # train为数据，多维的时间序列；n_input为输入的维数，n_out为预测的步数，n_features为要使用前几个特征
        train_x, train_y = self._get_data()
        X, y = list(), list()
        in_start = 0
        for _ in range(len(train_x)):
            in_end = in_start + n_input
            out_end = in_end + n_out
            if out_end <= len(train_x):
                X.append(train_x.iloc[in_start:in_end, 0:n_features])  # 使用几个特征
                y.append(train_y.iloc[in_end:out_end])
            in_start += 1
        
        return np.array(X), np.array(y)
    
    def transform(self):
        in_put, out_put, features = self.IOF[0], self.IOF[1], self.IOF[2]
        X, y = self._transform(n_input=in_put, n_out=out_put, n_features=features)
        
        self.X, self.y = X, y


s_no = 2015060043
in_put, out_put, features = 30, 5, 15
in_out_fea = [in_put, out_put, features]
train_data = TrainData(s_no=s_no, IOF=in_out_fea)
train_data.transform()
# LSTM 模型的训练数据
train_x = train_data.X
train_y = train_data.y

model = keras.Sequential()
model.add(Bidirectional(LSTM(3,activation='relu'), input_shape=(in_put, features)))
model.add(Dense(out_put))

model.compile(optimizer='adam',loss='mse')
model.fit(train_x, train_y, epochs=1000, shuffle=False, )

predict_y = model.predict(train_x)
predict_y = pd.DataFrame(predict_y)
predict_y.to_csv('../data/model_data/pre_{s_no}.csv'.format(s_no=s_no),encoding='gbk')