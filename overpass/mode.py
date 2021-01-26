#!/usr/bin/python3
# -*- coding:utf-8 -*-
# @time     : 2021/1/14 17:44
# @Author   : ReidChen
# Document  ：生成模型训练测试数据

import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from keras.models import Sequential
from keras.layers import Dense, Activation, Bidirectional, LSTM, TimeDistributed, RepeatVector
from keras.callbacks import ModelCheckpoint
from tensorflow.keras.callbacks import ModelCheckpoint
from sklearn.preprocessing import StandardScaler

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
        train_x = org_data.drop(columns=['S_NO', 'N_VALUE'])
        
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


def draw_data(in_put, out_put, features):
    in_out_fea = [in_put, out_put, features]
    # 训练数据
    s_no_train = '2015060043'
    train_data = TrainData(s_no=s_no_train, IOF=in_out_fea)
    
    # 测试数据
    s_no_test = '2015060043_test'
    test_data = TrainData(s_no=s_no_test, IOF=in_out_fea)
    
    # LSTM 模型的训练数据
    train_data.transform()
    train_x = train_data.X
    train_y = train_data.y
    # LSTM 模型测试数据
    test_data.transform()
    test_x = test_data.X
    test_y = test_data.y
    
    return train_x, train_y, test_x, test_y, train_data.s_no


# 双向LSTM模型
# def create_model(in_put, out_put, features):
#
#     model = keras.Sequential()
#     model.add(Bidirectional(LSTM(4, activation='relu'), input_shape=(in_put, features)))
#     model.add(Dense(out_put))
#
#     model.compile(optimizer='adam', loss='mse', metrics=['mae'])
#     return model

# 堆叠LSTM模型
def create_model(in_put, out_put, features):
    model = keras.Sequential()
    model.add(LSTM(5, activation='tanh', return_sequences=True, input_shape=(in_put, features)))
    model.add(LSTM(5, activation='tanh', return_sequences=True, dropout=0.2, recurrent_dropout=0.2))
    model.add(LSTM(3, activation='tanh', dropout=0.2, recurrent_dropout=0.2))
    model.add(Dense(out_put))
    
    model.compile(optimizer='adam', loss='mse', metrics=['mae'])
    return model


# 模型与数据均是输入50，输出5，特征数为16 的 seq_to_seq的预测模型
in_put, out_put, features = 30, 5, 16
train_x, train_y, test_x, test_y, s_no = draw_data(in_put, out_put, features)

# # 创建训练模型
model = create_model(in_put, out_put, features)
# # 为模型提供保存路径
filepath = "../model/LSTM.ckpt"
callback = ModelCheckpoint(filepath=filepath, monitor='val_loss',
                           verbose=1, save_best_only=True, save_weights_only=True,
                           model='min')
history = model.fit(train_x, train_y, epochs=150, shuffle=True,
                    validation_data=(test_x, test_y), callbacks=[callback])

# 加载最佳模型

pre_model = create_model(in_put, out_put, features)
pre_model.load_weights(filepath=filepath)

# 选择最好的模型进行数据预测
predict_y = pre_model.predict(train_x)
predict_test_y = pre_model.predict(test_x)

predict_y_ = pd.DataFrame(predict_y)
predict_y_.to_csv('../data/model_data/pre_train_{s_no}.csv'.format(s_no=s_no), encoding='gbk', index=False)

predict_test_y_ = pd.DataFrame(predict_test_y)
predict_test_y_.to_csv('../data/model_data/pre_test_{s_no}.csv'.format(s_no=s_no), encoding='gbk', index=False)

history_loss = pd.DataFrame(history.history['loss'])
history_loss.to_csv('../data/model_data/history_loss.csv', encoding='gbk', index=False)

history_val_loss = pd.DataFrame(history.history['val_loss'])
history_val_loss.to_csv('../data/model_data/history_val_loss.csv', encoding='gbk', index=False)


def draw_analyse():
    # 绘制训练集 loss 与验证集val_loss 的趋势变化
    plt.figure(figsize=(15, 15), dpi=200)
    plt.plot(history.history['loss'])
    plt.plot(history.history['val_loss'])
    plt.title('model train vs validation loss')
    plt.ylabel('loss')
    plt.xlabel('epoch')
    plt.legend(['train', 'validation'], loc='upper right')
    plt.show()
    
    train_y_value = train_y[:, 0]
    predict_y_value = predict_y[:, 0]
    # 绘制测试集在本模型下的预测效果
    plt.figure(figsize=(15, 15), dpi=200)
    plt.plot(train_y_value)
    plt.plot(predict_y_value)
    plt.title('Train data true value and predict value')
    plt.ylabel('loss')
    plt.xlabel('epoch')
    plt.legend(['train', 'predict'], loc='upper right')
    plt.show()
    
    test_y_value = test_y[:, 0]
    predict_test_y_value = predict_test_y[:, 0]
    # 绘制测试集在本模型下的预测效果
    plt.figure(figsize=(15, 15), dpi=200)
    plt.plot(test_y_value)
    plt.plot(predict_test_y_value)
    plt.title('Test data true value and predict value')
    plt.ylabel('loss')
    plt.xlabel('epoch')
    plt.legend(['train', 'predict'], loc='upper right')
    plt.show()


draw_analyse()
