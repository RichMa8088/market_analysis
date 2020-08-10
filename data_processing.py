# -*- coding:utf-8 -*-
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
from time import time
import pandas as pd
import numpy as np
#
from sqlalchemy import create_engine  # 引擎
from sqlalchemy.ext.declarative import declarative_base  # 基类
from sqlalchemy import Column  # sqlalchemy类型
from sqlalchemy.dialects.mysql import DECIMAL, INTEGER, CHAR, DATE, FLOAT, TIMESTAMP, VARCHAR
from read_xlsx_file import read_xlxs_file

# --参数设置
start_time = time()
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 500)
missing_values = ['n/a', 'na', '--', 'Null', 'NULL', '\t']
rootDir = '/home/rich/myfile/parameter'  # 输入参数的路径

# --sqlalchemy基本操作
HOSTNAME = "127.0.0.1"
PORT = "3306"
DATABASE = "market_analysis"
ACCOUNT = "root"
PASSWORD = "123456"
DB_URI = "mysql+pymysql://{}:{}@{}:{}/{}?charset=UTF8MB4". \
    format(ACCOUNT, PASSWORD, HOSTNAME, PORT, DATABASE)
engine = create_engine(DB_URI, pool_recycle=3600)
conn = engine.connect()  # --连接数据库
Base = declarative_base()  # --基类
# --------------------0、读取参数文件中的参数-------------------------#
pm_date = read_xlxs_file(rootDir, '参数表', missing_values, '日期范围', 0, 0, coding='utf-8')
# print(pm_date.info())  # 读取日期范围参数

pm_cg = read_xlxs_file(rootDir, '参数表', missing_values, '类目信息', 0, 0, coding='utf-8')
# print(pm_cg.info())  # 读取日期范围参数

# --------------------1、提取类目趋势数据-------------------------#
sql_cg_trends = 'SELECT 类目,月份,访客数,搜索人数,加购人数,支付人数,' \
                '交易金额 FROM market_analysis.category_trends'
df_cg_trends = pd.read_sql_query(sql=sql_cg_trends, con=conn, coerce_float=True, parse_dates=None)
df_cg_trends = df_cg_trends.loc[df_cg_trends['交易金额'] > 0]

# 去重复
df_cg_trends.sort_values(by=['类目', '月份'], ascending=True, inplace=True)
df_cg_trends = df_cg_trends.reset_index(drop=True)
df_cg_trends = df_cg_trends.drop_duplicates(subset=['类目', '月份'], keep='last')
# 增加新字段，四舍五入
df_cg_trends['客单价'] = df_cg_trends['交易金额'] / df_cg_trends['支付人数']
df_cg_trends['搜索占比'] = df_cg_trends['搜索人数'] / df_cg_trends['访客数']

rd_decimals = pd.Series([1, 2], index=['客单价', '搜索占比'])
df_cg_trends = df_cg_trends.round(rd_decimals)
# 合并参数信息
df_cg_trends = pd.merge(df_cg_trends, pm_date, how='left', on='月份')
df_cg_trends = pd.merge(df_cg_trends, pm_cg, how='left', left_on="类目", right_on="采集类目")
df_cg_trends = df_cg_trends.loc[
    df_cg_trends['周期'].notnull(), ['对应类目', '类目简称', '周期', '绘图月份', '月份', '访客数', '搜索人数', '支付人数',
                                   '交易金额', '客单价', '搜索占比']]

# print(df_cg_trends.info())
df_cg_trends.to_csv('/home/rich/myfile/output/result1.csv', index=False)

# --------------------1、提取品牌数据-------------------------#
sql_brand_tf = 'SELECT 类目,月份,品牌,访客人数,搜索人数,' \
               '交易金额 FROM market_analysis.brand_traffic_rank'
df_brand_tf = pd.read_sql_query(sql=sql_brand_tf, con=conn, coerce_float=True, parse_dates=None)
df_brand_tf = df_brand_tf.loc[df_brand_tf['交易金额'] > 0]
# 去重复
df_brand_tf.sort_values(by=['类目', '月份', '品牌'], ascending=True, inplace=True)
df_brand_tf = df_brand_tf.reset_index(drop=True)
df_brand_tf = df_brand_tf.drop_duplicates(subset=['类目', '月份', '品牌'], keep='last')

##############################
sql_brand_ts = 'SELECT 类目,月份,品牌,支付转化率,' \
               '交易金额 FROM market_analysis.brand_transaction_rank'
df_brand_ts = pd.read_sql_query(sql=sql_brand_ts, con=conn, coerce_float=True, parse_dates=None)
df_brand_ts = df_brand_ts.loc[df_brand_ts['交易金额'] > 0]
# 去重复
df_brand_ts.sort_values(by=['类目', '月份', '品牌'], ascending=True, inplace=True)
df_brand_ts = df_brand_ts.reset_index(drop=True)
df_brand_ts = df_brand_ts.drop_duplicates(subset=['类目', '月份', '品牌'], keep='last')

# 合并
df_brand = pd.merge(df_brand_tf, df_brand_ts, how='left', on=['类目', '月份', '品牌'])
# 合并参数信息
df_brand = pd.merge(df_brand, pm_date, how='left', on='月份')
df_brand = pd.merge(df_brand, pm_cg, how='left', left_on="类目", right_on="采集类目")
df_brand = df_brand.loc[
    df_brand['周期'].notnull(), ['对应类目', '类目简称', '周期', '绘图月份', '月份', '品牌', '访客人数', '搜索人数',
                               '支付转化率', '交易金额_x']]
df_brand.rename(columns={'交易金额_x': '交易金额', '访客人数': '访客数'}, inplace=True)

# 缺失值处理
df_brand['标识_names'] = df_brand['对应类目'] + df_brand['品牌']
mean_gp = df_brand.loc[df_brand['支付转化率'].notnull(), ['标识_names', '支付转化率']]
mean_v = mean_gp.groupby('标识_names').mean()
df_brand = pd.merge(df_brand, mean_v, how='left', on='标识_names')
fna_values = {'支付转化率_x': 0, '支付转化率_y': 0}
df_brand = df_brand.fillna(value=fna_values)
df_brand['支付转化率'] = np.where(df_brand['支付转化率_x'] > df_brand['支付转化率_y'],
                             df_brand['支付转化率_x'], df_brand['支付转化率_y'])
df_brand = df_brand.loc[df_brand['支付转化率'] > 0]
# 增加新字段，四舍五入
df_brand['支付人数'] = df_brand['访客数'] * df_brand['支付转化率']
df_brand['客单价'] = df_brand['交易金额'] / (df_brand['访客数'] * df_brand['支付转化率'])
df_brand['搜索占比'] = df_brand['搜索人数'] / df_brand['访客数']
rd_decimals = pd.Series([1, 2, 0, 4], index=['客单价', '搜索占比', '支付人数', '支付转化率'])
df_brand = df_brand.round(rd_decimals)
df_brand.drop(['支付转化率_x', '支付转化率_y', '标识_names'], axis=1, inplace=True)
df_brand = df_brand.loc[(df_brand['客单价'] > 1) & (df_brand['访客数'] > 1)]
# print(df_brand.info())
df_brand.to_csv('/home/rich/myfile/output/result2.csv', index=False)

# --------------------1、提取商品数据-------------------------#
sql_goods_tf = 'SELECT 类目,月份,商品ID,商品信息,店铺,访客人数,搜索人数,' \
               '交易金额 FROM market_analysis.goods_traffic_rank'
df_goods_tf = pd.read_sql_query(sql=sql_goods_tf, con=conn, coerce_float=True, parse_dates=None)
df_goods_tf = df_goods_tf.loc[df_goods_tf['交易金额'] > 0]
# 去重复
df_goods_tf.sort_values(by=['类目', '月份', '商品ID'], ascending=True, inplace=True)
df_goods_tf = df_goods_tf.reset_index(drop=True)
df_goods_tf = df_goods_tf.drop_duplicates(subset=['类目', '月份', '商品ID'], keep='last')

##############################
sql_goods_ts = 'SELECT 类目,月份,商品ID,支付转化率 FROM market_analysis.goods_transaction_rank'
df_goods_ts = pd.read_sql_query(sql=sql_goods_ts, con=conn, coerce_float=True, parse_dates=None)
# 去重复
df_goods_ts.sort_values(by=['类目', '月份', '商品ID'], ascending=True, inplace=True)
df_goods_ts = df_goods_ts.reset_index(drop=True)
df_goods_ts = df_goods_ts.drop_duplicates(subset=['类目', '月份', '商品ID'], keep='last')

##############################
sql_goods_ct = 'SELECT 类目,月份,商品ID,加购人数 FROM market_analysis.goods_cart_rank'
df_goods_ct = pd.read_sql_query(sql=sql_goods_ct, con=conn, coerce_float=True, parse_dates=None)
# 去重复
df_goods_ct.sort_values(by=['类目', '月份', '商品ID'], ascending=True, inplace=True)
df_goods_ct = df_goods_ct.reset_index(drop=True)
df_goods_ct = df_goods_ct.drop_duplicates(subset=['类目', '月份', '商品ID'], keep='last')
# 合并
df_goods = pd.merge(df_goods_tf, df_goods_ts, how='left', on=['类目', '月份', '商品ID'])
df_goods = pd.merge(df_goods, df_goods_ct, how='left', on=['类目', '月份', '商品ID'])
# 合并参数信息
df_goods = pd.merge(df_goods, pm_date, how='left', on='月份')
df_goods = pd.merge(df_goods, pm_cg, how='left', left_on="类目", right_on="采集类目")
df_goods = df_goods.loc[
    df_goods['周期'].notnull(), ['对应类目', '类目简称', '周期', '绘图月份', '月份', '商品ID', '商品信息', '店铺',
                               '访客人数', '搜索人数', '支付转化率', '加购人数', '交易金额']]
df_goods.rename(columns={'访客人数': '访客数'}, inplace=True)
# 缺失值处理
mean_gp_goods = df_goods.loc[df_goods['支付转化率'].notnull(), ['商品ID', '支付转化率']]
mean_v_goods = mean_gp_goods.groupby('商品ID').mean()
df_goods = pd.merge(df_goods, mean_v_goods, how='left', on='商品ID')
fna_values = {'支付转化率_x': 0, '支付转化率_y': 0}
df_goods = df_goods.fillna(value=fna_values)
df_goods['支付转化率'] = np.where(df_goods['支付转化率_x'] > df_goods['支付转化率_y'],
                             df_goods['支付转化率_x'], df_goods['支付转化率_y'])
df_goods = df_goods.loc[df_goods['支付转化率'] > 0]
#
df_goods['加购率'] = df_goods['加购人数'] / df_goods['访客数']
mean_gp_goods_ct = df_goods.loc[df_goods['加购率'].notnull(), ['商品ID', '加购率']]
mean_v_goods_ct = mean_gp_goods_ct.groupby('商品ID').mean()
df_goods = pd.merge(df_goods, mean_v_goods_ct, how='left', on='商品ID')
fna_values = {'加购率_x': 0, '加购率_y': 0}
df_goods = df_goods.fillna(value=fna_values)
df_goods['加购率'] = np.where(df_goods['加购率_x'] > df_goods['加购率_y'],
                           df_goods['加购率_x'], df_goods['加购率_y'])
# 增加新字段，四舍五入
df_goods['支付人数'] = df_goods['访客数'] * df_goods['支付转化率']
df_goods['客单价'] = df_goods['交易金额'] / (df_goods['访客数'] * df_goods['支付转化率'])
df_goods['搜索占比'] = df_goods['搜索人数'] / df_goods['访客数']
rd_decimals = pd.Series([1, 2, 0, 4, 4], index=['客单价', '搜索占比', '支付人数', '加购率', '支付转化率'])
df_goods = df_goods.round(rd_decimals)
df_goods.drop(['支付转化率_x', '支付转化率_y', '加购率_x', '加购率_y', '加购人数'], axis=1, inplace=True)
df_goods = df_goods.loc[(df_goods['客单价'] > 1) & (df_goods['访客数'] > 1)]
# print(df_goods.info())
df_goods.to_csv('/home/rich/myfile/output/result3.csv', index=False)

# 设置切分区域
listBins = [0, 10, 20, 30, 40, 50, 60, 1000000]

# 设置切分后对应标签
listLabels = ['0_10', '11_20', '21_30', '31_40', '41_50', '51_60', '61及以上']

# 利用pd.cut进行数据离散化切分
"""
pandas.cut(x,bins,right=True,labels=None,retbins=False,precision=3,include_lowest=False)
x:需要切分的数据


end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
