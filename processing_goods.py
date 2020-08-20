# -*- coding:utf-8 -*-
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
from time import time
import pandas as pd
import numpy as np
import itertools
#
from sqlalchemy import create_engine  # 引擎
from sqlalchemy.ext.declarative import declarative_base  # 基类
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
# print(pm_cg.info())  # 读取类目信息参数
#
pm_lb = read_xlxs_file(rootDir, '参数表', missing_values, '价格区间', 0, 0, coding='utf-8')
# print(pm_lb.info())  # 读取价格区间参数
# -------------------1.1
sql_goods_tf = 'SELECT 类目,月份,商品ID,商品信息,店铺,访客人数,搜索人数,交易金额 FROM market_analysis.goods_traffic_rank'
df_goods_tf = pd.read_sql_query(sql=sql_goods_tf, con=conn, coerce_float=True, parse_dates=None)
df_goods_tf = df_goods_tf.loc[df_goods_tf['交易金额'] > 0]
# 去重复
df_goods_tf.sort_values(by=['类目', '月份', '商品ID'], ascending=True, inplace=True)
df_goods_tf = df_goods_tf.reset_index(drop=True)
df_goods_tf = df_goods_tf.drop_duplicates(subset=['类目', '月份', '商品ID'], keep='last')
# -------------------1.2
sql_goods_ts = 'SELECT 类目,月份,商品ID,支付转化率 FROM market_analysis.goods_transaction_rank'
df_goods_ts = pd.read_sql_query(sql=sql_goods_ts, con=conn, coerce_float=True, parse_dates=None)
# 去重复
df_goods_ts.sort_values(by=['类目', '月份', '商品ID'], ascending=True, inplace=True)
df_goods_ts = df_goods_ts.reset_index(drop=True)
df_goods_ts = df_goods_ts.drop_duplicates(subset=['类目', '月份', '商品ID'], keep='last')
# -------------------1.3
sql_goods_ct = 'SELECT 类目,月份,商品ID,加购人数 FROM market_analysis.goods_cart_rank'
df_goods_ct = pd.read_sql_query(sql=sql_goods_ct, con=conn, coerce_float=True, parse_dates=None)
# 去重复
df_goods_ct.sort_values(by=['类目', '月份', '商品ID'], ascending=True, inplace=True)
df_goods_ct = df_goods_ct.reset_index(drop=True)
df_goods_ct = df_goods_ct.drop_duplicates(subset=['类目', '月份', '商品ID'], keep='last')
# -------------------2合并
df_goods = pd.merge(df_goods_tf, df_goods_ts, how='left', on=['类目', '月份', '商品ID'])
df_goods = pd.merge(df_goods, df_goods_ct, how='left', on=['类目', '月份', '商品ID'])
# 合并参数信息
df_goods = pd.merge(df_goods, pm_date, how='left', on='月份')
df_goods = pd.merge(df_goods, pm_cg, how='left', left_on="类目", right_on="采集类目")
df_goods = df_goods.loc[df_goods['周期'].notnull()]
# 缺失值处理
mean_gp_goods = df_goods.loc[df_goods['支付转化率'].notnull(), ['商品ID', '支付转化率']]
mean_v_goods = mean_gp_goods.groupby('商品ID').mean()
df_goods = pd.merge(df_goods, mean_v_goods, how='left', on='商品ID')
fna_values = {'支付转化率_x': 0, '支付转化率_y': 0}
df_goods = df_goods.fillna(value=fna_values)
df_goods['支付转化率'] = np.where(df_goods['支付转化率_x'] > df_goods['支付转化率_y'],
                             df_goods['支付转化率_x'], df_goods['支付转化率_y'])
df_goods = df_goods.loc[df_goods['支付转化率'] > 0]

df_goods['加购率'] = df_goods['加购人数'] / df_goods['访客人数']
mean_gp_goods_ct = df_goods.loc[df_goods['加购率'].notnull(), ['商品ID', '加购率']]
mean_v_goods_ct = mean_gp_goods_ct.groupby('商品ID').mean()
df_goods = pd.merge(df_goods, mean_v_goods_ct, how='left', on='商品ID')
fna_values = {'加购率_x': 0, '加购率_y': 0}
df_goods = df_goods.fillna(value=fna_values)
df_goods['加购率'] = np.where(df_goods['加购率_x'] > df_goods['加购率_y'],
                           df_goods['加购率_x'], df_goods['加购率_y'])
# 增加新字段，四舍五入
df_goods['支付人数'] = df_goods['访客人数'] * df_goods['支付转化率']
df_goods['客单价'] = df_goods['交易金额'] / (df_goods['访客人数'] * df_goods['支付转化率'])
df_goods['搜索占比'] = df_goods['搜索人数'] / df_goods['访客人数']
rd_decimals = pd.Series([1, 2, 0, 4, 4], index=['客单价', '搜索占比', '支付人数', '加购率', '支付转化率'])
df_goods = df_goods.round(rd_decimals)
df_goods.drop(
    ['支付转化率_x', '支付转化率_y', '加购率_x', '加购率_y', '加购人数', '加购人数', '采集类目'], axis=1, inplace=True)
df_goods = df_goods.loc[(df_goods['客单价'] > 1) & (df_goods['访客人数'] > 1)]
lm_names = pm_lb['对应类目'].drop_duplicates(keep='last')
df_kd_fq = pd.Series(dtype=object)
for lm_n in lm_names:
    listBins0 = pm_lb.loc[pm_lb['对应类目'] == lm_n, ['值']].values.tolist()
    listBins = list(itertools.chain.from_iterable(listBins0))
    listLabels0 = pm_lb.loc[pm_lb['对应类目'] == lm_n, ['价格区间']].values.tolist()
    listLabels = list(itertools.chain.from_iterable(listLabels0))
    listLabels.remove('区间外')
    lm_kd = df_goods.loc[df_goods['对应类目'] == lm_n, '客单价']
    kd_fq = pd.cut(lm_kd, bins=listBins, labels=listLabels)
    df_kd_fq = df_kd_fq.append(kd_fq)
op_goods = pd.merge(df_goods, pd.DataFrame(df_kd_fq), how='left', left_index=True, right_index=True)
op_goods.rename(columns={'访客人数': '访客数', '商品自定义分类': '自定义类别', '类目简称_x': '类目简称',
                         '商品信息_x': '商品信息', '店铺': '所属店铺', 0: '客单区间'}, inplace=True)
op_goods = op_goods[['类目', '对应类目', '类目简称', '周期', '绘图月份', '月份', '商品ID', '商品信息', '所属店铺',
                     '访客数', '搜索人数', '支付人数', '交易金额', '加购率', '客单区间']]
print(op_goods.info())
#
# --导出excel到本地
writer = pd.ExcelWriter('/home/rich/myfile/output/行业商品榜.xlsx')
#
op_goods.to_excel(writer, sheet_name='类目商品榜', header=True, index=False)
#
writer.save()

# ----------------------------------------
end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
