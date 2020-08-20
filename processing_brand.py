# -*- coding:utf-8 -*-
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
from time import time
import pandas as pd
import numpy as np
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

# -----------------1.1
sql_brand_tf = 'SELECT 类目,月份,品牌,访客人数,搜索人数,交易金额 FROM market_analysis.brand_traffic_rank'
df_brand_tf = pd.read_sql_query(sql=sql_brand_tf, con=conn, coerce_float=True, parse_dates=None)
df_brand_tf = df_brand_tf.loc[df_brand_tf['交易金额'] > 0]
# 去重复
df_brand_tf.sort_values(by=['类目', '月份', '品牌'], ascending=True, inplace=True)
df_brand_tf = df_brand_tf.reset_index(drop=True)
df_brand_tf = df_brand_tf.drop_duplicates(subset=['类目', '月份', '品牌'], keep='last')
# -----------------1.2
sql_brand_ts = 'SELECT 类目,月份,品牌,支付转化率,交易金额 FROM market_analysis.brand_transaction_rank'
df_brand_ts = pd.read_sql_query(sql=sql_brand_ts, con=conn, coerce_float=True, parse_dates=None)
df_brand_ts = df_brand_ts.loc[df_brand_ts['交易金额'] > 0]
# 去重复
df_brand_ts.sort_values(by=['类目', '月份', '品牌'], ascending=True, inplace=True)
df_brand_ts = df_brand_ts.reset_index(drop=True)
df_brand_ts = df_brand_ts.drop_duplicates(subset=['类目', '月份', '品牌'], keep='last')
# -----------------2合并
df_brand = pd.merge(df_brand_tf, df_brand_ts, how='left', on=['类目', '月份', '品牌'])
# 合并参数信息
df_brand = pd.merge(df_brand, pm_date, how='left', on='月份')
df_brand = pd.merge(df_brand, pm_cg, how='left', left_on="类目", right_on="采集类目")
df_brand = df_brand.loc[df_brand['周期'].notnull()]
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
# 增加新字段
df_brand['支付人数'] = df_brand['访客数'] * df_brand['支付转化率']
df_brand['客单价'] = df_brand['交易金额'] / (df_brand['访客数'] * df_brand['支付转化率'])
df_brand['搜索占比'] = df_brand['搜索人数'] / df_brand['访客数']
# 四舍五入
rd_decimals = pd.Series([1, 2, 0, 4], index=['客单价', '搜索占比', '支付人数', '支付转化率'])
df_brand = df_brand.round(rd_decimals)
df_brand.drop(['支付转化率_x', '支付转化率_y', '标识_names'], axis=1, inplace=True)
op_brand = df_brand.loc[(df_brand['客单价'] > 1) & (df_brand['访客数'] > 1)]
op_brand = op_brand[["类目", '对应类目', '类目简称', '周期', '绘图月份', '月份', '品牌', '访客数',
                     '搜索人数', '支付人数', '交易金额']]
print(op_brand.info())
# --导出excel到本地
writer = pd.ExcelWriter('/home/rich/myfile/output/类目品牌榜.xlsx')

op_brand.to_excel(writer, sheet_name='类目品牌榜', header=True, index=False)

writer.save()

# -------------------------------------------------------
end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
