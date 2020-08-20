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
pm_gjc = read_xlxs_file(rootDir, '参数表', missing_values, '竞品关键词', 0, 0, coding='utf-8')
# print(pm_gjc.info())  # 读取关键词参数

pm_gjc_lb = read_xlxs_file(rootDir, '参数表', missing_values, '关键词分类', 0, 0, coding='utf-8')
# print(pm_gjc_lb.info())  # 读取关键词参数

# --------------------1、热搜关键词排行
sql_keywords_rk = 'SELECT 类目,起始日期,关键词,搜索人数,点击人数,支付人数 FROM market_analysis.keywords_rank'
df_keywords_rk = pd.read_sql_query(sql=sql_keywords_rk, con=conn, coerce_float=True,
                                   parse_dates=None)
df_keywords_rk = df_keywords_rk.loc[df_keywords_rk['搜索人数'] > 0]
# 去重复
df_keywords_rk.sort_values(by=['起始日期', '类目', '关键词'], ascending=True, inplace=True)
df_keywords_rk = df_keywords_rk.reset_index(drop=True)
df_keywords_rk = df_keywords_rk.drop_duplicates(subset=['起始日期', '类目', '关键词'], keep='last')

# 合并参数信息
df_keywords_rk = pd.merge(df_keywords_rk, pm_cg, how='left', left_on="类目", right_on="采集类目")
op_keywords_rk = pd.merge(df_keywords_rk, pm_gjc_lb, how='left', left_on="关键词", right_on="关键词")
op_keywords_rk = op_keywords_rk[['类目', '对应类目', '类目简称', '分类', '起始日期', '关键词', '搜索人数',
                                 '点击人数', '支付人数']]
print(op_keywords_rk.info())

# --------------------2、关键词趋势
sql_keywords_td = 'SELECT 月份,关键词,搜索人数,点击人数,' \
                  '支付人数,交易金额 FROM market_analysis.keywords_trends'
df_keywords_td = pd.read_sql_query(sql=sql_keywords_td, con=conn, coerce_float=True,
                                   parse_dates=None)
df_keywords_td = df_keywords_td.loc[df_keywords_td['搜索人数'] > 0]
# 去重复
df_keywords_td.sort_values(by=['月份', '关键词'], ascending=True, inplace=True)
df_keywords_td = df_keywords_td.reset_index(drop=True)
df_keywords_td = df_keywords_td.drop_duplicates(subset=['月份', '关键词'], keep='last')
# 合并参数信息
op_keywords_td = pd.merge(df_keywords_td, pm_gjc, how='left', left_on="关键词", right_on="关键词")
op_keywords_td = pd.merge(op_keywords_td, pm_date, how='left', on='月份')
op_keywords_td = op_keywords_td[['类目', '周期', '绘图月份', '月份', '关键词', '搜索人数', '点击人数',
                                 '支付人数', '交易金额']]
print(op_keywords_td.info())

# --导出excel到本地
writer = pd.ExcelWriter('/home/rich/myfile/output/行业搜索词.xlsx')

op_keywords_rk.to_excel(writer, sheet_name='类目热搜词', header=True, index=False)
op_keywords_td.to_excel(writer, sheet_name='类目核心词趋势', header=True, index=False)

writer.save()

##############################
end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
