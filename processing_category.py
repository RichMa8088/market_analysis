# -*- coding:utf-8 -*-
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
from time import time
import pandas as pd
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

# ------------------
sql_cg_trends = 'SELECT 类目,月份,访客数,搜索人数,加购人数,支付人数,交易金额 FROM market_analysis.category_trends'
df_cg_trends = pd.read_sql_query(sql=sql_cg_trends, con=conn, coerce_float=True, parse_dates=None)
df_cg_trends = df_cg_trends.loc[df_cg_trends['交易金额'] > 0]
# 去重复
df_cg_trends.sort_values(by=['类目', '月份'], ascending=True, inplace=True)
df_cg_trends = df_cg_trends.reset_index(drop=True)
df_cg_trends = df_cg_trends.drop_duplicates(subset=['类目', '月份'], keep='last')
# 增加新字段
df_cg_trends['客单价'] = df_cg_trends['交易金额'] / df_cg_trends['支付人数']
df_cg_trends['搜索占比'] = df_cg_trends['搜索人数'] / df_cg_trends['访客数']
# 四舍五入
rd_decimals = pd.Series([1, 2], index=['客单价', '搜索占比'])
df_cg_trends = df_cg_trends.round(rd_decimals)
# 合并参数信息
df_cg_trends = pd.merge(df_cg_trends, pm_date, how='left', on='月份')
df_cg_trends = pd.merge(df_cg_trends, pm_cg, how='left', left_on="类目", right_on="采集类目")
op_cg_trends = df_cg_trends.loc[
    df_cg_trends['周期'].notnull(), ["类目", '对应类目', '类目简称', '周期', '绘图月份', '月份', '访客数',
                                   '搜索人数', '支付人数', '交易金额']]
print(op_cg_trends.info())
# --导出excel到本地
writer = pd.ExcelWriter('/home/rich/myfile/output/行业类目趋势.xlsx')
op_cg_trends.to_excel(writer, sheet_name='类目趋势', header=True, index=False)
writer.save()

# -------------------------------------------------------
end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
