# -*- coding:utf-8 -*-
# ----------------------------------------------------------------------------#
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
from time import time
import pandas as pd
# --加载其他模块
from read_csv_file import read_csv_file
from sqlalchemy.dialects.mysql import BIGINT, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL, DOUBLE, \
    FLOAT, INTEGER, MEDIUMINT, SMALLINT, TIME, TIMESTAMP, TINYINT, VARCHAR, YEAR  # mysql类型
from sqlalchemy import create_engine  # 引擎
from sqlalchemy.ext.declarative import declarative_base  # 基类

# --参数设置
start_time = time()
pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 500)
missing_values = ['n/a', 'na', '--', 'Null', 'NULL', '\t']
rootDir = '/home/rich/myfile/data'  # 输入根目录的路径


# --数据库类型字典
def mapping_df_types(df):
    type_dict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            type_dict.update({i: VARCHAR(length=512)})
        if "float" in str(j):
            type_dict.update({i: DECIMAL(19, 2)})
        if "int" in str(j):
            type_dict.update({i: INTEGER()})
    return type_dict


# --sqlalchemy基本操作
HOSTNAME = "127.0.0.1"
PORT = "3306"
DATABASE = "market_analysis"
ACCOUNT = "root"
PASSWORD = "123456"
DB_URI = "mysql+pymysql://{}:{}@{}:{}/{}?charset=UTF8MB4" \
    .format(ACCOUNT, PASSWORD, HOSTNAME, PORT, DATABASE)
engine = create_engine(DB_URI, pool_recycle=3600)
conn = engine.connect()  # --连接数据库
Base = declarative_base()  # --基类

# ----------------------------------------------------------------------------#
# --------------------1、类目趋势数据读取并且处理--------#
df_cg_trends = read_csv_file(rootDir, '行业趋势', missing_values, 0, coding='utf-8')
df_cg_trends = df_cg_trends.rename(
    columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
# --字段处理
df_cg_trends.drop(['终端', '类别', '客单价', '路径', '文件名'], axis=1, inplace=True)

# 格式转化
df_cg_trends['月份'] = pd.to_datetime(df_cg_trends['月份'])
#
df_cg_trends['搜索人数'] = df_cg_trends['搜索人数'].astype(int)
df_cg_trends['搜索次数'] = df_cg_trends['搜索次数'].astype(int)
df_cg_trends['访客数'] = df_cg_trends['访客数'].astype(int)
df_cg_trends['浏览量'] = df_cg_trends['浏览量'].astype(int)
df_cg_trends['收藏人数'] = df_cg_trends['收藏人数'].astype(int)
df_cg_trends['收藏次数'] = df_cg_trends['收藏次数'].astype(int)
df_cg_trends['加购人数'] = df_cg_trends['加购人数'].astype(int)
df_cg_trends['加购次数'] = df_cg_trends['加购次数'].astype(int)
df_cg_trends['支付人数'] = df_cg_trends['支付人数'].astype(int)
#
df_cg_trends['交易金额'] = df_cg_trends['交易金额'].astype(float)
# 保留最新的数据记录,去重复
df_cg_trends.sort_values(by=['类目名', '月份'], ascending=True, inplace=True)
df_cg_trends = df_cg_trends.reset_index(drop=True)
df_cg_trends = df_cg_trends.drop_duplicates(subset=['类目名', '月份'], keep='last')
# print(df_cg_trends.info())
# df_cg_trends.to_csv('/home/rich/myfile/output/temp.csv')


# ----------------------------------------------------------------------------#
# ----------------------将数据导入数据库------#
df_cg_trends.to_sql(name='category_trends', con=engine, if_exists='append', index=False,
                    dtype=mapping_df_types(df_cg_trends))
# ----------------------------------------------------------------------------#
end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
