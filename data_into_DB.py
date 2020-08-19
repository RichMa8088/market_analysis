# -*- coding:utf-8 -*-
# ----------------------------------------------------------------------------#
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
from time import time
import pandas as pd
# --加载其他模块
from read_csv_file import read_csv_file
from pre_category import pre_category_trends
from pre_brand_rank import pre_brand_tf_rank, pre_brand_ts_rank
from pre_goods_rank import pre_goods_tf_rank, pre_goods_ts_rank, pre_goods_ct_rank
from pre_keywords import pre_keywords_trends, pre_keywords_rank
# mysql类型
from sqlalchemy.dialects.mysql import DECIMAL, INTEGER, CHAR, DATE, FLOAT, TIMESTAMP, VARCHAR
from sqlalchemy import create_engine  # 引擎
from sqlalchemy.ext.declarative import declarative_base  # 基类

# --参数设置
start_time = time()
pd.set_option('display.max_rows', 20)
pd.set_option('display.max_columns', 30)
pd.set_option('display.width', 500)
missing_values = ['n/a', 'na', '--', 'Null', 'NULL', '\t', 'NaN']
rootDir = '/home/rich/myfile/data'  # 输入根目录的路径


# --数据库类型字典
def mapping_df_types(df):
    type_dict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            type_dict.update({i: VARCHAR(length=512)})
        if "float" in str(j):
            type_dict.update({i: DECIMAL(19, 4)})
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
# 1,读取行业趋势数据并进行预处理
df_cg_trends = pre_category_trends(rootDir, '行业趋势', missing_values, 0, 'utf-8')
# print(df_cg_trends.info())
# df_cg_trends.to_csv('/home/rich/myfile/output/temp.csv', index=False)
# 导入数据库
if len(df_cg_trends) > 0:
    df_cg_trends.to_sql(name='category_trends', con=engine, if_exists='append', index=False,
                        dtype=mapping_df_types(df_cg_trends))
else:
    print("无行业趋势数据文件导入")

# ----------------------------------------------------------------------------#
# 2,读取品牌排行数据并进行预处理
df_brand_tf_rank = pre_brand_tf_rank(rootDir, '品牌 高流量排行', missing_values, 0, 'utf-8')
# print(df_brand_tf_rank.info())
# df_brand_tf_rank.to_csv('/home/rich/myfile/output/temp.csv', index=False)
# 导入数据库
if len(df_brand_tf_rank) > 0:
    df_brand_tf_rank.to_sql(name='brand_traffic_rank', con=engine, if_exists='append', index=False,
                            dtype=mapping_df_types(df_brand_tf_rank))
else:
    print("无品牌高流量排行数据文件导入")
# ------2.2
df_brand_ts_rank = pre_brand_ts_rank(rootDir, '品牌 高交易排行', missing_values, 0, 'utf-8')
# print(df_brand_ts_rank.info())
# df_brand_ts_rank.to_csv('/home/rich/myfile/output/temp.csv', index=False)
# 导入数据库
if len(df_brand_tf_rank) > 0:
    df_brand_ts_rank.to_sql(name='brand_transaction_rank', con=engine, if_exists='append',
                            index=False, dtype=mapping_df_types(df_brand_ts_rank))
else:
    print("无品牌高交易排行数据文件导入")

# ----------------------------------------------------------------------------#
# 3,读取商品排行数据并进行预处理
df_goods_tf_rank = pre_goods_tf_rank(rootDir, '商品 高流量排行', missing_values, 0, 'utf-8')
# print(df_goods_tf_rank.info())
# df_goods_tf_rank.to_csv('/home/rich/myfile/output/temp.csv', index=False)
# 导入数据库
if len(df_goods_tf_rank) > 0:
    df_goods_tf_rank.to_sql(name='goods_traffic_rank', con=engine, if_exists='append', index=False,
                            dtype=mapping_df_types(df_goods_tf_rank))
else:
    print("无商品高流量排行数据文件导入")
# ------3.2
df_goods_ts_rank = pre_goods_ts_rank(rootDir, '商品 高交易排行', missing_values, 0, 'utf-8')
# print(df_goods_ts_rank.info())
# df_goods_ts_rank.to_csv('/home/rich/myfile/output/temp.csv')
# 导入数据库
if len(df_goods_ts_rank) > 0:
    df_goods_ts_rank.to_sql(name='goods_transaction_rank', con=engine, if_exists='append',
                            index=False, dtype=mapping_df_types(df_goods_ts_rank))
else:
    print("无商品高交易排行数据文件导入")
# ------3.3
df_goods_ct_rank = pre_goods_ct_rank(rootDir, '商品 高意向排行', missing_values, 0, 'utf-8')
# print(df_goods_ct_rank.info())
# df_goods_ct_rank.to_csv('/home/rich/myfile/output/temp.csv')
# 导入数据库
if len(df_goods_ct_rank) > 0:
    df_goods_ct_rank.to_sql(name='goods_cart_rank', con=engine, if_exists='append', index=False,
                            dtype=mapping_df_types(df_goods_ct_rank))
else:
    print("无商品高意向排行数据文件导入")

# ----------------------------------------------------------------------------#
# 4,读取关键词数据并进行预处理
df_terms_td = pre_keywords_trends(rootDir, '搜索词趋势', missing_values, 0, 'utf-8')
# print(df_terms_td.info())
# df_terms_td.to_csv('/home/rich/myfile/output/temp.csv')
# 导入数据库
if len(df_terms_td) > 0:
    df_terms_td.to_sql(name='keywords_trends', con=engine, if_exists='append', index=False,
                       dtype=mapping_df_types(df_terms_td))
else:
    print("无搜索词趋势数据文件导入")
# ------4.2
df_terms_rank = pre_keywords_rank(rootDir, '热搜 排行', missing_values, 0, coding='utf-8')
# print(df_terms_rank.info())
# df_terms_rank.to_csv('/home/rich/myfile/output/temp.csv')
# 导入数据库
if len(df_terms_rank) > 0:
    df_terms_rank.to_sql(name='keywords_rank', con=engine, if_exists='append', index=False,
                         dtype=mapping_df_types(df_terms_rank))
else:
    print("无行业热搜词排行数据文件导入")

# ----------------------------------------------------------------------------#
end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
