# -*- coding:utf-8 -*-
# ----------------------------------------------------------------------------#
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
from time import time
import pandas as pd
# --加载其他模块
from read_csv_file import read_csv_file
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
# --------------------1、类目趋势数据读取并且处理--------#
df_cg_trends = read_csv_file(rootDir, '行业趋势', missing_values, 0, coding='utf-8')
df_cg_trends = df_cg_trends.rename(
    columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
# --字段处理
df_cg_trends.drop(['终端', '类别', '客单价', '路径', '文件名'], axis=1, inplace=True)
df_cg_trends.rename(columns={'类目名': '类目'}, inplace=True)
df_cg_trends['类目'].replace([r'\s+', '>'], ['', '_'], regex=True, inplace=True)
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
df_cg_trends.sort_values(by=['类目', '月份'], ascending=True, inplace=True)
df_cg_trends = df_cg_trends.reset_index(drop=True)
df_cg_trends = df_cg_trends.drop_duplicates(subset=['类目', '月份'], keep='last')
# print(df_cg_trends.info())
# df_cg_trends.to_csv('/home/rich/myfile/output/temp.csv',index=False)

# ----------------------------------------------------------------------------#
# --------------------1、品牌排行数据读取并且处理--------#
df_brand_tf_rank = read_csv_file(rootDir, '品牌 高流量排行', missing_values, 0, coding='utf-8')
df_brand_tf_rank = df_brand_tf_rank.rename(
    columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
# # --提取文件路径和下载时间信息
df_brand_tf_rank.reset_index(inplace=True, drop=True)
df_brand_tf_rank.reset_index(inplace=True)
file_split = df_brand_tf_rank['文件名'].str.split('】', expand=True)
file_split.reset_index(inplace=True)
file_split.drop([1], axis=1, inplace=True)
file_split.rename(columns={0: '类目'}, inplace=True)
df_brand_tf_rank = pd.merge(df_brand_tf_rank, file_split, how='left', on='index')

# --字段处理
df_brand_tf_rank['行业排名'].replace(['持平', '升', '降'], ['-', '-', '-'], regex=True, inplace=True)
file_split00 = df_brand_tf_rank['行业排名'].str.split('-', expand=True)
file_split00.drop([1], axis=1, inplace=True)
file_split00.rename(columns={0: '排行'}, inplace=True)
file_split00.reset_index(inplace=True)
df_brand_tf_rank = pd.merge(df_brand_tf_rank, file_split00, how='left', on='index')
df_brand_tf_rank.rename(columns={'日期': '月份', '品牌信息': '品牌', '排行': '行业访客排名'}, inplace=True)

df_brand_tf_rank['类目'].replace([r'\s+', '【'], ['', ''], regex=True, inplace=True)
df_brand_tf_rank['月份'] = df_brand_tf_rank['月份'].str[0:10]
# 格式转化
df_brand_tf_rank['月份'] = pd.to_datetime(df_brand_tf_rank['月份'])
#
df_brand_tf_rank['行业访客排名'] = df_brand_tf_rank['行业访客排名'].astype(int)
df_brand_tf_rank['访客人数'] = df_brand_tf_rank['访客人数'].astype(int)
df_brand_tf_rank['搜索人数'] = df_brand_tf_rank['搜索人数'].astype(int)
df_brand_tf_rank['交易金额'] = df_brand_tf_rank['交易金额'].astype(float)
# 保留最新的数据记录,去重复
df_brand_tf_rank.sort_values(by=['类目', '品牌', '月份'], ascending=True, inplace=True)
df_brand_tf_rank = df_brand_tf_rank.reset_index(drop=True)
df_brand_tf_rank = df_brand_tf_rank.drop_duplicates(subset=['类目', '品牌', '月份'], keep='last')
df_brand_tf_rank = df_brand_tf_rank[['类目', '月份', '品牌', '行业访客排名', '访客人数', '搜索人数', '交易金额']]

# print(df_brand_tf_rank.info())
# df_brand_tf_rank.to_csv('/home/rich/myfile/output/temp.csv')

# ----------------------------------------------------------------------------#
# --------------------1、品牌排行数据读取并且处理--------#
df_brand_ts_rank = read_csv_file(rootDir, '品牌 高交易排行', missing_values, 0, coding='utf-8')
df_brand_ts_rank = df_brand_ts_rank.rename(
    columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
# # --提取文件路径和下载时间信息
df_brand_ts_rank.reset_index(inplace=True, drop=True)
df_brand_ts_rank.reset_index(inplace=True)
file_split1 = df_brand_ts_rank['文件名'].str.split('】', expand=True)
file_split1.reset_index(inplace=True)
file_split1.drop([1], axis=1, inplace=True)
file_split1.rename(columns={0: '类目'}, inplace=True)
df_brand_ts_rank = pd.merge(df_brand_ts_rank, file_split1, how='left', on='index')

# --字段处理
df_brand_ts_rank['类目'].replace([r'\s+', '【'], ['', ''], regex=True, inplace=True)
df_brand_ts_rank['行业排名'].replace(['持平', '升', '降'], ['-', '-', '-'], regex=True, inplace=True)
file_split01 = df_brand_ts_rank['行业排名'].str.split('-', expand=True)
file_split01.drop([1], axis=1, inplace=True)
file_split01.rename(columns={0: '排行'}, inplace=True)
file_split01.reset_index(inplace=True)
df_brand_ts_rank = pd.merge(df_brand_ts_rank, file_split01, how='left', on='index')
#
df_brand_ts_rank.rename(columns={'日期': '月份', '品牌信息': '品牌', '排行': '行业交易排名'}, inplace=True)
#
df_brand_ts_rank['月份'] = df_brand_ts_rank['月份'].str[0:10]
df_brand_ts_rank['支付转化率'].replace('%', '', regex=True, inplace=True)
# 格式转化
df_brand_ts_rank['月份'] = pd.to_datetime(df_brand_ts_rank['月份'])
#
df_brand_ts_rank['行业交易排名'] = df_brand_ts_rank['行业交易排名'].astype(int)
df_brand_ts_rank['支付转化率'] = df_brand_ts_rank['支付转化率'].astype(float) / 100
df_brand_ts_rank['交易金额'] = df_brand_ts_rank['交易金额'].astype(float)
# 保留最新的数据记录,去重复
df_brand_ts_rank.sort_values(by=['类目', '品牌', '月份'], ascending=True, inplace=True)
df_brand_ts_rank = df_brand_ts_rank.reset_index(drop=True)
df_brand_ts_rank = df_brand_ts_rank.drop_duplicates(subset=['类目', '品牌', '月份'], keep='last')
df_brand_ts_rank = df_brand_ts_rank[['类目', '月份', '品牌', '行业交易排名', '交易金额', '支付转化率']]

# print(df_brand_ts_rank.info())
# df_brand_ts_rank.to_csv('/home/rich/myfile/output/temp.csv')


# ----------------------------------------------------------------------------#
# --------------------1、商品排行数据读取并且处理--------#
df_goods_tf_rank = read_csv_file(rootDir, '商品 高流量排行', missing_values, 0, coding='utf-8')
df_goods_tf_rank = df_goods_tf_rank.rename(
    columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
# # --提取文件路径和下载时间信息
df_goods_tf_rank.reset_index(inplace=True, drop=True)
df_goods_tf_rank.reset_index(inplace=True)
file_split = df_goods_tf_rank['文件名'].str.split('】', expand=True)
file_split.reset_index(inplace=True)
file_split.drop([1], axis=1, inplace=True)
file_split.rename(columns={0: '类目'}, inplace=True)
df_goods_tf_rank = pd.merge(df_goods_tf_rank, file_split, how='left', on='index')

# --字段处理
df_goods_tf_rank['行业排名'].replace(['持平', '升', '降'], ['-', '-', '-'], regex=True, inplace=True)
file_split00 = df_goods_tf_rank['行业排名'].str.split('-', expand=True)
file_split00.drop([1], axis=1, inplace=True)
file_split00.rename(columns={0: '排行'}, inplace=True)
file_split00.reset_index(inplace=True)
df_goods_tf_rank = pd.merge(df_goods_tf_rank, file_split00, how='left', on='index')
df_goods_tf_rank.rename(columns={'日期': '月份', '店铺名称': '店铺', '排行': '行业访客排名'}, inplace=True)

df_goods_tf_rank['类目'].replace([r'\s+', '【'], ['', ''], regex=True, inplace=True)
df_goods_tf_rank['月份'] = df_goods_tf_rank['月份'].str[0:10]
# 格式转化
df_goods_tf_rank['月份'] = pd.to_datetime(df_goods_tf_rank['月份'])
#
df_goods_tf_rank['行业访客排名'] = df_goods_tf_rank['行业访客排名'].astype(int)
df_goods_tf_rank['访客人数'] = df_goods_tf_rank['访客人数'].astype(int)
df_goods_tf_rank['搜索人数'] = df_goods_tf_rank['搜索人数'].astype(int)
df_goods_tf_rank['交易金额'] = df_goods_tf_rank['交易金额'].astype(float)
# 保留最新的数据记录,去重复
df_goods_tf_rank.sort_values(by=['类目', '商品ID', '月份'], ascending=True, inplace=True)
df_goods_tf_rank = df_goods_tf_rank.reset_index(drop=True)
df_goods_tf_rank = df_goods_tf_rank.drop_duplicates(subset=['类目', '商品ID', '月份'], keep='last')
df_goods_tf_rank = df_goods_tf_rank[
    ['类目', '月份', '商品ID', '商品信息', '店铺', '行业访客排名', '访客人数', '搜索人数', '交易金额']]

# print(df_goods_tf_rank.info())
# df_goods_tf_rank.to_csv('/home/rich/myfile/output/temp.csv')

# ----------------------------------------------------------------------------#
# --------------------1、品牌排行数据读取并且处理--------#
df_goods_ts_rank = read_csv_file(rootDir, '商品 高交易排行', missing_values, 0, coding='utf-8')
df_goods_ts_rank = df_goods_ts_rank.rename(
    columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
# # --提取文件路径和下载时间信息
df_goods_ts_rank.reset_index(inplace=True, drop=True)
df_goods_ts_rank.reset_index(inplace=True)
file_split1 = df_goods_ts_rank['文件名'].str.split('】', expand=True)
file_split1.reset_index(inplace=True)
file_split1.drop([1], axis=1, inplace=True)
file_split1.rename(columns={0: '类目'}, inplace=True)
df_goods_ts_rank = pd.merge(df_goods_ts_rank, file_split1, how='left', on='index')

# --字段处理
df_goods_ts_rank['类目'].replace([r'\s+', '【'], ['', ''], regex=True, inplace=True)
df_goods_ts_rank['行业排名'].replace(['持平', '升', '降'], ['-', '-', '-'], regex=True, inplace=True)
file_split01 = df_goods_ts_rank['行业排名'].str.split('-', expand=True)
file_split01.drop([1], axis=1, inplace=True)
file_split01.rename(columns={0: '排行'}, inplace=True)
file_split01.reset_index(inplace=True)
df_goods_ts_rank = pd.merge(df_goods_ts_rank, file_split01, how='left', on='index')
#
df_goods_ts_rank.rename(columns={'日期': '月份', '店铺名称': '店铺', '排行': '行业交易排名'}, inplace=True)
#
df_goods_ts_rank['月份'] = df_goods_ts_rank['月份'].str[0:10]
df_goods_ts_rank['支付转化率'].replace('%', '', regex=True, inplace=True)
# 格式转化
df_goods_ts_rank['月份'] = pd.to_datetime(df_goods_ts_rank['月份'])
#
df_goods_ts_rank['行业交易排名'] = df_goods_ts_rank['行业交易排名'].astype(int)
df_goods_ts_rank['支付转化率'] = df_goods_ts_rank['支付转化率'].astype(float) / 100
df_goods_ts_rank['交易金额'] = df_goods_ts_rank['交易金额'].astype(float)
# 保留最新的数据记录,去重复
df_goods_ts_rank.sort_values(by=['类目', '商品ID', '月份'], ascending=True, inplace=True)
df_goods_ts_rank = df_goods_ts_rank.reset_index(drop=True)
df_goods_ts_rank = df_goods_ts_rank.drop_duplicates(subset=['类目', '商品ID', '月份'], keep='last')
df_goods_ts_rank = df_goods_ts_rank[['类目', '月份', '商品ID', '商品信息', '店铺', '行业交易排名', '交易金额', '支付转化率']]

# print(df_goods_ts_rank.info())
# df_goods_ts_rank.to_csv('/home/rich/myfile/output/temp.csv')

# ----------------------------------------------------------------------------#
# --------------------1、品牌排行数据读取并且处理--------#
df_goods_ct_rank = read_csv_file(rootDir, '商品 高意向排行', missing_values, 0, coding='utf-8')
df_goods_ct_rank = df_goods_ct_rank.rename(
    columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
# # --提取文件路径和下载时间信息
df_goods_ct_rank.reset_index(inplace=True, drop=True)
df_goods_ct_rank.reset_index(inplace=True)
file_split1 = df_goods_ct_rank['文件名'].str.split('】', expand=True)
file_split1.reset_index(inplace=True)
file_split1.drop([1], axis=1, inplace=True)
file_split1.rename(columns={0: '类目'}, inplace=True)
df_goods_ct_rank = pd.merge(df_goods_ct_rank, file_split1, how='left', on='index')

# --字段处理
df_goods_ct_rank['类目'].replace([r'\s+', '【'], ['', ''], regex=True, inplace=True)
df_goods_ct_rank['行业排名'].replace(['持平', '升', '降'], ['-', '-', '-'], regex=True, inplace=True)
file_split01 = df_goods_ct_rank['行业排名'].str.split('-', expand=True)
file_split01.drop([1], axis=1, inplace=True)
file_split01.rename(columns={0: '排行'}, inplace=True)
file_split01.reset_index(inplace=True)
df_goods_ct_rank = pd.merge(df_goods_ct_rank, file_split01, how='left', on='index')
#
df_goods_ct_rank.rename(columns={'日期': '月份', '店铺名称': '店铺', '排行': '行业意向排名'}, inplace=True)
#
df_goods_ct_rank['月份'] = df_goods_ct_rank['月份'].str[0:10]
# 格式转化
df_goods_ct_rank['月份'] = pd.to_datetime(df_goods_ct_rank['月份'])
#
df_goods_ct_rank['行业意向排名'] = df_goods_ct_rank['行业意向排名'].astype(int)
df_goods_ct_rank['加购人数'] = df_goods_ct_rank['加购人数'].astype(int)
df_goods_ct_rank['交易金额'] = df_goods_ct_rank['交易金额'].astype(float)
# 保留最新的数据记录,去重复
df_goods_ct_rank.sort_values(by=['类目', '商品ID', '月份'], ascending=True, inplace=True)
df_goods_ct_rank = df_goods_ct_rank.reset_index(drop=True)
df_goods_ct_rank = df_goods_ct_rank.drop_duplicates(subset=['类目', '商品ID', '月份'], keep='last')
df_goods_ct_rank = df_goods_ct_rank[['类目', '月份', '商品ID', '商品信息', '店铺', '行业意向排名', '交易金额', '加购人数']]

# print(df_goods_ct_rank.info())
# df_goods_ct_rank.to_csv('/home/rich/myfile/output/temp.csv')

# ----------------------------------------------------------------------------#
# --------------------1、品牌排行数据读取并且处理--------#
df_competitor_zb = read_csv_file(rootDir, '竞品分析', missing_values, 0, coding='utf-8')
df_competitor_zb = df_competitor_zb.rename(
    columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
# --字段处理
df_competitor_zb = df_competitor_zb[
    ['月份', '商品ID', '商品信息', '交易金额', '访客人数', '搜索人数', '收藏人数', '加购人数',
     '支付人数', '支付件数', '客单价', '搜索占比', '支付转化率']]
df_competitor_zb['支付转化率'].replace('%', '', regex=True, inplace=True)
df_competitor_zb['搜索占比'].replace('%', '', regex=True, inplace=True)
# 格式转化
df_competitor_zb['月份'] = pd.to_datetime(df_competitor_zb['月份'])
#
df_competitor_zb['访客人数'] = df_competitor_zb['访客人数'].astype(int)
df_competitor_zb['搜索人数'] = df_competitor_zb['搜索人数'].astype(int)
df_competitor_zb['收藏人数'] = df_competitor_zb['收藏人数'].astype(int)
df_competitor_zb['支付人数'] = df_competitor_zb['支付人数'].astype(int)
df_competitor_zb['加购人数'] = df_competitor_zb['加购人数'].astype(int)
df_competitor_zb['支付件数'] = df_competitor_zb['支付件数'].astype(int)
df_competitor_zb['交易金额'] = df_competitor_zb['交易金额'].astype(float)
df_competitor_zb['客单价'] = df_competitor_zb['客单价'].astype(float)
df_competitor_zb['搜索占比'] = df_competitor_zb['搜索占比'].astype(float) / 100
df_competitor_zb['支付转化率'] = df_competitor_zb['支付转化率'].astype(float) / 100
# 保留最新的数据记录,去重复
df_competitor_zb.sort_values(by=['商品ID', '月份'], ascending=True, inplace=True)
df_competitor_zb = df_competitor_zb.reset_index(drop=True)
df_competitor_zb = df_competitor_zb.drop_duplicates(subset=['商品ID', '月份'], keep='last')

# print(df_competitor_zb.info())
# df_competitor_zb.to_csv('/home/rich/myfile/output/temp.csv')

# ----------------------------------------------------------------------------#
# --------------------1、品牌排行数据读取并且处理--------#
df_competitor_ly = read_csv_file(rootDir, '入店来源  竞品', missing_values, 0, coding='utf-8')
df_competitor_ly = df_competitor_ly.rename(
    columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
# --字段处理
df_competitor_ly = df_competitor_ly[
    ['日期', '商品ID', '流量来源', '交易金额', '访客人数', '支付人数', '客单价', '支付转化率']]
df_competitor_ly['支付转化率'].replace('%', '', regex=True, inplace=True)
df_competitor_ly.rename(columns={'日期': '月份'}, inplace=True)
df_competitor_ly['月份'] = df_competitor_ly['月份'].str[0:10]
# 格式转化
df_competitor_ly['月份'] = pd.to_datetime(df_competitor_ly['月份'])
#
df_competitor_ly['访客人数'] = df_competitor_ly['访客人数'].astype(int)
df_competitor_ly['支付人数'] = df_competitor_ly['支付人数'].astype(int)
df_competitor_ly['交易金额'] = df_competitor_ly['交易金额'].astype(float)
df_competitor_ly['客单价'] = df_competitor_ly['客单价'].astype(float)
df_competitor_ly['支付转化率'] = df_competitor_ly['支付转化率'].astype(float) / 100
# 保留最新的数据记录,去重复
df_competitor_ly.sort_values(by=['商品ID', '流量来源', '月份'], ascending=True, inplace=True)
df_competitor_ly = df_competitor_ly.reset_index(drop=True)
df_competitor_ly = df_competitor_ly.drop_duplicates(subset=['商品ID', '流量来源', '月份'], keep='last')

# print(df_competitor_ly.info())
# df_competitor_ly.to_csv('/home/rich/myfile/output/temp.csv')

# ----------------------------------------------------------------------------#
# ----------------------将数据导入数据库------#

df_cg_trends.to_sql(name='category_trends', con=engine, if_exists='append', index=False,
                    dtype=mapping_df_types(df_cg_trends))
df_brand_tf_rank.to_sql(name='brand_traffic_rank', con=engine, if_exists='append', index=False,
                        dtype=mapping_df_types(df_brand_tf_rank))
df_brand_ts_rank.to_sql(name='brand_transaction_rank', con=engine, if_exists='append', index=False,
                        dtype=mapping_df_types(df_brand_ts_rank))
df_goods_tf_rank.to_sql(name='goods_traffic_rank', con=engine, if_exists='append', index=False,
                        dtype=mapping_df_types(df_goods_tf_rank))
df_goods_ts_rank.to_sql(name='goods_transaction_rank', con=engine, if_exists='append', index=False,
                        dtype=mapping_df_types(df_goods_ts_rank))
df_goods_ct_rank.to_sql(name='goods_cart_rank', con=engine, if_exists='append', index=False,
                        dtype=mapping_df_types(df_goods_ct_rank))
df_competitor_zb.to_sql(name='competitor_index', con=engine, if_exists='append', index=False,
                        dtype=mapping_df_types(df_competitor_zb))
df_competitor_ly.to_sql(name='competitor_traffic', con=engine, if_exists='append', index=False,
                        dtype=mapping_df_types(df_competitor_ly))
# ----------------------------------------------------------------------------#

end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
