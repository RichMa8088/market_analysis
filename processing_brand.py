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
# print(pm_cg.info())  # 读取类目信息参数

pm_fq = read_xlxs_file(rootDir, '参数表', missing_values, '商品信息', 0, 0, coding='utf-8')
# print(pm_fq.info())  # 读取商品信息参数

pm_lb = read_xlxs_file(rootDir, '参数表', missing_values, '价格区间', 0, 0, coding='utf-8')
# print(pm_lb.info())  # 读取价格区间参数

pm_jp = read_xlxs_file(rootDir, '参数表', missing_values, '竞品信息', 0, 0, coding='utf-8')
# print(pm_jp.info())  # 读取竞品参数

pm_gjc = read_xlxs_file(rootDir, '参数表', missing_values, '竞品关键词', 0, 0, coding='utf-8')
# print(pm_gjc.info())  # 读取关键词参数

pm_gjc_lb = read_xlxs_file(rootDir, '参数表', missing_values, '关键词分类', 0, 0, coding='utf-8')
# print(pm_gjc_lb.info())  # 读取关键词参数

pm_ly = read_xlxs_file(rootDir, '参数表', missing_values, '流量分类表', 0, 0, coding='utf-8')
# print(pm_ly.info())  # 读取流量来源参数

pm_tp = read_xlxs_file(rootDir, '参数表', missing_values, 'top品牌', 0, 0, coding='utf-8')
# print(pm_tp.info())  # 读取流量来源参数


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
op_cg_trends = df_cg_trends.loc[
    df_cg_trends['周期'].notnull(), ['对应类目', '类目简称', '周期', '绘图月份', '月份', '访客数', '搜索人数', '支付人数',
                                   '交易金额', '客单价', '搜索占比']]

# print(df_cg_trends.info())

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
df_brand = pd.merge(df_brand, pm_tp, how='left', left_on="品牌", right_on="重点品牌")
df_brand = df_brand.loc[
    df_brand['周期'].notnull(), ['对应类目', '类目简称', '周期', '绘图月份', '月份', '品牌', '品牌分组', '访客人数',
                               '搜索人数', '支付转化率', '交易金额_x']]
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
op_brand = df_brand.loc[(df_brand['客单价'] > 1) & (df_brand['访客数'] > 1)]
# print(df_brand.info())

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
df_goods = pd.merge(df_goods, pm_fq, how='left', on='商品ID')
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
#
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
    ['支付转化率_x', '支付转化率_y', '加购率_x', '加购率_y', '加购人数', '加购人数', '类目', '类目简称_y',
     '商品信息_y', '类目', '采集类目'], axis=1, inplace=True)
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
                         '商品信息_x': '商品信息', 0: '客单区间'}, inplace=True)
# print(df_goods.info())
# --------------------1、提取竞品数据-------------------------#
sql_competitor_id = 'SELECT 月份,商品ID,访客人数,搜索人数,加购人数,支付人数,支付件数,' \
                    '交易金额 FROM market_analysis.competitor_index'
df_competitor_id = pd.read_sql_query(sql=sql_competitor_id, con=conn, coerce_float=True,
                                     parse_dates=None)
df_competitor_id = df_competitor_id.loc[df_competitor_id['交易金额'] > 0]
# 去重复
df_competitor_id.sort_values(by=['月份', '商品ID'], ascending=True, inplace=True)
df_competitor_id = df_competitor_id.reset_index(drop=True)
df_competitor_id = df_competitor_id.drop_duplicates(subset=['月份', '商品ID'], keep='last')

# 合并参数信息
op_competitor_id = pd.merge(df_competitor_id, pm_jp, how='left', on='商品ID')
op_competitor_id.rename(columns={'访客人数': '访客数', '类别': '自定义类别'}, inplace=True)
# print(df_competitor_id.info())

# --------------------1、提取竞品数据-------------------------#
sql_competitor_tf = 'SELECT 月份,商品ID,流量来源,访客人数,支付人数,' \
                    '交易金额 FROM market_analysis.competitor_traffic'
df_competitor_tf = pd.read_sql_query(sql=sql_competitor_tf, con=conn, coerce_float=True,
                                     parse_dates=None)
df_competitor_tf = df_competitor_tf.loc[df_competitor_tf['访客人数'] > 0]
# 去重复
df_competitor_tf.sort_values(by=['月份', '商品ID', '流量来源'], ascending=True, inplace=True)
df_competitor_tf = df_competitor_tf.reset_index(drop=True)
df_competitor_tf = df_competitor_tf.drop_duplicates(subset=['月份', '商品ID', '流量来源'], keep='last')

# 合并参数信息
df_competitor_tf = pd.merge(df_competitor_tf, pm_jp, how='left', on='商品ID')
op_competitor_tf = pd.merge(df_competitor_tf, pm_ly, how='left', on='流量来源')
op_competitor_tf.rename(columns={'访客人数': '访客数', '类别': '自定义类别'}, inplace=True)
# print(df_competitor_tf.info())

# --------------------1、提取关键词数据-------------------------#
sql_keywords_rk = 'SELECT 类目,起始日期,关键词,搜索人数,点击人数,' \
                  '支付人数 FROM market_analysis.keywords_rank'
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
op_keywords_rk.drop(['类目', '采集类目'], axis=1, inplace=True)
# print(df_keywords_rk.info())

# --------------------1、提取关键词数据-------------------------#
sql_keywords_td = 'SELECT 月份,关键词,搜索人数,点击人数,' \
                  '支付人数,交易金额 FROM market_analysis.keywords_traffic'
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
# print(df_keywords_td.info())

# --导出excel到本地
writer = pd.ExcelWriter('/home/rich/myfile/output/汇总输出.xlsx')
op_cg_trends.to_excel(writer, sheet_name='类目趋势', header=True, index=False)
op_brand.to_excel(writer, sheet_name='类目品牌榜', header=True, index=False)
op_goods.to_excel(writer, sheet_name='类目商品榜', header=True, index=False)
op_keywords_rk.to_excel(writer, sheet_name='类目热搜词', header=True, index=False)
op_keywords_td.to_excel(writer, sheet_name='类目核心词趋势', header=True, index=False)
op_competitor_id.to_excel(writer, sheet_name='竞品指标', header=True, index=False)
op_competitor_tf.to_excel(writer, sheet_name='竞品流量来源', header=True, index=False)
writer.save()

##############################
end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
