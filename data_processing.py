# -*- coding:utf-8 -*-
# --------------------0、加载模块及初始化参数--------#
# --加载常规模块
from time import time
import pandas as pd
#
from sqlalchemy import create_engine  # 引擎
from sqlalchemy.ext.declarative import declarative_base  # 基类
from sqlalchemy import Column  # sqlalchemy类型
from sqlalchemy.dialects.mysql import BIGINT, BOOLEAN, CHAR, DATE, DATETIME, DECIMAL, DOUBLE, \
    FLOAT, INTEGER, MEDIUMINT, SMALLINT, TIME, TIMESTAMP, TINYINT, VARCHAR, YEAR  # mysql类型
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
# print(pm_date.info()) #读取日期范围参数
'''
# --------------------1、提取类目趋势数据-------------------------#
sql_cg_trends = 'SELECT 类目名,月份,访客数,搜索人数,加购人数,支付人数,' \
                '交易金额 FROM market_analysis.category_trends'
df_cg_trends = pd.read_sql_query(sql=sql_cg_trends, con=conn, coerce_float=True, parse_dates=None)
df_cg_trends = df_cg_trends.loc[df_cg_trends['交易金额'] > 0]

# 去重复
df_cg_trends.sort_values(by=['类目名', '月份'], ascending=True, inplace=True)
df_cg_trends = df_cg_trends.reset_index(drop=True)
df_cg_trends = df_cg_trends.drop_duplicates(subset=['类目名', '月份'], keep='last')
# 增加新字段，四舍五入
df_cg_trends['客单价'] = df_cg_trends['交易金额'] / df_cg_trends['支付人数']
df_cg_trends['搜索占比'] = df_cg_trends['搜索人数'] - df_cg_trends['访客数']
rd_decimals = pd.Series([1, 4], index=['客单价', '搜索占比'])
df_cg_trends.round(rd_decimals)

print(df_cg_trends.info())







# --------------------2、提取淘客中有用信息-------------------------#
select_taoke = 'SELECT 商品ID,来源或淘客昵称,团长名称,计划名称,淘客结算时间,淘宝父订单编号,淘宝子订单编号,' \
               '实际成交价格,佣金,服务费金额 FROM taoke_info'
df_select_taoke = pd.read_sql_query(sql=select_taoke, con=conn, coerce_float=True, parse_dates=None)
df_select_taoke.rename(columns={'来源或淘客昵称': '淘客昵称', '服务费金额': '服务费', '淘宝子订单编号': '子订单号',
                                '淘宝父订单编号': '订单号', '团长名称': '团长'},
                       inplace=True)
df_select_taoke.sort_values(by=['子订单号', '淘客结算时间'], ascending=True, inplace=True)
df_select_taoke = df_select_taoke.reset_index(drop=True)
df_last_taoke = df_select_taoke.drop_duplicates(subset=['子订单号'], keep='last')
# print(df_last_taoke.info())
# --------------------3、提取淘客维权中有用信息-------------------------#
select_taoke_refund = 'SELECT 维权退款金额,应退回服务费,应退回佣金,维权完成时间,淘宝订单编号,' \
                      '淘宝子订单编号 FROM taoke_refund'
df_select_taoke_refund = pd.read_sql_query(sql=select_taoke_refund, con=conn, coerce_float=True,
                                           parse_dates=None)
df_select_taoke_refund.rename(columns={'淘宝子订单编号': '子订单号', '淘宝订单编号': '订单号'}, inplace=True)
df_select_taoke_refund.sort_values(by=['子订单号', '维权完成时间'], ascending=True, inplace=True)
df_select_taoke_refund = df_select_taoke_refund.reset_index(drop=True)
df_last_taoke_refund = df_select_taoke_refund.drop_duplicates(subset=['子订单号'], keep='last')
# print(df_last_taoke_refund.info())

# 合并淘客及淘客维权信息
df_update_taoke = pd.merge(df_last_taoke, df_last_taoke_refund, on="子订单号", how="left")
fna_values = {'退款金额': 0, '实际成交价格': 0, '佣金': 0, '服务费': 0, '维权退款金额': 0,
              '应退回服务费': 0, '应退回佣金': 0, '优惠金额': 0, 'SKUID': '-'}
df_update_taoke = df_update_taoke.fillna(value=fna_values)
df_update_taoke['淘客成交金额'] = df_update_taoke['实际成交价格'] - df_update_taoke['维权退款金额']
df_update_taoke['淘客佣金'] = df_update_taoke['佣金'] - df_update_taoke['应退回佣金']
df_update_taoke['淘客服务费'] = df_update_taoke['服务费'] - df_update_taoke['应退回服务费']
df_update_taoke['子订单号'] = df_update_taoke['子订单号'].str.replace('	', '')
df_update_taoke.drop(['维权完成时间', '实际成交价格', '维权退款金额', '佣金', '应退回佣金', '服务费',
                      '应退回服务费', '订单号_y', '订单号_x', '商品ID'], axis=1, inplace=True)
# print(df_update_taoke.info())

# ------------------3、读取优惠信息
df_coupon_info = read_csv_file_to_dataframe(rootDir, 'coupon', missing_values, 0, coding='utf-8')
df_coupon_info['购物券金额'] = df_coupon_info['购物券金额'].astype(float)
df_coupon_info = df_coupon_info[['订单号', '优惠券信息', '优惠券类型', '优惠分组', '购物券类型', '购物券金额']]
# print(df_coupon_info.info())
df_taoke_info = read_csv_file_to_dataframe(rootDir, 'taoke_info', missing_values, 0, coding='utf-8')
df_taoke_info.drop(['路径', '文件名'], axis=1, inplace=True)
# print(df_taoke_info.info())

# ------------------4、读取地理信息
df_geo_info = read_csv_file_to_dataframe(rootDir, 'geo_city', missing_values, 0, coding='utf-8')
df_geo_info['标识b'] = df_geo_info['省份'] + df_geo_info['城市']
df_geo_info.drop(['路径', '文件名', '省编号', '行政区域', '省份', '城市', 'tableau城市'], axis=1, inplace=True)
# print(df_geo_info.info())
df_geo_un = read_csv_file_to_dataframe(rootDir, 'city_un', missing_values, 0, coding='utf-8')
df_coupon_info['购物券金额'] = df_coupon_info['购物券金额'].astype(float)
df_geo_un.drop(['路径', '文件名', '省份', '省管市'], axis=1, inplace=True)
# print(df_geo_un.info())

# ------------------5、读取商品信息
df_product = read_csv_file_to_dataframe(rootDir, 'product', missing_values, 0, coding='utf-8')
df_product['标识c'] = df_product['商品ID'] + df_product['SKU ID']
df_product.drop(['路径', '文件名', '商品ID', 'SKU ID', '商品SKU'], axis=1, inplace=True)
# print(df_product.info())

# --汇总
df_grouped = pd.merge(df_last_orders, df_update_taoke, on="子订单号", how="left")
df_grouped = pd.merge(df_grouped, df_coupon_info, on="订单号", how="left")
df_grouped = pd.merge(df_grouped, df_taoke_info, on="淘客昵称", how="left")
df_grouped['是否付款'] = df_grouped['付款时间'].notnull()
df_grouped['是否淘客'] = df_grouped['淘客结算时间'].notnull()
df_grouped['是否发货'] = df_grouped['子订单发货时间'].notnull()
df_grouped['是否退款'] = df_grouped['退款金额'].notnull()
df_grouped['是否完结'] = df_grouped['交易结束时间'].notnull()
df_grouped['是否使用购物券'] = df_grouped['购物券类型'].notnull()
df_grouped.replace(True, "是", inplace=True)
df_grouped.replace(False, "否", inplace=True)
df_grouped.drop(['下载时间', '标价', '运费', '计划名称', '买家实际支付总金额'], axis=1, inplace=True)
df_order_new = df_grouped.loc[df_grouped['优惠分组'].isnull(), ['订单号', '拍下时间', '优惠详情']]
# --淘客信息处理
df_tk = df_grouped.loc[df_grouped['淘客结算时间'].notnull(), ['淘客昵称', '团长', '淘客成交金额']]
df_tk.sort_values(by=['淘客昵称', '团长', '淘客成交金额'], ascending=True, inplace=True)
df_tk_N = df_tk.drop_duplicates(subset=['淘客昵称'], keep='last')
# --地理信息处理
df_grouped['标识a'] = df_grouped['省'] + df_grouped['区']
df_grouped = pd.merge(df_grouped, df_geo_un, on="标识a", how="left")
df1 = pd.DataFrame(df_grouped.loc[df_grouped['对应省'].isnull()])
df1.drop(['对应省', '对应市'], axis=1, inplace=True)
df0 = pd.DataFrame(df_grouped.loc[df_grouped['对应省'].notnull()])
df0.drop(['省', '市'], axis=1, inplace=True)
df0.rename(columns={'对应省': '省', '对应市': '市'}, inplace=True)

df_order1 = pd.concat([df1, df0], axis=0, ignore_index=True)
df_order1['标识a'] = df_order1['省'] + df_order1['市']
df_order1 = pd.merge(df_order1, df_geo_un, on="标识a", how="left")
df11 = pd.DataFrame(df_order1.loc[df_order1['对应省'].isnull()])
df11.drop(['对应省', '对应市'], axis=1, inplace=True)
df01 = pd.DataFrame(df_order1.loc[df_order1['对应省'].notnull()])
df01.drop(['省', '市'], axis=1, inplace=True)
df01.rename(columns={'对应省': '省', '对应市': '市'}, inplace=True)
df_order = pd.concat([df11, df01], axis=0, ignore_index=True)
df_order['标识b'] = df_order['省'] + df_order['市']
df_order = pd.merge(df_order, df_geo_info, on="标识b", how="left")
df_order['人口'] = df_order['人口'].astype(float)
df_order['纬度'] = df_order['纬度'].astype(float)
df_order['经度'] = df_order['经度'].astype(float)

# --商品信息处理
fna_values = {'SKUID': '-'}
df_order = df_order.fillna(value=fna_values)
df_order['标识c'] = df_order['商品ID'] + df_order['SKUID']
df_order = pd.merge(df_order, df_product, on="标识c", how="left")
df_order_gn = df_order.loc[df_order['省份简称'].isnull(), ['省', '市', '区', '订单号']]
df_order_pn = df_order.loc[df_order['商品分组'].isnull(), ['商品ID', 'SKUID', '订单号']]
df_order['组合件数'] = df_order['组合件数'].astype(float)
df_order.drop(['标识a', '标识b', '标识c', '优惠详情', '人口'], axis=1, inplace=True)
# --去重复并且排序
df_order.sort_values(by=['拍下时间', '订单号', '子订单号'], ascending=True, inplace=True)
df_order_b = df_order.drop_duplicates(subset=['订单号'], keep='last')
df_order = pd.merge(df_order, df_order_b[['子订单号', '购物券金额']], on="子订单号", how="left")
df_order = df_order.reset_index(drop=True)
df_order.drop(['购物券金额_x'], axis=1, inplace=True)
df_order.rename(columns={'购物券金额_y': '购物券金额'}, inplace=True)
print(df_order.info())

# --导出excel到本地
writer = pd.ExcelWriter('/home/rich/File/result/订单汇总.xlsx')
df_tk_N.to_excel(writer, sheet_name='淘客', header=True, index=False)
df_order_new.to_excel(writer, sheet_name='新订单', header=True, index=False)
df_order_gn.to_excel(writer, sheet_name='新省市', header=True, index=False)
df_order_pn.to_excel(writer, sheet_name='新商品', header=True, index=False)
writer.save()
df_order.to_csv('/home/rich/File/result/order.csv')
end_time = time()  # 计时结束
print('运行时长： %f' % (end_time - start_time))  # 打印运行时长
'''
