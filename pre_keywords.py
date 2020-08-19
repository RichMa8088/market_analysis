# -*- coding:utf-8 -*-
# --加载
import pandas as pd
from read_csv_file import read_csv_file


# ----------------------------------------------------------------------------#
def pre_keywords_trends(file_dir, file_name_key, missing_values, header_rows, coding):
    # ------
    df_terms_td = read_csv_file(file_dir, file_name_key, missing_values, header_rows, coding)
    df_terms_td = df_terms_td.rename(
        columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
    if len(df_terms_td) == 0:
        return pd.DataFrame()
    else:
        # --字段处理
        df_terms_td = df_terms_td[['月份', '关键词', '搜索人数', '搜索次数', '点击率', '点击人数',
                                   '点击次数', '交易金额', '支付人数', '支付转化率', '客单价']]
        df_terms_td['支付转化率'].replace('%', '', regex=True, inplace=True)
        df_terms_td['点击率'].replace('%', '', regex=True, inplace=True)
        # 格式转化
        df_terms_td['月份'] = pd.to_datetime(df_terms_td['月份'])
        df_terms_td['搜索人数'] = df_terms_td['搜索人数'].astype(int)
        df_terms_td['搜索次数'] = df_terms_td['搜索次数'].astype(int)
        df_terms_td['点击人数'] = df_terms_td['点击人数'].astype(int)
        df_terms_td['点击次数'] = df_terms_td['点击次数'].astype(int)
        df_terms_td['支付人数'] = df_terms_td['支付人数'].astype(int)
        df_terms_td['交易金额'] = df_terms_td['交易金额'].astype(float)
        df_terms_td['客单价'] = df_terms_td['客单价'].astype(float)
        df_terms_td['支付转化率'] = df_terms_td['支付转化率'].astype(float) / 100
        df_terms_td['点击率'] = df_terms_td['点击率'].astype(float) / 100
        # 去重复
        df_terms_td.sort_values(by=['关键词', '月份'], ascending=True, inplace=True)
        df_terms_td = df_terms_td.reset_index(drop=True)
        df_terms_td = df_terms_td.drop_duplicates(subset=['关键词', '月份'], keep='last')
    return df_terms_td


# ----------------------------------------------------------------------------#
def pre_keywords_rank(file_dir, file_name_key, missing_values, header_rows, coding):
    # ------
    df_terms_rank = read_csv_file(file_dir, file_name_key, missing_values, header_rows, coding)
    df_terms_rank = df_terms_rank.rename(
        columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
    if len(df_terms_rank) == 0:
        return pd.DataFrame()
    else:
        # --提取文件路径
        df_terms_rank.reset_index(inplace=True, drop=True)
        df_terms_rank.reset_index(inplace=True)
        file_split = df_terms_rank['文件名'].str.split('】', expand=True)
        file_split.reset_index(inplace=True)
        file_split.drop([1], axis=1, inplace=True)
        file_split.rename(columns={0: '类目'}, inplace=True)
        df_terms_rank = pd.merge(df_terms_rank, file_split, how='left', on='index')

        # --字段处理
        df_terms_rank = df_terms_rank[['类目', '日期', '关键词', '排名', '搜索人数', '点击率', '点击人数',
                                       '支付人数', '支付转化率']]
        df_terms_rank['支付转化率'].replace('%', '', regex=True, inplace=True)
        df_terms_rank['点击率'].replace('%', '', regex=True, inplace=True)
        df_terms_rank['类目'].replace([r'\s+', '【'], ['', ''], regex=True, inplace=True)
        df_terms_rank.rename(columns={'日期': '起始日期', '排名': '热搜词排名'}, inplace=True)
        df_terms_rank['起始日期'] = df_terms_rank['起始日期'].str[0:10]
        # 格式转化
        df_terms_rank['起始日期'] = pd.to_datetime(df_terms_rank['起始日期'])
        #
        df_terms_rank['搜索人数'] = df_terms_rank['搜索人数'].astype(int)
        df_terms_rank['热搜词排名'] = df_terms_rank['热搜词排名'].astype(int)
        df_terms_rank['点击人数'] = df_terms_rank['点击人数'].astype(int)
        df_terms_rank['支付人数'] = df_terms_rank['支付人数'].astype(int)
        df_terms_rank['支付转化率'] = df_terms_rank['支付转化率'].astype(float) / 100
        df_terms_rank['点击率'] = df_terms_rank['点击率'].astype(float) / 100
        # 去重复
        df_terms_rank.sort_values(by=['类目', '关键词', '起始日期'], ascending=True, inplace=True)
        df_terms_rank = df_terms_rank.reset_index(drop=True)
        df_terms_rank = df_terms_rank.drop_duplicates(subset=['类目', '关键词', '起始日期'], keep='last')

    return df_terms_rank
