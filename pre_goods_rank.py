# -*- coding:utf-8 -*-
# --加载
import pandas as pd
from read_csv_file import read_csv_file


# ----------------------------------------------------------------------------#
def pre_goods_tf_rank(file_dir, file_name_key, missing_values, header_rows, coding):
    # ------
    df_goods_tf_rank = read_csv_file(file_dir, file_name_key, missing_values, header_rows, coding)
    df_goods_tf_rank = df_goods_tf_rank.rename(
        columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
    if len(df_goods_tf_rank) == 0:
        return pd.DataFrame()
    else:
        # --提取文件路径
        df_goods_tf_rank.reset_index(inplace=True, drop=True)
        df_goods_tf_rank.reset_index(inplace=True)
        file_split = df_goods_tf_rank['文件名'].str.split('】', expand=True)
        file_split.reset_index(inplace=True)
        file_split.drop([1], axis=1, inplace=True)
        file_split.rename(columns={0: '类目'}, inplace=True)
        df_goods_tf_rank = pd.merge(df_goods_tf_rank, file_split, how='left', on='index')
        # --字段处理
        df_goods_tf_rank['行业排名'].replace(['持平', '升', '降'], ['-', '-', '-'], regex=True,
                                         inplace=True)
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
        df_goods_tf_rank['行业访客排名'] = df_goods_tf_rank['行业访客排名'].astype(int)
        df_goods_tf_rank['访客人数'] = df_goods_tf_rank['访客人数'].astype(int)
        df_goods_tf_rank['搜索人数'] = df_goods_tf_rank['搜索人数'].astype(int)
        df_goods_tf_rank['交易金额'] = df_goods_tf_rank['交易金额'].astype(float)
        # 去重复
        df_goods_tf_rank.sort_values(by=['类目', '商品ID', '月份'], ascending=True, inplace=True)
        df_goods_tf_rank = df_goods_tf_rank.reset_index(drop=True)
        df_goods_tf_rank = df_goods_tf_rank.drop_duplicates(subset=['类目', '商品ID', '月份'],
                                                            keep='last')
        df_goods_tf_rank = df_goods_tf_rank[
            ['类目', '月份', '商品ID', '商品信息', '店铺', '行业访客排名', '访客人数', '搜索人数', '交易金额']]

    return df_goods_tf_rank


# ----------------------------------------------------------------------------#
def pre_goods_ts_rank(file_dir, file_name_key, missing_values, header_rows, coding):
    # ----------------------------------------------------------------------------#
    # --------------------1、品牌排行数据读取并且处理--------#
    df_goods_ts_rank = read_csv_file(file_dir, file_name_key, missing_values, header_rows, coding)
    df_goods_ts_rank = df_goods_ts_rank.rename(
        columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
    if len(df_goods_ts_rank) == 0:
        return pd.DataFrame()
    else:
        # --提取文件路径
        df_goods_ts_rank.reset_index(inplace=True, drop=True)
        df_goods_ts_rank.reset_index(inplace=True)
        file_split1 = df_goods_ts_rank['文件名'].str.split('】', expand=True)
        file_split1.reset_index(inplace=True)
        file_split1.drop([1], axis=1, inplace=True)
        file_split1.rename(columns={0: '类目'}, inplace=True)
        df_goods_ts_rank = pd.merge(df_goods_ts_rank, file_split1, how='left', on='index')
        # --字段处理
        df_goods_ts_rank['类目'].replace([r'\s+', '【'], ['', ''], regex=True, inplace=True)
        df_goods_ts_rank['行业排名'].replace(['持平', '升', '降'], ['-', '-', '-'], regex=True,
                                         inplace=True)
        file_split01 = df_goods_ts_rank['行业排名'].str.split('-', expand=True)
        file_split01.drop([1], axis=1, inplace=True)
        file_split01.rename(columns={0: '排行'}, inplace=True)
        file_split01.reset_index(inplace=True)
        df_goods_ts_rank = pd.merge(df_goods_ts_rank, file_split01, how='left', on='index')
        df_goods_ts_rank.rename(columns={'日期': '月份', '店铺名称': '店铺', '排行': '行业交易排名'}, inplace=True)
        df_goods_ts_rank['月份'] = df_goods_ts_rank['月份'].str[0:10]
        df_goods_ts_rank['支付转化率'].replace('%', '', regex=True, inplace=True)
        # 格式转化
        df_goods_ts_rank['月份'] = pd.to_datetime(df_goods_ts_rank['月份'])
        df_goods_ts_rank['行业交易排名'] = df_goods_ts_rank['行业交易排名'].astype(int)
        df_goods_ts_rank['支付转化率'] = df_goods_ts_rank['支付转化率'].astype(float) / 100
        df_goods_ts_rank['交易金额'] = df_goods_ts_rank['交易金额'].astype(float)
        # 去重复
        df_goods_ts_rank.sort_values(by=['类目', '商品ID', '月份'], ascending=True, inplace=True)
        df_goods_ts_rank = df_goods_ts_rank.reset_index(drop=True)
        df_goods_ts_rank = df_goods_ts_rank.drop_duplicates(subset=['类目', '商品ID', '月份'],
                                                            keep='last')
        df_goods_ts_rank = df_goods_ts_rank[
            ['类目', '月份', '商品ID', '商品信息', '店铺', '行业交易排名', '交易金额', '支付转化率']]

    return df_goods_ts_rank


# ----------------------------------------------------------------------------#
def pre_goods_ct_rank(file_dir, file_name_key, missing_values, header_rows, coding):
    # ----------------------------------------------------------------------------#
    # --------------------1、品牌排行数据读取并且处理--------#
    df_goods_ct_rank = read_csv_file(file_dir, file_name_key, missing_values, header_rows, coding)
    df_goods_ct_rank = df_goods_ct_rank.rename(
        columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
    if len(df_goods_ct_rank) == 0:
        return pd.DataFrame()
    else:
        # --提取文件路径
        df_goods_ct_rank.reset_index(inplace=True, drop=True)
        df_goods_ct_rank.reset_index(inplace=True)
        file_split1 = df_goods_ct_rank['文件名'].str.split('】', expand=True)
        file_split1.reset_index(inplace=True)
        file_split1.drop([1], axis=1, inplace=True)
        file_split1.rename(columns={0: '类目'}, inplace=True)
        df_goods_ct_rank = pd.merge(df_goods_ct_rank, file_split1, how='left', on='index')
        # --字段处理
        df_goods_ct_rank['类目'].replace([r'\s+', '【'], ['', ''], regex=True, inplace=True)
        df_goods_ct_rank['行业排名'].replace(['持平', '升', '降'], ['-', '-', '-'], regex=True,
                                         inplace=True)
        file_split01 = df_goods_ct_rank['行业排名'].str.split('-', expand=True)
        file_split01.drop([1], axis=1, inplace=True)
        file_split01.rename(columns={0: '排行'}, inplace=True)
        file_split01.reset_index(inplace=True)
        df_goods_ct_rank = pd.merge(df_goods_ct_rank, file_split01, how='left', on='index')
        df_goods_ct_rank.rename(columns={'日期': '月份', '店铺名称': '店铺', '排行': '行业意向排名'}, inplace=True)
        df_goods_ct_rank['月份'] = df_goods_ct_rank['月份'].str[0:10]
        df_goods_ct_rank['月份'] = pd.to_datetime(df_goods_ct_rank['月份'])
        df_goods_ct_rank['行业意向排名'] = df_goods_ct_rank['行业意向排名'].astype(int)
        df_goods_ct_rank['加购人数'] = df_goods_ct_rank['加购人数'].astype(int)
        df_goods_ct_rank['交易金额'] = df_goods_ct_rank['交易金额'].astype(float)
        # 去重复
        df_goods_ct_rank.sort_values(by=['类目', '商品ID', '月份'], ascending=True, inplace=True)
        df_goods_ct_rank = df_goods_ct_rank.reset_index(drop=True)
        df_goods_ct_rank = df_goods_ct_rank.drop_duplicates(subset=['类目', '商品ID', '月份'],
                                                            keep='last')
        df_goods_ct_rank = df_goods_ct_rank[
            ['类目', '月份', '商品ID', '商品信息', '店铺', '行业意向排名', '交易金额', '加购人数']]

    return df_goods_ct_rank
