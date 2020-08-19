# -*- coding:utf-8 -*-
# --加载
import pandas as pd
from read_csv_file import read_csv_file


# ----------------------------------------------------------------------------#
def pre_category_trends(file_dir, file_name_key, missing_values, header_rows, coding):
    # ------
    df_cg_trends = read_csv_file(file_dir, file_name_key, missing_values, header_rows, coding)
    df_cg_trends = df_cg_trends.rename(
        columns=lambda x: x.replace("'", "").replace('"', '').replace(" ", ""))
    if len(df_cg_trends) == 0:
        return pd.DataFrame()
    else:
        # --字段处理
        df_cg_trends.drop(['终端', '类别', '客单价', '路径', '文件名'], axis=1, inplace=True)
        df_cg_trends.rename(columns={'类目名': '类目'}, inplace=True)
        df_cg_trends['类目'].replace([r'\s+', '>'], ['', '_'], regex=True, inplace=True)
        # 格式转化
        df_cg_trends['月份'] = pd.to_datetime(df_cg_trends['月份'])
        df_cg_trends['搜索人数'] = df_cg_trends['搜索人数'].astype(int)
        df_cg_trends['搜索次数'] = df_cg_trends['搜索次数'].astype(int)
        df_cg_trends['访客数'] = df_cg_trends['访客数'].astype(int)
        df_cg_trends['浏览量'] = df_cg_trends['浏览量'].astype(int)
        df_cg_trends['收藏人数'] = df_cg_trends['收藏人数'].astype(int)
        df_cg_trends['收藏次数'] = df_cg_trends['收藏次数'].astype(int)
        df_cg_trends['加购人数'] = df_cg_trends['加购人数'].astype(int)
        df_cg_trends['加购次数'] = df_cg_trends['加购次数'].astype(int)
        df_cg_trends['支付人数'] = df_cg_trends['支付人数'].astype(int)
        df_cg_trends['交易金额'] = df_cg_trends['交易金额'].astype(float)
        # 去重复
        df_cg_trends.sort_values(by=['类目', '月份'], ascending=True, inplace=True)
        df_cg_trends = df_cg_trends.reset_index(drop=True)
        df_cg_trends = df_cg_trends.drop_duplicates(subset=['类目', '月份'], keep='last')
    return df_cg_trends
