# -*- coding:utf-8 -*-
# -----------------------------------0、加载模块及初始化参数---------------------------------------#
# --加载常规模块
import os  # 文件路径
import pandas as pd

'''
pandas.read_excel(io,    #路径
                  sheet_name=0,    #工作表名称
                  header=0,    #标题所在行，如果没有标题，None
                  skiprows=None,    #开头跳过的行
                  na_values=None,    #缺失值
                  thousands=None,    #千位符
                  encoding='ISO-8859-1'，    #编码
                  **kwds)
'''


# --读取xlxs文件，根据关键字进行筛选，将选中的数据放入一个df
def read_xlxs_file(file_dir, file_name_key, na_values, sheetname, header_rows, skiprows, coding):
    df_xlxs = pd.DataFrame()
    for parent, DirNames, FileNames in os.walk(file_dir):
        for FileName in FileNames:
            if FileName.__contains__('.xlxs'):
                if FileName.__contains__(file_name_key):
                    xlxs1 = pd.read_excel(os.path.join(parent, FileName), sheet_name=sheetname,
                                          encoding=coding, skiprows=skiprows,
                                          dtype=object, header=header_rows,
                                          na_values=na_values)
                    df_xlxs = df_xlxs.append(xlxs1)
                else:
                    continue
            else:
                continue
    df_xlxs.dropna(axis=0, how='all', inplace=True)
    return df_xlxs
