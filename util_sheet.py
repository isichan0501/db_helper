
# -*- coding:utf-8 -*-
"""
スプレッドシートを扱う
json sheet key fileとシートキーが必要


"""
import gspread
import pandas as pd
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pysnooper
from glob import glob
import loguru
from loguru import logger

import os
from dotenv import load_dotenv
import logging

lg = logging.getLogger(__name__)

# 環境変数を参照
load_dotenv()
SHEET_JSON_FILE = os.getenv('SHEET_JSON_FILE')
SHEET_KEY = os.getenv('SHEET_KEY')
SHEET_NAME = os.getenv('SHEET_NAME')

class Temple:

    def __init__(self, cnm):
        self.cnm = cnm
        self.sh = Temple.get_sheet()
        #self.tem_ple = self._get('Temple')

    @classmethod
    def get_sheet(cls):
        # token_dir = os.path.abspath(os.path.join(os.path.dirname(__file__),'token'))
        # tokens = glob('{}/*sheet.json'.format(token_dir))
        # file_path = random.choice(tokens)
        gc = gspread.service_account(SHEET_JSON_FILE)
        return gc.open_by_key(SHEET_KEY)

    def get_temple(self):
        cnm = self.cnm
        wks = self.sh.worksheet("Temple")
        list_of_dict = wks.get_all_records()
        tem_ples = [ld for ld in list_of_dict if ('cnm' in ld) and (ld['cnm'] == cnm)]
        return tem_ples[0]

    @classmethod
    def get_worksheet(cls, sheetname):
        sh = Temple.get_sheet()
        wks = sh.worksheet(sheetname)
        list_of_dict = wks.get_all_records()
        return list_of_dict

    #sheetnameシートのsearch_key=search_valのrowをreturn
    @classmethod
    def get_dict_from_list(cls, sheetname, search_key, search_val):
        list_of_dict = Temple.get_worksheet(sheetname)
        dict_list = [ld for ld in list_of_dict if (search_key in ld) and (ld[search_key] == search_val)]
        return dict_list


    #cell_valで検索してwrite_valで置き換え
    #@pysnooper.snoop()
    def change_cell(self, sheetname, cell_val, write_val):
        """
        site名指定でID削除

        Args:
            site (str): pc,hp,ik,jm
        """
        #ワークシート選択
        wks = self.sh.worksheet(sheetname)
        #cell.value, cell.row, cell.col
        cell = wks.find(cell_val)
        #wks.update_cell(cell.row, cell.col, write_val, value_input_option='USER_ENTERED')
        wks.update_cell(cell.row, cell.col, write_val)

    #row_val = pc,hp などサイト名, col_val = cnm('mika','eri'...etc)
    #@pysnooper.snoop()
    @classmethod
    def write_cell(cls, sheetname, row_val, col_val, write_val):
        #ワークシート選択
        sh = Temple.get_sheet()
        wks = sh.worksheet(sheetname)
        #row,colのリストを取得してindex+1のrow & col valueを取得
        # [補足]第2引数が2の場合、値ではなく数式を格納する - worksheet.row_values(1,2)
        row_list = wks.row_values(1)
        col_list = wks.col_values(1)
        cell_col = row_list.index(row_val) + 1
        cell_row = col_list.index(col_val) + 1
        #書き込み
        wks.update_cell(cell_row, cell_col, write_val)
        logger.debug(f'chagne cell sheetname:{sheetname}-{row_val}:{col_val}={write_val}')

    @classmethod
    def add_row(cls, sheetname, row_vals):
        #row_vals = list or dict
        #ワークシート選択
        sh = Temple.get_sheet()
        wks = sh.worksheet(sheetname)
        #書き込み.
        if isinstance(row_vals, list):
            wks.append_rows(row_vals)
        else:
            wks.append_row(row_vals)
        logger.debug('append row: {}'.format(row_vals))


#スプレッドシートからデータ取得用
@pysnooper.snoop()
def get_sheet_with_pd(sheetname=SHEET_NAME):
    try:
        gc = gspread.service_account(SHEET_JSON_FILE)
        sh = gc.open_by_key(SHEET_KEY)
        worksheet = sh.worksheet(sheetname)
        df = get_as_dataframe(worksheet, skiprows=0, header=0)
        return df
    except Exception as e:
        lg.exception(e)

@pysnooper.snoop()
def set_sheet_with_pd(sheetname, df):
    lg.debug(sheetname)
    try:
        gc = gspread.service_account(SHEET_JSON_FILE)
        sh = gc.open_by_key(SHEET_KEY)
        worksheet = sh.worksheet(sheetname)
        set_with_dataframe(worksheet, df)
        print("set df ok")
    except Exception as e:
        lg.exception(e)
    
    
# シート名、キャラ名,行の名前、書きこむ値が引数
@pysnooper.snoop()
def writeSheet(sheetname, col_name, row_name, input_val):
    gc = gspread.service_account(SHEET_JSON_FILE)
    sh = gc.open_by_key(SHEET_KEY)
    worksheet = sh.worksheet(sheetname)
    # 最初の列と列を読み込む
    row_num = worksheet.row_values(1)
    col_num = worksheet.col_values(1)
    # 読み込んだ最初の列の列番号のインデックス
    mycol = col_num.index(col_name) + 1
    myrow = row_num.index(row_name) + 1
    # 指定した行を出す
    worksheet.update_cell(mycol, myrow, input_val)


@pysnooper.snoop()
def change_cell(sheetname, cell_val, write_val):
    gc = gspread.service_account(SHEET_JSON_FILE)
    sh = gc.open_by_key(SHEET_KEY)
    worksheet = sh.worksheet(sheetname)
    #cell.value, cell.row, cell.col
    cell = worksheet.find(cell_val)
    #wks.update_cell(cell.row, cell.col, write_val, value_input_option='USER_ENTERED')
    worksheet.update_cell(cell.row, cell.col, write_val)