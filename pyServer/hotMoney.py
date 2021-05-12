# coding=utf8
import requests
import json
import pymysql
import threading
from time import sleep
from threading import Timer
import pandas as pd
import numpy as np
import heapq
import array
import logging
from operator import itemgetter
import datetime
import os.path
from RepeatedTimer import RepeatedTimer
import schedule
import time
import json
from datetime import datetime
from csv import writer
import pytse_client as tse
from pytse_client import download_client_types_records, all_symbols

token = '6e6671c1fcc42c94bf448fe7d880fa88'

today = datetime.today().strftime('%Y-%m-%d')


def populateDatabase(dbname, tbname, table_list, flag):
    dbName = dbname
    tableName = tbname
    values = ""
    for idx, val in enumerate(table_list):
        if idx > 0:
            values = values + ","
        if flag == 1:
            values = values + "('" + val["symbol"] + "','" + str(val["vol"]) + "','" + str(val["percent"]) + "')"
        if flag == 2:
            values = values + "('" + val["symbol"] + "','" + str(val["close"]) + "','" + str(
                val["closeP"]) + "','" + str(
                val["percent"]) + "')"
        if flag == 3:
            values = values + "('" + val["symbol"] + "','" + str(val["close"]) + "','" + str(val["closeP"]) + "')"
        if flag == 4:
            values = values + "('" + str(val["name"]) + "','" + str(val["market"]) + "','" + str(val["instance_code"]) \
                     + "','" + str(val["namad_code"]) + "','" + str(val["industry_code"]) \
                     + "','" + str(val["industry"]) + "','" + str(val["state"]) \
                     + "','" + str(val["full_name"]) + "','" + str(val["first_price"]) \
                     + "','" + str(val["yesterday_price"]) + "','" + str(val["close_price"]) \
                     + "','" + str(val["close_price_change"]) + "','" + str(val["close_price_change_percent"]) \
                     + "','" + str(val["final_price"]) + "','" + str(val["final_price_change"]) \
                     + "','" + str(val["final_price_change_percent"]) + "','" + str(val["eps"]) \
                     + "','" + str(val["free_float"]) + "','" + str(val["highest_price"]) \
                     + "','" + str(val["lowest_price"]) + "','" + str(val["daily_price_high"]) \
                     + "','" + str(val["daily_price_low"]) + "','" + str(val["P:E"]) \
                     + "','" + str(val["trade_number"]) + "','" + str(val["trade_volume"]) \
                     + "','" + str(val["trade_value"]) + "','" + str(val["all_stocks"]) \
                     + "','" + str(val["basis_volume"]) + "','" + str(val["real_buy_volume"]) \
                     + "','" + str(val["co_buy_volume"]) + "','" + str(val["real_sell_volume"]) \
                     + "','" + str(val["co_sell_volume"]) + "','" + str(val["real_buy_value"]) \
                     + "','" + str(val["co_buy_value"]) + "','" + str(val["real_sell_value"]) \
                     + "','" + str(val["co_sell_value"]) + "','" + str(val["real_buy_count"]) \
                     + "','" + str(val["co_buy_count"]) + "','" + str(val["real_sell_count"]) \
                     + "','" + str(val["co_sell_count"]) + "','" + str(val["1_sell_count"]) \
                     + "','" + str(val["2_sell_count"]) + "','" + str(val["3_sell_count"]) \
                     + "','" + str(val["1_buy_count"]) + "','" + str(val["2_buy_count"]) \
                     + "','" + str(val["3_buy_count"]) + "','" + str(val["1_sell_price"]) \
                     + "','" + str(val["2_sell_price"]) + "','" + str(val["3_sell_price"]) \
                     + "','" + str(val["1_buy_price"]) + "','" + str(val["2_buy_price"]) \
                     + "','" + str(val["3_buy_price"]) + "','" + str(val["1_sell_volume"]) \
                     + "','" + str(val["2_sell_volume"]) + "','" + str(val["3_sell_volume"]) \
                     + "','" + str(val["1_buy_volume"]) + "','" + str(val["2_buy_volume"]) \
                     + "','" + str(val["3_buy_volume"]) + "','" + str(val["market_value"]) + "')"
        if flag == 5:
            values = values + "('" + str(val["name"]) + "','" + str(val["full_name"]) + "','" + str(
                val["instance_code"]) + "','" + str(val["namad_code"]) + "')"
        if flag == 6:
            values = values + "('" + str(val["slug"]) + "','" + str(val["name"]) + "','" + str(
                val["price"]) + "','" + str(val["minPrice"]) + "','" + str(val["maxPrice"]) + "','" + str(
                val["time"]) + "')"
        if flag == 7:
            values = values + "('" + str(val["symbol"]) + "','" + str(val["name"]) + "','" + str(
                val["price"]) + "','" + str(val["change_percent_24h"]) + "','" + str(val["volume_24h"]) + \
                     "','" + str(val["market_cap"]) + "')"
        if flag == 8:
            values = values + "('" + str(val["model"]) + "','" + str(val["type"]) + "','" + str(
                val["price"]) + "','" + str(val["market_price"]) + "','" + str(val["last_update"]) + "')"
        if flag == 9:
            values = values + "('" + str(val["state"]) + "','" + str(val["b_index"]) + "','" + str(
                val["index_change"]) + "','" + str(val["index_change_percent"]) + "','" \
                     + str(val["index_h"]) + "','" + str(val["index_h_change"]) + "','" + str(
                val["index_h_change_percent"]) + "','" + str(val["market_value"]) \
                     + "','" + str(val["trade_number"]) + "','" + str(val["trade_value"]) + "','" + str(
                val["trade_volume"]) + "')"
        if flag == 10:
            values = values + "('" + str(val["time"]) + "','" + str(val["name"]) + "','" + str(
                val["full_name"]) + "','" + str(val["close"]) + "','" \
                     + str(val["percent"]) + "','" + str(val["number"]) + "','" + str(val["average"]) + "','" + str(
                val["total"]) \
                     + "','" + str(val["attribute"]) + "','" + str(val["type"]) + "')"

    # print(values)
    if len(table_list) > 0:
        connection = pymysql.connect(host='194.5.175.58',  # 194.5.175.58   localhost
                                     user='root',
                                     password='Hadi2150008140@$&!',  # Hadi2150008140@$&!   root
                                     database=dbName,
                                     port=3306,
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection:
            if tbname != "hot_money":
                with connection.cursor() as cursor:
                    sql = "DELETE FROM " + tableName
                    cursor.execute(sql, args=None)
                connection.commit()

            with connection.cursor() as cursor:
                sql = "INSERT INTO " + tableName + " VALUES " + values + ";"
                cursor.execute("SET CHARACTER SET utf8", args=None)
                cursor.execute(sql, args=None)
            connection.commit()


def is_non_zero_file(fpath):
    return os.path.isfile(fpath) and os.path.getsize(fpath) > 0


def lastChanges():
    try:
        resp = requests.get(
            'https://sourcearena.ir/api/?token=' + token + '&all&type=0')  # 6e6671c1fcc42c94bf448fe7d880fa88&all&type=0')
        print("lastChanges ", resp.status_code)
        if resp.status_code == 200:
            data = json.loads(resp.text)
            return data
    except:
        logging.exception("Error")

    return None


saveData = None


def hotMoney(dataA):
    global saveData
    if saveData is None:
        saveData = dataA
    else:
        try:
            hotMoneyList = []
            for idx, val in enumerate(dataA):
                value = int(dataA[idx]['real_buy_value']) - int(saveData[idx]['real_buy_value'])
                if saveData[idx]['real_buy_count'] is not None and dataA[idx]['real_buy_count'] is not None and value > 2000000000 and saveData[idx]['real_buy_count'] != "0" and \
                        dataA[idx]['real_buy_count'] != "0":
                    print(value)
                    count = int(dataA[idx]['real_buy_count']) - int(saveData[idx]['real_buy_count'])
                    if count > 0:
                        average = value / count
                        cell = {"time": datetime.now().strftime('%H-%M-%S'), "name": dataA[idx]['name'],
                                "full_name": dataA[idx]['full_name'],
                                "close": dataA[idx]['close_price'], "percent": dataA[idx]['close_price_change_percent'],
                                "average": average, "total": value, "number": count, "attribute": 1, "type": 1}
                        hotMoneyList.append(cell)

                value = int(dataA[idx]['real_sell_value']) - int(saveData[idx]['real_sell_value'])
                if saveData[idx]['real_sell_count'] is not None and dataA[idx]['real_sell_count'] is not None and value > 2000000000 and saveData[idx]['real_sell_count'] != "0" and dataA[idx]['real_sell_count'] != "0":
                    count = int(dataA[idx]['real_sell_count']) - int(saveData[idx]['real_sell_count'])
                    if count > 0:
                        average = value / count
                        cell = {"time": datetime.now().strftime('%H-%M-%S'), "name": dataA[idx]['name'],
                                "full_name": dataA[idx]['full_name'],
                                "close": dataA[idx]['close_price'], "percent": dataA[idx]['close_price_change_percent'],
                                "average": average, "total": value, "number": count, "attribute": 1, "type": 2}
                        hotMoneyList.append(cell)

            saveData = dataA
            if hotMoneyList:
                print(hotMoneyList)
                populateDatabase("price", "hot_money", hotMoneyList, 10)
        except:
            logging.exception("Error")


def appendNewLineToCsv(file_name, list_of_elem, isUpdate):
    if isUpdate:
        with open(file_name, 'a+', newline='') as write_obj:
            csv_writer = writer(write_obj)
            csv_writer.writerow(list_of_elem)
    else:
        with open(file_name, 'w', newline='') as write_obj:
            csv_writer = writer(write_obj)
            csv_writer.writerow(list_of_elem)


def readCsv(json):
    try:
        fileNameTicker = 'tickers_data/' + json['name'] + '.csv'
        fileNameVolume = 'client_types_data/' + json['name'] + '.csv'

        row_contents = [datetime.now().strftime('%Y-%m-%d'), json['yesterday_price'], json['highest_price'],
                        json['lowest_price'], json['close_price'], json['trade_value'], json['trade_volume'],
                        json['trade_number'], json['final_price'], datetime.now().strftime('%Y-%m-%d')]

        if (json['real_buy_count'] is not None and json['real_buy_count'] != "0") and (
                json['real_sell_count'] is not None and json['real_sell_count'] != "0"):
            real_buy_mean_price = int(json['real_buy_value'] / int(json['real_buy_count']))
            real_sell_mean_price = int(json['real_sell_value'] / int(json['real_sell_count']))
            if int(json['co_buy_count']) > 0:
                co_buy_mean_price = int(json['co_buy_value'] / int(json['co_buy_count']))
            else:
                co_buy_mean_price = 0

            if int(json['co_sell_count']) > 0:
                co_sell_mean_price = int(json['co_sell_value'] / int(json['co_sell_count']))
            else:
                co_sell_mean_price = 0

            row_contentsVolume = [datetime.now().strftime('%Y-%m-%d'), json['real_buy_count'], json['co_buy_count'],
                                  json['real_sell_count'], json['co_sell_count'], json['real_buy_volume'],
                                  json['co_buy_volume'],
                                  json['real_sell_volume'], json['co_sell_volume'], json['real_buy_value'],
                                  json['co_buy_value'],
                                  json['real_sell_value'], json['co_sell_value'], json['co_sell_volume'],
                                  real_buy_mean_price,
                                  real_sell_mean_price, co_buy_mean_price, co_sell_mean_price]

            if os.path.isfile(fileNameTicker) and is_non_zero_file(fileNameTicker):
                # print(fileNameTicker)
                df = pd.read_csv(fileNameTicker, index_col=False, low_memory=False, error_bad_lines=False)

                now = datetime.strptime(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
                past = datetime.strptime(str(df.iloc[-1]['date']), '%Y-%m-%d')

                if now > past:
                    # print("create ticker row")
                    appendNewLineToCsv(fileNameTicker, row_contents, True)
                elif now == past:
                    # if len(df) == 1:
                    #     columns = ['date', 'open', 'high', 'low', 'adjClose', 'value', 'volume', 'count', 'close', 'jdate']
                    #     appendNewLineToCsv(fileNameTicker, columns, True)
                    # else:
                    df.iloc[-1, df.columns.get_loc('date')] = row_contents[0]
                    df.iloc[-1, df.columns.get_loc('open')] = row_contents[1]
                    df.iloc[-1, df.columns.get_loc('high')] = row_contents[2]
                    df.iloc[-1, df.columns.get_loc('low')] = row_contents[3]
                    df.iloc[-1, df.columns.get_loc('adjClose')] = row_contents[4]
                    df.iloc[-1, df.columns.get_loc('value')] = row_contents[5]
                    df.iloc[-1, df.columns.get_loc('volume')] = row_contents[6]
                    df.iloc[-1, df.columns.get_loc('count')] = row_contents[7]
                    df.iloc[-1, df.columns.get_loc('close')] = row_contents[8]
                    df.iloc[-1, df.columns.get_loc('jdate')] = row_contents[9]

                    # df.drop(df.tail(-1).index, inplace=True)
                    # appendNewLineToCsv(fileNameTicker, row_contents, True)
                    df.to_csv(fileNameTicker, index=False)
                    # print("update ticker row")
            else:
                columns = ['date', 'open', 'high', 'low', 'adjClose', 'value', 'volume', 'count', 'close', 'jdate']
                appendNewLineToCsv(fileNameTicker, columns, True)
                appendNewLineToCsv(fileNameTicker, row_contents, True)
                # print("exist create ticker row")

            if os.path.isfile(fileNameVolume) and is_non_zero_file(fileNameVolume):
                # print(fileNameVolume)
                df = pd.read_csv(fileNameVolume, index_col=False, low_memory=False, error_bad_lines=False)

                now = datetime.strptime(datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
                past = datetime.strptime(str(df.iloc[-1]['date']), '%Y-%m-%d')

                if now > past:
                    # print("create volume row")
                    appendNewLineToCsv(fileNameVolume, row_contentsVolume, True)

                elif now == past:
                    df.iloc[-1, df.columns.get_loc('date')] = row_contentsVolume[0]
                    df.iloc[-1, df.columns.get_loc('individual_buy_count')] = row_contentsVolume[1]
                    df.iloc[-1, df.columns.get_loc('corporate_buy_count')] = row_contentsVolume[2]
                    df.iloc[-1, df.columns.get_loc('individual_sell_count')] = row_contentsVolume[3]
                    df.iloc[-1, df.columns.get_loc('corporate_sell_count')] = row_contentsVolume[4]
                    df.iloc[-1, df.columns.get_loc('individual_buy_vol')] = row_contentsVolume[5]
                    df.iloc[-1, df.columns.get_loc('corporate_buy_vol')] = row_contentsVolume[6]
                    df.iloc[-1, df.columns.get_loc('individual_sell_vol')] = row_contentsVolume[7]
                    df.iloc[-1, df.columns.get_loc('corporate_sell_vol')] = row_contentsVolume[8]
                    df.iloc[-1, df.columns.get_loc('individual_buy_value')] = row_contentsVolume[9]
                    df.iloc[-1, df.columns.get_loc('corporate_buy_value')] = row_contentsVolume[10]
                    df.iloc[-1, df.columns.get_loc('individual_sell_value')] = row_contentsVolume[11]
                    df.iloc[-1, df.columns.get_loc('corporate_sell_value')] = row_contentsVolume[12]
                    df.iloc[-1, df.columns.get_loc('individual_buy_mean_price')] = row_contentsVolume[14]
                    df.iloc[-1, df.columns.get_loc('individual_sell_mean_price')] = row_contentsVolume[15]
                    df.iloc[-1, df.columns.get_loc('corporate_buy_mean_price')] = row_contentsVolume[16]
                    df.iloc[-1, df.columns.get_loc('corporate_sell_mean_price')] = row_contentsVolume[17]
                    df.iloc[-1, df.columns.get_loc('individual_ownership_change')] = row_contentsVolume[
                        13]  # co_sell_volume

                    df.to_csv(fileNameVolume, index=False)
                    # print("update volume row")
            else:
                columns = ['date', 'individual_buy_count', 'corporate_buy_count', 'individual_sell_count',
                           'corporate_sell_count',
                           'individual_buy_vol', 'corporate_buy_vol', 'individual_sell_vol',
                           'corporate_sell_vol', 'individual_buy_value', 'corporate_buy_value', 'individual_sell_value',
                           'corporate_sell_value', 'individual_buy_mean_price', 'individual_sell_mean_price',
                           'corporate_buy_mean_price', 'corporate_sell_mean_price', 'individual_ownership_change',
                           'jdate']
                appendNewLineToCsv(fileNameVolume, columns, True)
                appendNewLineToCsv(fileNameVolume, row_contentsVolume, True)
                # print("exist create volume row")
    except:
        logging.exception("Error")


def historyVolume(dataA):
    for idx, val in enumerate(dataA):
        if dataA[idx]['name'] == 'فایرا':
            readCsv(dataA[idx])


def detectVolume():
    dataA = lastChanges()
    if dataA is not None:
        hotMoney(dataA)
        # historyVolume(dataA)
        lastList = []
        for data in dataA:
            cell = {"name": data['name'], "market": data['market'], "instance_code": data['instance_code'],
                    "namad_code": data['namad_code'], "industry_code": data['industry_code'],
                    "industry": data['industry'], "state": data['state'],
                    "full_name": data['full_name'],
                    "first_price": data['first_price'], "yesterday_price": data['yesterday_price'],
                    "close_price": data['close_price'],
                    "close_price_change": data['close_price_change'],
                    "close_price_change_percent": str(data['close_price_change_percent']).replace("%", ""),
                    "final_price": data['final_price'],
                    "final_price_change": data['final_price_change'],
                    "final_price_change_percent": str(data['final_price_change_percent']).replace("%", ""),
                    "eps": data['eps'],
                    "free_float": data['free_float'], "highest_price": data['highest_price'],
                    "lowest_price": data['lowest_price'],
                    "daily_price_high": data['daily_price_high'], "daily_price_low": data['daily_price_low'],
                    "P:E": data['P:E'],
                    "trade_number": data['trade_number'], "trade_volume": data['trade_volume'],
                    "trade_value": data['trade_value'],
                    "all_stocks": data['all_stocks'], "basis_volume": data['basis_volume'],
                    "real_buy_volume": data['real_buy_volume'],
                    "co_buy_volume": data['co_buy_volume'], "real_sell_volume": data['real_sell_volume'],
                    "co_sell_volume": data['co_sell_volume'], "real_buy_value": data['real_buy_value'],
                    "co_buy_value": data['co_buy_value'], "real_sell_value": data['real_sell_value'],
                    "co_sell_value": data['co_sell_value'], "real_buy_count": data['real_buy_count'],
                    "co_buy_count": data['co_buy_count'], "real_sell_count": data['real_sell_count'],
                    "co_sell_count": data['co_sell_count'],
                    "1_sell_count": data['1_sell_count'], "2_sell_count": data['2_sell_count'],
                    "3_sell_count": data['3_sell_count'],
                    "1_buy_count": data['1_buy_count'], "2_buy_count": data['2_buy_count'],
                    "3_buy_count": data['3_buy_count'], "1_sell_price": data['1_sell_price'],
                    "2_sell_price": data['2_sell_price'],
                    "3_sell_price": data['3_sell_price'],
                    "1_buy_price": data['1_buy_price'],
                    "2_buy_price": data['2_buy_price'], "3_buy_price": data['3_buy_price'],
                    "1_sell_volume": data['1_sell_volume'],
                    "2_sell_volume": data['2_sell_volume'], "3_sell_volume": data['3_sell_volume'],
                    "1_buy_volume": data['1_buy_volume'],
                    "2_buy_volume": data['2_buy_volume'], "3_buy_volume": data['3_buy_volume'],
                    "market_value": data['market_value'],
                    }
            lastList.append(cell)  # (1366, "Incorrect integer value: 'None' for column 'industry_code' at row 1")
        if lastList:
            populateDatabase('price', 'last_price', lastList, 4)


def all_stocks():
    with open('../data.json') as json_file:
        dataA = json.load(json_file)
        print(dataA[0]['name'])
    allStocks = []
    for data in dataA:
        cell = {"name": data['name'], "full_name": data['full_name'],
                "instance_code": data['instance_code'], "namad_code": data['namad_code']
                }
        allStocks.append(cell)
        if allStocks:
            populateDatabase('temp', 'all_stocks', allStocks, 5)

def max_Volume_buy():
        buy10 = []
        buy20 = []
        buy30 = []
        buy45 = []
        buy60 = []
        buy10Ind = []
        buy20Ind = []
        buy30Ind = []

        for symbol in all_symbols():
            # tick = tse.Ticker(symbol)
            # df = tick.client_types
                fileNameTicker = 'tickers_data/' + symbol + '.csv'
                fileNameVolume = 'client_types_data/' + symbol + '.csv'
                if os.path.isfile(fileNameVolume) and os.path.isfile(fileNameTicker):
                    ticker = pd.read_csv(fileNameTicker, index_col=False, low_memory=False, error_bad_lines=False)
                    df = pd.read_csv(fileNameVolume, index_col=False, low_memory=False, error_bad_lines=False)

                    if not ticker.empty and ticker.size > 2:
                        if ticker.iloc[-1].close is not None and df['individual_buy_vol'].size > 1 and today == df['date'].iloc[
                            -1]:
                            maxNow = int(df['individual_buy_vol'].iloc[-1]) + int(df['corporate_buy_vol'].iloc[-1])
                            max10 = int(max(df['individual_buy_vol'][-10:-1] + df['corporate_buy_vol'][-10:-1]))
                            max20 = int(max(df['individual_buy_vol'][-20:-1] + df['corporate_buy_vol'][-20:-1]))
                            max30 = int(max(df['individual_buy_vol'][-30:-1] + df['corporate_buy_vol'][-30:-1]))
                            max45 = int(max(df['individual_buy_vol'][-45:-1] + df['corporate_buy_vol'][-45:-1]))
                            max60 = int(max(df['individual_buy_vol'][-60:-1] + df['corporate_buy_vol'][-60:-1]))

                            maxNowIndividual = int(df['individual_buy_vol'].iloc[-1])
                            max10Individual = int(max(df['individual_buy_vol'][-10:-1]))
                            max20Individual = int(max(df['individual_buy_vol'][-20:-1]))
                            max30Individual = int(max(df['individual_buy_vol'][-30:-1]))

                            if maxNow > max10:
                                percent = (maxNow - max10) / maxNow
                                y10 = {"symbol": symbol, "vol": maxNow, "percent": float("{:.2f}".format(round(percent, 2)))}
                                buy10.append(y10)

                            if maxNow > max20:
                                percent = (maxNow - max20) / maxNow
                                y20 = {"symbol": symbol, "vol": maxNow, "percent": float("{:.2f}".format(round(percent, 2)))}
                                buy20.append(y20)

                            if maxNow > max30:
                                percent = (maxNow - max30) / maxNow
                                y30 = {"symbol": symbol, "vol": maxNow, "percent": float("{:.2f}".format(round(percent, 2)))}
                                buy30.append(y30)

                            if maxNow > max45:
                                percent = (maxNow - max45) / maxNow
                                y45 = {"symbol": symbol, "vol": maxNow, "percent": float("{:.2f}".format(round(percent, 2)))}
                                buy45.append(y45)

                            if maxNow > max60:
                                percent = (maxNow - max60) / maxNow
                                y60 = {"symbol": symbol, "vol": maxNow, "percent": float("{:.2f}".format(round(percent, 2)))}
                                buy60.append(y60)

                            if maxNowIndividual > max10Individual:
                                percent = (maxNowIndividual - max10Individual) / maxNow
                                y10Individual = {"symbol": symbol, "vol": maxNowIndividual,
                                                 "percent": float("{:.2f}".format(round(percent, 2)))}
                                buy10Ind.append(y10Individual)

                            if maxNowIndividual > max20Individual:
                                percent = (maxNowIndividual - max20Individual) / maxNow
                                y20Individual = {"symbol": symbol, "vol": maxNowIndividual,
                                                 "percent": float("{:.2f}".format(round(percent, 2)))}
                                buy20Ind.append(y20Individual)

                            if maxNowIndividual > max30Individual:
                                percent = (maxNowIndividual - max30Individual) / maxNow
                                y30Individual = {"symbol": symbol, "vol": maxNowIndividual,
                                                 "percent": float("{:.2f}".format(round(percent, 2)))}
                                buy30Ind.append(y30Individual)

        return buy10, buy20, buy30, buy45, buy60, buy10Ind, buy20Ind, buy30Ind


def max_Volume_sell():
    sell10 = []
    sell20 = []
    sell30 = []
    sell45 = []
    sell60 = []
    sell10Ind = []
    sell20Ind = []
    sell30Ind = []

    for symbol in all_symbols():
        fileNameTicker = 'tickers_data/' + symbol + '.csv'
        fileNameVolume = 'client_types_data/' + symbol + '.csv'
        if os.path.isfile(fileNameTicker) and os.path.isfile(fileNameVolume):
            ticker = pd.read_csv(fileNameTicker, index_col=False, low_memory=False, error_bad_lines=False)
            df = pd.read_csv(fileNameVolume, index_col=False, low_memory=False, error_bad_lines=False)
            df = df.astype({"individual_buy_vol": int})
            df = df.astype({"individual_buy_count": int})
            df = df.astype({"corporate_buy_vol": int})
            df = df.astype({"corporate_buy_count": int})
            df = df.astype({"individual_ownership_change": int})
            df = df.astype({"corporate_sell_vol": int})
            df = df.astype({"individual_sell_vol": int})

            if not ticker.empty and ticker.size > 2:
                if ticker.iloc[-1].close is not None and df['individual_buy_vol'].size > 1 and today == df['date'].iloc[
                    -1]:
                    maxNowSell = int(df['individual_sell_vol'].iloc[-1]) + int(df['corporate_sell_vol'].iloc[-1])
                    max10Sell = int(max(df['individual_sell_vol'][-10:-1] + df['corporate_sell_vol'][-10:-1]))
                    max20Sell = int(max(df['individual_sell_vol'][-20:-1] + df['corporate_sell_vol'][-20:-1]))
                    max30Sell = int(max(df['individual_sell_vol'][-30:-1] + df['corporate_sell_vol'][-30:-1]))
                    max45Sell = int(max(df['individual_sell_vol'][-45:-1] + df['corporate_sell_vol'][-45:-1]))
                    max60Sell = int(max(df['individual_sell_vol'][-60:-1] + df['corporate_sell_vol'][-60:-1]))

                    maxNowIndividualSell = int(df['individual_sell_vol'].iloc[-1])
                    max10IndividualSell = int(max(df['individual_sell_vol'][-10:-1]))
                    max20IndividualSell = int(max(df['individual_sell_vol'][-20:-1]))
                    max30IndividualSell = int(max(df['individual_sell_vol'][-30:-1]))

                    if maxNowSell > max10Sell:
                        percent = (maxNowSell - max10Sell) / maxNowSell
                        y10Sell = {"symbol": symbol, "vol": maxNowSell,
                                   "percent": float("{:.2f}".format(round(percent, 2)))}
                        sell10.append(y10Sell)

                    if maxNowSell > max20Sell:
                        percent = (maxNowSell - max20Sell) / maxNowSell
                        y20Sell = {"symbol": symbol, "vol": maxNowSell,
                                   "percent": float("{:.2f}".format(round(percent, 2)))}
                        sell20.append(y20Sell)

                    if maxNowSell > max30Sell:
                        percent = (maxNowSell - max30Sell) / maxNowSell
                        y30Sell = {"symbol": symbol, "vol": maxNowSell,
                                   "percent": float("{:.2f}".format(round(percent, 2)))}
                        sell30.append(y30Sell)

                    if maxNowSell > max45Sell:
                        percent = (maxNowSell - max45Sell) / maxNowSell
                        y45Sell = {"symbol": symbol, "vol": maxNowSell,
                                   "percent": float("{:.2f}".format(round(percent, 2)))}
                        sell45.append(y45Sell)

                    if maxNowSell > max60Sell:
                        percent = (maxNowSell - max60Sell)  / maxNowSell
                        y60Sell = {"symbol": symbol, "vol": maxNowSell,
                                   "percent": float("{:.2f}".format(round(percent, 2)))}
                        sell60.append(y60Sell)

                    if maxNowIndividualSell > max10IndividualSell:
                        percent = (maxNowIndividualSell - max10IndividualSell) / maxNowSell
                        y10IndividualSell = {"symbol": symbol, "vol": maxNowIndividualSell,
                                             "percent": float("{:.2f}".format(round(percent, 2)))}
                        sell10Ind.append(y10IndividualSell)

                    if maxNowIndividualSell > max20IndividualSell:
                        percent = (maxNowIndividualSell - max20IndividualSell) / maxNowSell
                        y20IndividualSell = {"symbol": symbol, "vol": maxNowIndividualSell,
                                             "percent": float("{:.2f}".format(round(percent, 2)))}
                        sell20Ind.append(y20IndividualSell)

                    if maxNowIndividualSell > max30IndividualSell:
                        percent = (maxNowIndividualSell - max30IndividualSell) / maxNowSell
                        y30IndividualSell = {"symbol": symbol, "vol": maxNowIndividualSell,
                                             "percent": float("{:.2f}".format(round(percent, 2)))}
                        sell30Ind.append(y30IndividualSell)

    return sell10, sell20, sell30, sell45, sell60, sell10Ind, sell20Ind, sell30Ind


def pushMaxBuy():
    maxListBuy = max_Volume_buy()

    max_Volume_buyFrom10 = maxListBuy[0]
    max_Volume_buyFrom20 = maxListBuy[1]
    max_Volume_buyFrom30 = maxListBuy[2]
    max_Volume_buyFrom45 = maxListBuy[3]
    max_Volume_buyFrom60 = maxListBuy[4]
    max_Individual_Volume_buy10 = maxListBuy[5]
    max_Individual_Volume_buy20 = maxListBuy[6]
    max_Individual_Volume_buy30 = maxListBuy[7]

    max_Volume_buyFrom10.sort(key=itemgetter('percent'), reverse=True)
    max_Volume_buyFrom20.sort(key=itemgetter('percent'), reverse=True)
    max_Volume_buyFrom30.sort(key=itemgetter('percent'), reverse=True)
    max_Volume_buyFrom45.sort(key=itemgetter('percent'), reverse=True)
    max_Volume_buyFrom60.sort(key=itemgetter('percent'), reverse=True)
    max_Individual_Volume_buy10.sort(key=itemgetter('percent'), reverse=True)
    max_Individual_Volume_buy20.sort(key=itemgetter('percent'), reverse=True)
    max_Individual_Volume_buy30.sort(key=itemgetter('percent'), reverse=True)

    populateDatabase("temp", "max_Volume_buyFrom10", max_Volume_buyFrom10, 1)
    populateDatabase("temp", "max_Volume_buyFrom20", max_Volume_buyFrom20, 1)
    populateDatabase("temp", "max_Volume_buyFrom30", max_Volume_buyFrom30, 1)
    populateDatabase("temp", "max_Volume_buyFrom45", max_Volume_buyFrom45, 1)
    populateDatabase("temp", "max_Volume_buyFrom60", max_Volume_buyFrom60, 1)
    populateDatabase("temp", "max_Individual_Volume_buy10", max_Individual_Volume_buy10, 1)
    populateDatabase("temp", "max_Individual_Volume_buy20", max_Individual_Volume_buy20, 1)
    populateDatabase("temp", "max_Individual_Volume_buy30", max_Individual_Volume_buy30, 1)


def pushMaxSell():
    maxListSell = max_Volume_sell()

    max_Volume_sellFrom10 = maxListSell[0]
    max_Volume_sellFrom20 = maxListSell[1]
    max_Volume_sellFrom30 = maxListSell[2]
    max_Volume_sellFrom45 = maxListSell[3]
    max_Volume_sellFrom60 = maxListSell[4]
    max_Individual_Volume_sell10 = maxListSell[5]
    max_Individual_Volume_sell20 = maxListSell[6]
    max_Individual_Volume_sell30 = maxListSell[7]

    max_Volume_sellFrom10.sort(key=itemgetter('percent'), reverse=True)
    max_Volume_sellFrom20.sort(key=itemgetter('percent'), reverse=True)
    max_Volume_sellFrom30.sort(key=itemgetter('percent'), reverse=True)
    max_Volume_sellFrom45.sort(key=itemgetter('percent'), reverse=True)
    max_Volume_sellFrom60.sort(key=itemgetter('percent'), reverse=True)
    max_Individual_Volume_sell10.sort(key=itemgetter('percent'), reverse=True)
    max_Individual_Volume_sell20.sort(key=itemgetter('percent'), reverse=True)
    max_Individual_Volume_sell30.sort(key=itemgetter('percent'), reverse=True)

    populateDatabase("temp", "max_Volume_sellFrom10", max_Volume_sellFrom10, 1)
    populateDatabase("temp", "max_Volume_sellFrom20", max_Volume_sellFrom20, 1)
    populateDatabase("temp", "max_Volume_sellFrom30", max_Volume_sellFrom30, 1)
    populateDatabase("temp", "max_Volume_sellFrom45", max_Volume_sellFrom45, 1)
    populateDatabase("temp", "max_Volume_sellFrom60", max_Volume_sellFrom60, 1)
    populateDatabase("temp", "max_Individual_Volume_sell10", max_Individual_Volume_sell10, 1)
    populateDatabase("temp", "max_Individual_Volume_sell20", max_Individual_Volume_sell20, 1)
    populateDatabase("temp", "max_Individual_Volume_sell30", max_Individual_Volume_sell30, 1)


def possibleQueueBuy():
    possibleBuy = []
    for symbol in all_symbols():
        fileNameTicker = 'tickers_data/' + symbol + '.csv'
        fileNameVolume = 'client_types_data/' + symbol + '.csv'
        if os.path.isfile(fileNameTicker) and os.path.isfile(fileNameVolume):
            ticker = pd.read_csv(fileNameTicker, index_col=False, low_memory=False, error_bad_lines=False)
            df = pd.read_csv(fileNameVolume, index_col=False, low_memory=False, error_bad_lines=False)
            if ticker['close'].iloc[-1] is not None and today == df['date'].iloc[-1]:
                if ticker['close'].iloc[-1] > ticker['adjClose'].iloc[-1]:
                    percent = (ticker['close'].iloc[-1] - ticker['adjClose'].iloc[-1]) * 100 / ticker['adjClose'].iloc[-1]
                    if percent > 3:
                        cell = {"symbol": symbol,
                                "close": ticker['close'].iloc[-1],
                                "closeP": ticker['adjClose'].iloc[-1],
                                "percent": float("{:.2f}".format(round(percent, 2)))}
                        possibleBuy.append(cell)

    return possibleBuy


def possibleQueueSell():
    possibleSell = []
    for symbol in all_symbols():
        fileNameTicker = 'tickers_data/' + symbol + '.csv'
        fileNameVolume = 'client_types_data/' + symbol + '.csv'
        if os.path.isfile(fileNameTicker) and os.path.isfile(fileNameVolume):
            ticker = pd.read_csv(fileNameTicker, index_col=False, low_memory=False, error_bad_lines=False)
            df = pd.read_csv(fileNameVolume, index_col=False, low_memory=False, error_bad_lines=False)
            if ticker['close'].iloc[-1] is not None and today == df['date'].iloc[-1]:
                if ticker['adjClose'].iloc[-1]> ticker['close'].iloc[-1]:
                    percent = (ticker['adjClose'].iloc[-1] - ticker['close'].iloc[-1]) * 100 / ticker['close'].iloc[-1]
                    if percent > 3:
                        cell = {"symbol": symbol,
                                "close": ticker['close'].iloc[-1],
                                "closeP": ticker['adjClose'].iloc[-1],
                                "percent": float("{:.2f}".format(round(percent, 2)))}
                        possibleSell.append(cell)

    return possibleSell


def pushPossibleQueueBuy():
    pushPossibleQBuy = possibleQueueBuy()
    pushPossibleQBuy.sort(key=itemgetter('percent'), reverse=True)
    if pushPossibleQBuy:
        populateDatabase("temp", "possibleQueueBuy", pushPossibleQBuy, 2)


def pushPossibleQueueSell():
    pushPossibleQSell = possibleQueueSell()
    pushPossibleQSell.sort(key=itemgetter('percent'), reverse=True)
    if pushPossibleQSell:
        populateDatabase("temp", "possibleQueueSell", pushPossibleQSell, 2)


def currency():
    resp = requests.get(
        'https://sourcearena.ir/api/?token=' + token + '& currency')
    print("currency ", resp.status_code)
    dataA = json.loads(resp.text)
    if resp.status_code == 200:
        allCurrency = []
        for data in dataA['data']:
            cell = {"slug": data["slug"], "name": data["name"], "price": data["price"], "minPrice": data["min_price"],
                    "maxPrice": data["max_price"], "time": data["jalali_last_update"]}
            allCurrency.append(cell)
            if allCurrency:
                populateDatabase('temp', 'currency', allCurrency, 6)


def car():
    resp = requests.get(
        'https://sourcearena.ir/api/?token=' + token + '&car=all')
    print("car ", resp.status_code)
    if resp.status_code == 200:
        dataA = json.loads(resp.text)
        # print(dataA)

        carList = []
        for data in dataA:
            cell = {"model": data["model"], "type": data["type"], "price": data["price"],
                    "market_price": data["market_price"], "last_update": data["last_update"]}
            carList.append(cell)
            if carList:
                populateDatabase('temp', 'car', carList, 8)


def digital_currency():
    resp = requests.get(
        'https://sourcearena.ir/api/?token=' + token + '&crypto_v2=all')
    # print("digital_currency ", resp.status_code)
    if resp.status_code == 200:
        dataA = json.loads(resp.text)
        # print(dataA)

        allDCurrency = []
        for data in dataA["data"]:
            cell = {"symbol": data["symbol"], "name": data["name"], "price": data["price"],
                    "change_percent_24h": data["change_percent_24h"], "volume_24h": data["volume_24h"],
                    "market_cap": data["market_cap"]}
            allDCurrency.append(cell)
            if allDCurrency:
                populateDatabase('temp', 'digital_currency', allDCurrency, 7)


def shakhesBource():
    resp = requests.get(
        'https://sourcearena.ir/api/?token=' + token + '& market=market_bourse')
    # print("shakhesBource ", resp.status_code)
    if resp.status_code == 200:
        dataA = json.loads(resp.text)
        shakhesBource = []
        cell = {"state": dataA["bourse"]["state"], "b_index": dataA["bourse"]["index"],
                "index_change": dataA["bourse"]["index_change"],
                "index_change_percent": dataA["bourse"]["index_change_percent"], "index_h": dataA["bourse"]["index_h"],
                "index_h_change": dataA["bourse"]["index_h_change"],
                "index_h_change_percent": dataA["bourse"]["index_h_change_percent"]
            , "market_value": dataA["bourse"]["market_value"], "trade_number": dataA["bourse"]["trade_number"],
                "trade_value": dataA["bourse"]["trade_value"], "trade_volume": dataA["bourse"]["trade_volume"]}
        shakhesBource.append(cell)
        if shakhesBource:
            populateDatabase('temp', 'main_index', shakhesBource, 9)


def startDetectVolume():
    print("start detectVolume...")
    rt = RepeatedTimer(10, detectVolume)
    try:
        sleep(14400)
    finally:
        rt.stop()


def timeVolume():
    pushMaxBuy()
    pushMaxSell()
    possibleQueueBuy()
    possibleQueueSell()


def startDateVolume():
    print("start dateVolume...")
    rt = RepeatedTimer(1800, timeVolume)
    try:
        sleep(14400)
    finally:
        rt.stop()


def startShakhes():
    print("start shakhesBource...")
    rt = RepeatedTimer(10, shakhesBource)
    rt = RepeatedTimer(30, car)
    try:
        sleep(14400)
    finally:
        rt.stop()


def startServer():
    try:
        print("I'm working...")
        rt = RepeatedTimer(10, shakhesBource)
        rt = RepeatedTimer(1000, car)
        rt = RepeatedTimer(800, currency)
        rt = RepeatedTimer(700, digital_currency)
        rt = RepeatedTimer(30, detectVolume)
        rt = RepeatedTimer(1080, timeVolume)
        try:
            sleep(14400)
        finally:
            rt.stop()
        # startShakhes()
        # startDetectVolume()
        # startDateVolume()
        # car()
        # currency()
    except ZeroDivisionError:
        logging.exception("message")


def downloadCsvs():
    print("to download Csv ...")
    tickers = tse.download(symbols='all', write_to_csv=True, include_jdate=True)
    records_dict = download_client_types_records(symbols='all', write_to_csv=True, include_jdate=True)
    for symbol in all_symbols():
        df = pd.read_csv('client_types_data/' + symbol + '.csv', index_col=False, low_memory=False, error_bad_lines=False)
        df = df.sort_values(by='date', ascending=True)
        df.to_csv('client_types_data/' + symbol + '.csv', index=False)
        print(symbol)
    print("finish download csv")
    # timeVolume()

def downloadOneCsv(symbol):
    print("to download Csv ...")
    tickers = tse.download(symbols=symbol, write_to_csv=True, include_jdate=True)
    records_dict = download_client_types_records(symbols=symbol, write_to_csv=True, include_jdate=True)
    df = pd.read_csv('client_types_data/' + symbol + '.csv', index_col=False, low_memory=False,
                     error_bad_lines=False)
    df = df.sort_values(by='date', ascending=True)
    df.to_csv('client_types_data/' + symbol + '.csv', index=False)
    print(symbol)
    print("finish download csv")


logging.basicConfig(filename="log.txt",
                    filemode='a',
                    format='%(levelname)s: %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.ERROR)

logger = logging.getLogger('urbanGUI')

# schedule.every().saturday.at("09:00").do(startServer)
# schedule.every().sunday.at("09:00").do(startServer)
# schedule.every().monday.at("09:00").do(startServer)
# schedule.every().tuesday.at("10:07").do(startServer)
# schedule.every().wednesday.at("09:00").do(startServer)
# # schedule.every().day.at("17:00").do(downloadCsvs)
#
# while True:
#     schedule.run_pending()
#     time.sleep(5)


# downloadOneCsv('فایرا')
# startServer()
downloadCsvs()
# detectVolume()
# timeVolume()
# all_stocks()
# print(volumeChanges())
# currency()
# digital_currency()
# car()
# shakhesBource()
# startShakhes()
# readCsv()
# possibleQueueBuy()
# startDetectVolume()
