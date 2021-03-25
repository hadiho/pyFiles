# coding=utf8
import requests
import json
import pymysql.cursors
import threading
from time import sleep
from threading import Timer
import pandas as pd
import numpy as np
import heapq
import array
import operator
import datetime
import os.path
from RepeatedTimer import RepeatedTimer
import schedule
import time
from csv import writer

token2 = 'gAAAAABgN8ikU2LE-3ioT9oZVQ_XIpGDR760L9NPt4jK7IZb56Hc6-zwOPHSfjXNfWyC74JPmHTj2r5YpJnGp6kONEZ-ltE6XDBNFrrTbRYowPFNU4m8UFYnFsfFcOQOKXg-ADPxTWDyomtDHW7g5AfFwz4XgJm-mVjJhAxq6JB_4oQORTBlytRy3Yju8mGUA-HHrh_540BUs6bAjhT4yExLm6qGxydY5xQSW-3Ff8GrNKj3tkXAcYyku40HelqbeLyF2nB1yViV2QXOWPYXKQx8wYt1pot79f_et0fFEZCuVIVpv1J1mgHBugG6Z9Ft3QNR8xiVVBp7OR7I-wQOwKU3_lyoo7EOtr63wyV-ePLWUMX81BYu9BI='

boursViewUrl = 'http://api.bourseview.com/v2/quotes?filters%20on=close:3500&lastN=1&items=indinst&exchanges=IRTSENO&tickers=IRO1IKCO0001'

token = '6e6671c1fcc42c94bf448fe7d880fa88'





def populateDatabase(dbname, tbname, table_list, flag):
    dbName = dbname
    tableName = tbname
    values = ""
    for idx, val in enumerate(table_list):
        if idx > 0:
            values = values + ","
        if flag == 1:
            values = values + "('" + val["symbol"] + "','" + str(val["vol"]) + "','" + str(val["percent"]) + ")"
        if flag == 2:
            values = values + "('" + val["symbol"] + "','" + str(val["close"]) + "','" + str(
                val["closeP"]) + "','" + str(
                val["percent"]) + ")"
        if flag == 3:
            values = values + "('" + val["symbol"] + "','" + str(val["close"]) + "','" + str(val["closeP"]) + ")"
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
            values = values + "('" + str(val["name"]) + "','" + str(val["sana_usd"]) + "','" + str(
                val["price_usd"]) + "','" + str(val["price_irr"]) + "','" + str(val["daily_change_usd"]) + \
                     "','" + str(val["daily_change_irr"]) + "','" + str(val["daily_change_percent"]) + "')"
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
            values = values + "('" + str(val["TIME"]) + "','" + str(val["NAME"]) + "','" + str(
                val["FULLNAME"]) + "','" + str(val["CLOSE"]) + "','" \
                     + str(val["PERCENT"]) + "','" + str(val["AVERAGE"]) + "','" + str(val["TOTAL"]) + "','" + str(
                val["NUMBER"]) \
                     + "','" + str(val["ATTRIBUTE"]) + "','" + str(val["TYPE"]) + "')"

        # print(values)

    if len(table_list) > 0:
        connection = pymysql.connect(host='194.5.175.58',
                                     user='root',
                                     password='Hadi2150008140@$&!',
                                     database=dbName,
                                     port=3306,
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection:
            with connection.cursor() as cursor:
                sql = "DELETE FROM " + tableName
                cursor.execute(sql, args=None)
            connection.commit()

            with connection.cursor() as cursor:
                sql = "INSERT INTO " + tableName + " VALUES " + values + ";"
                cursor.execute("SET CHARACTER SET utf8", args=None)
                cursor.execute(sql, args=None)
            connection.commit()


def volumeChanges():
    resp = requests.get(
        'https://sourcearena.ir/api/?token=' + token + '&all&type=0')  # 6e6671c1fcc42c94bf448fe7d880fa88&all&type=0')
    print(resp.status_code)
    data = json.loads(resp.text)

    return data


saveData = None


def hotMoney(dataA):
    global saveData
    if saveData is None:
        saveData = dataA

    hotMoneyList = []
    # for data in dataA:
    for idx, val in enumerate(dataA):
        value = dataA[idx]['real_buy_value'] - saveData[idx]['real_buy_value']
        if value > 2000000000:
            count = dataA['real_buy_count'] - saveData[idx]['real_buy_count']
            average = value / count
            cell = {"time": dataA[idx]['TIME'], "name": dataA[idx]['name'], "full_name": dataA[idx]['full_name'],
                    "close": dataA[idx]['close_price'], "percent": dataA[idx]['close_price_change_percent'],
                    "average": average, "total": value, "number": count, "attribute": 1, "type": 1}
            hotMoneyList.append(cell)

        value = dataA[idx]['real_sell_value'] - saveData[idx]['real_sell_value']
        if value > 2000000000:
            count = dataA['real_sell_count'] - saveData[idx]['real_sell_count']
            average = value / count
            cell = {"time": dataA[idx]['TIME'], "name": dataA[idx]['name'], "full_name": dataA[idx]['full_name'],
                    "close": dataA[idx]['close_price'], "percent": dataA[idx]['close_price_change_percent'],
                    "average": average, "total": value, "number": count, "attribute": 1, "type": 2}
            hotMoneyList.append(cell)

    saveData = dataA
    populateDatabase('price', 'hot_money', hotMoneyList, 10)


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
    fileNameTicker = 'tickers_data/' + json['name'] + '.csv'
    fileNameVolume = 'client_types_data/' + json['name'] + '.csv'

    row_contents = [datetime.datetime.now().strftime('%Y-%m-%d'), json['yesterday_price'], json['highest_price'],
                    json['lowest_price'], json['close_price'], json['trade_value'], json['trade_volume'],
                    json['trade_number'], json['final_price'], datetime.datetime.now().strftime('%Y-%m-%d')]

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

    row_contentsVolume = [datetime.datetime.now().strftime('%Y-%m-%d'), json['real_buy_count'], json['co_buy_count'],
                          json['real_sell_count'], json['co_sell_count'], json['real_buy_volume'],
                          json['co_buy_volume'],
                          json['real_sell_volume'], json['co_sell_volume'], json['real_buy_value'],
                          json['co_buy_value'],
                          json['real_sell_value'], json['co_sell_value'], json['co_sell_volume'], real_buy_mean_price,
                          real_sell_mean_price, co_buy_mean_price, co_sell_mean_price]

    if os.path.isfile(fileNameTicker):
        df = pd.read_csv(fileNameTicker, index_col=False)
        print(fileNameTicker)
        # print(datetime.datetime.now().strftime('%Y-%m-%d'))
        # print(df.tail(1)['date'])
        # print(df.iloc[-1]['date'])

        now = datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
        past = datetime.datetime.strptime(df.iloc[-1]['date'], '%Y-%m-%d')

        if now > past:
            print("create ticker row")
            appendNewLineToCsv(fileNameTicker, row_contents, False)
        elif now == past:
            # if len(df) == 1:
            #     columns = ['date', 'open', 'high', 'low', 'adjClose', 'value', 'volume', 'count', 'close', 'jdate']
            #     appendNewLineToCsv(fileNameTicker, columns, True)
            # else:
            df.drop(df.tail(-1).index, inplace=True)
            appendNewLineToCsv(fileNameTicker, row_contents, True)
            df.to_csv(fileNameTicker, index=False)
            print("update ticker row")
    else:
        columns = ['date', 'open', 'high', 'low', 'adjClose', 'value', 'volume', 'count', 'close', 'jdate']
        appendNewLineToCsv(fileNameTicker, columns, True)
        appendNewLineToCsv(fileNameTicker, row_contents, True)
        print("exist create ticker row")

    if os.path.isfile(fileNameVolume):
        df = pd.read_csv(fileNameVolume, index_col=False)

        now = datetime.datetime.strptime(datetime.datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d')
        past = datetime.datetime.strptime(df.iloc[-1]['date'], '%Y-%m-%d')

        if now > past:
            print("create volume row")
            appendNewLineToCsv(fileNameVolume, row_contentsVolume, False)
        elif now == past:
            # if len(df) == 1:
            #     columns = ['date', 'real_buy_count', 'co_buy_count', 'real_sell_count', 'co_sell_count',
            #                'real_buy_volume',
            #                'co_buy_volume',
            #                'real_sell_volume', 'co_sell_volume', 'real_buy_value', 'co_buy_value', 'real_sell_value',
            #                'co_sell_value', 'co_sell_volume',
            #                'real_buy_mean_price', 'real_sell_mean_price', 'co_buy_mean_price', 'co_sell_mean_price']
            #     appendNewLineToCsv(fileNameVolume, columns, True)
            # else:
            df.drop(df.tail(-1).index, inplace=True)
            appendNewLineToCsv(fileNameVolume, row_contentsVolume, True)
            df.to_csv(fileNameVolume, index=False)
            print("update volume row")
    else:
        columns = ['date', 'real_buy_count', 'co_buy_count', 'real_sell_count', 'co_sell_count', 'real_buy_volume',
                   'co_buy_volume',
                   'real_sell_volume', 'co_sell_volume', 'real_buy_value', 'co_buy_value', 'real_sell_value',
                   'co_sell_value', 'co_sell_volume',
                   'real_buy_mean_price', 'real_sell_mean_price', 'co_buy_mean_price', 'co_sell_mean_price']
        appendNewLineToCsv(fileNameVolume, columns, True)
        appendNewLineToCsv(fileNameVolume, row_contentsVolume, True)
        print("exist create volume row")


def historyVolume(dataA):
    for idx, val in enumerate(dataA):
        if dataA[idx]['name'] == 'وسپهر':
            readCsv(dataA[idx])


def detectVolume():
    dataA = volumeChanges()

    hotMoney(dataA)
    historyVolume(dataA)
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
        lastList.append(cell)

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
    populateDatabase('temp', 'all_stocks', allStocks, 5)


def currency():
    resp = requests.get(
        'https://sourcearena.ir/api/?token=' + token + '& currency')
    print(resp.status_code)
    dataA = json.loads(resp.text)
    print(dataA)

    allCurrency = []
    for data in dataA['data']:
        cell = {"slug": data["slug"], "name": data["name"], "price": data["price"], "minPrice": data["min_price"],
                "maxPrice": data["max_price"], "time": data["jalali_last_update"]}
        allCurrency.append(cell)
    populateDatabase('temp', 'currency', allCurrency, 6)


def car():
    resp = requests.get(
        'https://sourcearena.ir/api/?token=' + token + '&car=all')
    print(resp.status_code)
    dataA = json.loads(resp.text)
    print(dataA)

    allDCurrency = []
    for data in dataA:
        cell = {"model": data["model"], "type": data["type"], "price": data["price"],
                "market_price": data["market_price"], "last_update": data["last_update"]}
        allDCurrency.append(cell)
    populateDatabase('temp', 'car', allDCurrency, 8)


def digital_currency():
    resp = requests.get(
        'https://sourcearena.ir/api/?token=' + token + '& dcurrency')
    print(resp.status_code)
    dataA = json.loads(resp.text)
    print(dataA)

    allDCurrency = []
    for data in dataA:
        cell = {"name": data["name"], "sana_usd": data["sana_usd"], "price_usd": data["price_usd"],
                "price_irr": data["price_irr"], "daily_change_usd": data["daily_change_usd"],
                "daily_change_irr": data["daily_change_irr"], "daily_change_percent": data["daily_change_percent"]}
        allDCurrency.append(cell)
    populateDatabase('temp', 'digital_currency', allDCurrency, 7)


def shakhesBource():
    resp = requests.get(
        'https://sourcearena.ir/api/?token=' + token + '& market=market_bourse')
    print(resp.status_code)
    dataA = json.loads(resp.text)
    print(dataA)

    shakhesBource = []
    cell = {"state": dataA["bourse"]["state"], "b_index": dataA["bourse"]["index"],
            "index_change": dataA["bourse"]["index_change"],
            "index_change_percent": dataA["bourse"]["index_change_percent"], "index_h": dataA["bourse"]["index_h"],
            "index_h_change": dataA["bourse"]["index_h_change"],
            "index_h_change_percent": dataA["bourse"]["index_h_change_percent"]
        , "market_value": dataA["bourse"]["market_value"], "trade_number": dataA["bourse"]["trade_number"],
            "trade_value": dataA["bourse"]["trade_value"], "trade_volume": dataA["bourse"]["trade_volume"]}
    shakhesBource.append(cell)
    populateDatabase('temp', 'shakhesBource', shakhesBource, 9)


def startDetectVolume():
    print("start detectVolume...")
    rt = RepeatedTimer(5, detectVolume())
    try:
        sleep(14400)
    finally:
        rt.stop()


def startShakhes():
    print("start shakhesBource...")
    rt = RepeatedTimer(30, shakhesBource)
    try:
        sleep(14400)
    finally:
        rt.stop()


def startServer():
    print("I'm working...")
    startShakhes()
    startDetectVolume()
    car()
    currency()


# schedule.every().saturday.at("14:53").do(startServer)
# schedule.every().sunday.at("08:55").do(startServer)
# schedule.every().monday.at("08:55").do(startServer)
# schedule.every().tuesday.at("08:55").do(startServer)
# schedule.every().wednesday.at("08:55").do(startServer)
#
# while True:
#     schedule.run_pending()
#     time.sleep(5)


# detectVolume()
# all_stocks()
# print(volumeChanges())
# currency()
# digital_currency()
car()
shakhesBource()
# startShakhes()
# readCsv()
