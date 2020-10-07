#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 13 17:52:44 2020

@author: nelsonandara
"""
from yahoo_fin.stock_info import get_data
from sqlalchemy import create_engine
import pymysql
import pandas as pd
from urllib import parse as urlparse

sql_connection_string = 'mysql+pymysql://FinanceYahoo:%s@localhost/FinanceYahoo' % urlparse.quote_plus('F!n@nc3Y@h00')
sql_engine = create_engine(sql_connection_string)
sql_connection = sql_engine.connect()

#Function to create a dataframe with the date range to capture for each symbol
def date_ranges():
    query = "DROP TABLE FinanceYahoo.Equity_Daily"
    sql_connection.execute(query)
    query = "SELECT Ticker,DATE_SUB(MIN(CAST(`Last Trade Date` as DATE)), INTERVAL 21 DAY) As FromDt, MAX(CAST(`Last Trade Date` as DATE)) As ToDt "
    query += "FROM FinanceYahoo.Option GROUP BY Ticker ORDER BY Ticker"
    enumerator = sql_connection.execute(query)
    df = pd.DataFrame(enumerator.fetchall())
    df.columns = enumerator.keys()
    return df

for x in date_ranges().itertuples():
    print(x.Ticker,x.FromDt,x.ToDt)
    try:
        df = get_data(x.Ticker,x.FromDt,x.ToDt)
        df.to_sql('Equity_Daily',sql_connection,if_exists='append')
    except:
        print('%s - Skipped',x.Ticker)



