#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 13 18:02:11 2020

@author: nelsonandara
"""
from yahoo_fin.stock_info import get_stats
from sqlalchemy import create_engine
import pymysql
import pandas as pd
from urllib import parse as urlparse

sql_connection_string = 'mysql+pymysql://FinanceYahoo:%s@localhost/FinanceYahoo' % urlparse.quote_plus('F!n@nc3Y@h00')
sql_engine = create_engine(sql_connection_string)
sql_connection = sql_engine.connect()

def tickers_for_dividend():
    query = "DROP TABLE FinanceYahoo.Dividend"
    sql_connection.execute(query)
    query = "SELECT Ticker FROM FinanceYahoo.Option GROUP By Ticker ORDER BY Ticker"
    enumerator = sql_connection.execute(query)
    df = pd.DataFrame(enumerator.fetchall())
    df.columns = enumerator.keys()
    return df

for x in tickers_for_dividend().itertuples():
    try:
        df = get_stats(x.Ticker)
        try:
            y = str(df[df['Attribute'] == 'Forward Annual Dividend Yield 4']['Value'].iloc[0]).replace('%','')
            dividend = 0.0 if y == 'nan' else float(y)
        except:
            dividend = 0.0
    except:
        dividend = 0.0
    print(f'{x.Ticker} - {dividend}')
    dividend_dictionary = {'Ticker':x.Ticker, 'Dividend':dividend}
    df = pd.DataFrame.from_records([dividend_dictionary])
    df.to_sql('Dividend',sql_connection,if_exists='append')