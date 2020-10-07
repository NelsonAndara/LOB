#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  3 13:12:59 2020

@author: nelsonandara
"""

#Imports from yahoo_fin

from yahoo_fin.stock_info import tickers_dow
from yahoo_fin.stock_info import tickers_nasdaq
from yahoo_fin.stock_info import tickers_other
from yahoo_fin.stock_info import tickers_sp500
from yahoo_fin.stock_info import get_data
from yahoo_fin.stock_info import get_stats
from yahoo_fin.options import get_expiration_dates
from yahoo_fin.options import get_options_chain

#Imports to interact with MySQL

from sqlalchemy import create_engine
import pymysql

#This is needed in order to pass special characters in the MySQL connection string
from urllib import parse as urlparse

#Additional Imports

# This is needed to convert string date time stamps into datetime objects
from dateutil.parser import parse
# This is needed to invoke the pandas to_datetime function
import pandas as pd

# This is needed to download a file from an FTP site
from ftplib import FTP

#Get unique list of tickers

def get_tickers():
    tickers = tickers_dow()
    tickers.extend(tickers_nasdaq())
    tickers.extend(tickers_other())
    tickers.extend(tickers_sp500())
    tickers = list(dict.fromkeys(tickers)) #This instruction eliminates duplicate tickers
    return [item for item in tickers if "-" not in item and "$" not in item and "." not in item]

def get_tickers_from_ftp():
    ftp = FTP('ftp.nasdaqtrader.com')
    ftp.login()
    ftp.cwd('SymbolDirectory')
    with open('options.txt', 'wb') as fp:
        ftp.retrbinary('RETR options.txt', fp.write)
    ftp.quit()
    df = pd.read_csv('options.txt', sep='|', index_col=False, names=['Root Symbol','Options Closing Type','Options Type','Expiration Date','Explicit Strike Price','Underlying Symbol','Underlying Issue Name','Pending'])
    tickers = df['Underlying Symbol'].unique()
    midpoint = len(tickers)//2
    return tickers[1:midpoint]

def get_tickers_from_db():
    query = "SELECT distinct Ticker FROM `Option`"
    return pd.read_sql(query,sql_connection)['Ticker']

#Connect to MySQL

#urllib.parse.quote_plus is used to url encode the special characters in the password
sql_connection_string = 'mysql+pymysql://FinanceYahoo:%s@localhost/FinanceYahoo' % urlparse.quote_plus('F!n@nc3Y@h00')
sql_engine = create_engine(sql_connection_string)
sql_connection = sql_engine.connect()

#Function to parse options data from Finance Yahoo

def parse_dataframe(df,expiration):
    df.loc[df['Last Price'] == '-','Last Price'] = None
    df.loc[df['Bid'] == '-','Bid'] = None
    df.loc[df['Ask'] == '-','Ask'] = None
    df.loc[df['Change'] == '-','Change'] = None
    df['% Change'] = df['% Change'].map(lambda x: x.lstrip('+-').rstrip('%').replace(',',''))
    df.loc[df['% Change'] == '','% Change'] = '0'
    df.loc[df['Open Interest'] == '-','Open Interest'] = '0'
    df.loc[df['Volume'] == '-','Volume'] = '0'
    df['Implied Volatility'] = df['Implied Volatility'].map(lambda x: x.rstrip('%').replace(',',''))
    df['Last Trade Date'] = df['Last Trade Date'].map(lambda x: x.rstrip(' EDT'))
    df['Last Trade Date'] = pd.to_datetime(df['Last Trade Date'],format="%Y-%m-%d %I:%M%p")
    df['Ticker'] = df['Contract Name'].str.extract(r'([A-Z]*)')
    df['Expiration'] = parse(expiration)
    df['OptionType'] = df['Contract Name'].str.extract(r'[A-Z]*[0-9]{6}([A-Z])')
    return df.astype({"% Change" : float, "Volume" : int, "Implied Volatility" : float}) 



#Run parser for current date across all optionable securities

ticker_count = 1
#tickers = get_tickers_from_ftp()
tickers = get_tickers_from_ftp()

for ticker in tickers:
    print(f'{ticker}: {ticker_count} of {len(tickers)}')    
    ticker_count += 1
    
    for date in get_expiration_dates(ticker):
        try:
            options_chain = get_options_chain(ticker,date)
            try:
                df = parse_dataframe(options_chain['calls'],date)
                df.to_sql('Option',sql_connection,if_exists='append')
            except:
                print(f'Skipped {ticker},{date}')
            try:
                df = parse_dataframe(options_chain['puts'],date)
                df.to_sql('Option',sql_connection,if_exists='append')
            except:
                print(f'Skipped {ticker},{date}')
        except:
            print(f'No option chains for {ticker} , {date}')
        
print('Done')

