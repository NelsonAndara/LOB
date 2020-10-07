#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 13 18:05:57 2020

@author: nelsonandara
"""
from sqlalchemy import create_engine
import pymysql
import pandas as pd
import numpy as np
from urllib import parse as urlparse
import py_vollib.black_scholes as bs
import py_vollib.black_scholes.greeks.analytical as greeks
from datetime import datetime

sql_connection_string = 'mysql+pymysql://FinanceYahoo:%s@localhost/FinanceYahoo' % urlparse.quote_plus('F!n@nc3Y@h00')
sql_engine = create_engine(sql_connection_string)
sql_connection = sql_engine.connect()

def get_interest_rate(date):
    query = "SELECT Rate FROM FinanceYahoo.InterestRate WHERE '"+date+"' BETWEEN FromDt AND ToDt"
    enumerator = sql_connection.execute(query)
    rate = enumerator.fetchone()
    return rate[0]

def get_annualized_stddev(ticker,date):
    query = "SELECT A.adjclose - B.adjclose as equity_return "
    query += "FROM FinanceYahoo.Equity_Daily A "
    query += "INNER JOIN FinanceYahoo.Equity_Daily B "
    query += "ON A.`index` = DATE_ADD(B.`index`,INTERVAL 1 DAY) AND "
    query += "A.ticker = B.ticker "
    query += "WHERE A.ticker = '"+ticker+"' "
    query += "AND A.`index` <= '"+date+"' "
    query += "ORDER BY A.`index` DESC LIMIT 20"
    enumerator = sql_connection.execute(query)
    df = pd.DataFrame(enumerator.fetchall())
    df.columns = enumerator.keys()
    return df['equity_return'].std() * np.sqrt(252)

def get_black_scholes_inputs():
    query = "SELECT B.Ticker, A.Expiration, B.`index` as TradeDt,A.Strike, A.OptionType, B.adjclose FROM FinanceYahoo.Option A "
    query += "INNER JOIN FinanceYahoo.Equity_Daily B "
    query += "ON A.ticker = B.ticker AND "
    query += "CAST(A.`Last Trade Date` as DATE) = CAST(B.`index` as DATE) "
    query += "GROUP BY B.Ticker, A.Expiration, B.`index`,A.Strike, A.OptionType, B.adjclose "
    query += "ORDER BY B.Ticker, A.Expiration, B.`index`,A.Strike, A.OptionType, B.adjclose "
    enumerator = sql_connection.execute(query)
    df = pd.DataFrame(enumerator.fetchall())
    df.columns = enumerator.keys()
    return df

black_scholes_inputs = get_black_scholes_inputs()
count = 0

for x in black_scholes_inputs.itertuples():
    flag = str(x.OptionType).lower()
    S = x.adjclose
    K = x.Strike
    delta = x.Expiration - x.TradeDt
    t = delta.days / 252.0
    r = get_interest_rate(x.TradeDt.strftime("%Y-%m-%d"))
    sigma = get_annualized_stddev(x.Ticker,x.TradeDt.strftime("%Y-%m-%d")) / 100.0
    price = bs.black_scholes(flag,S,K,t,r,sigma)
    delta = greeks.delta(flag,S,K,t,r,sigma)
    gamma = greeks.gamma(flag,S,K,t,r,sigma)
    rho = greeks.rho(flag,S,K,t,r,sigma)
    theta = greeks.theta(flag,S,K,t,r,sigma)
    vega = greeks.vega(flag,S,K,t,r,sigma)
    data = {'Ticker': x.Ticker, 'Expiration': x.Expiration, 'TradeDt': x.TradeDt, 'Strike': x.Strike, 'OptionType': x.OptionType, 'Theo Price': price, 'Delta': delta, 'Gamma': gamma, 'Rho': rho, 'Theta': theta, 'Vega': vega }
    df = pd.DataFrame(data, columns = ['Ticker','Expiration','TradeDt','Strike','OptionType','Theo Price','Delta','Gamma','Rho','Theta','Vega'], index = [count])
    df.to_sql('BlackScholes',sql_connection,if_exists='append')
    count += 1
    if count % 1000 == 0:
        print(f'{count} - {len(black_scholes_inputs)} : {x.Ticker} - {x.Expiration} - {x.TradeDt} - {x.Strike} - {x.OptionType} - {price}')