#This notebook is set to automatically run on a raspberry pi at 6:30pm EST weekday.

import yfinance as yf
import pandas as pd
import numpy as np
import datetime as dt
import requests
from datetime import date
import pygsheets

#Connect to google sheet using google service account authetication file
gs=pygsheets.authorize(service_file=r'Google_authentication_file_path')

#Get the list of tickers from google sheet
tickers=gs.open('Tickers')[0]
tickers_df=tickers.get_as_df()
tickers_list=tickers_df['Tickers']
tickers_list=" ".join(tickers_list)

#download today's data from yahoo finance
stocks = yf.download(tickers_list, start=dt.date.today(), group_by='ticker')
stocks=stocks.reset_index()
stocks= pd.melt(stocks, id_vars=['Date']).dropna()
stocks=pd.pivot_table(stocks, values=['value'], index=['Date','variable_0'],columns=['variable_1']).reset_index()
columns_1=stocks.columns.get_level_values(1)
columns_2=stocks.columns.get_level_values(0)
stocks.columns=columns_2[0:1].tolist()+['Tickers']+columns_1[2:8].tolist() 

#Append new stock data to the history data in google drive
stocks_history=gs.open('Stocks')[0]
history_df=stocks_history.get_as_df()
stocks_new=stocks.append(history_df, sort=False, ignore_index=False)
stocks_history.set_dataframe(stocks_new,'A1')
