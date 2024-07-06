#This notebook is set to run automatatically every 15th on a raspberry pi

import yfinance as yf
import pandas as pd
import numpy as np
import datetime as dt
import requests
from datetime import date
import pygsheets

#Connect to google sheet using google service account authetication file
gs=pygsheets.authorize(service_file=r'Google_authentication_file_path')

#get tickers: condition is set to only download info for stocks and mutual funds
tickers=gs.open('Tickers')[0]
tickers_df=tickers.get_as_df()
stickers_df=tickers_df[tickers_df['Stocks YN']=="Y"]
stickers_list=stickers_df['Tickers']
mtickers_df=tickers_df[tickers_df['Group']=="Mutual Funds"]
mtickers_list=mtickers_df['Tickers']

#Open existing Info file from google sheet
Info_gs=gs.open('Info')
Info_gs=Info_gs[0]
Info_df=Info_gs.get_as_df()
Info_item_gs=gs.open('Info Item')
Info_item_gs=Info_item_gs[0]
Info_item_df=Info_item_gs.get_as_df()

#Append newly downloaded info to existing file, then save back to google drive
df_info=pd.DataFrame()
for sticker in stickers_list:            
    stock=yf.Ticker(sticker)
    info_new=stock.info
    df_info_new=pd.Series(info_new).to_frame()
    df_info_new['Tickers']=sticker
    df_info=df_info.append(df_info_new)
df_info['Date']=dt.date.today()
df_info=df_info.reset_index()
df_info=df_info.set_axis(['Info Item', 'Value', 'Tickers', 'Date'], axis=1, inplace=False)
Info_trim=pd.merge(df_info,Info_item_df,how="inner", on='Info Item' )
Info_trim=Info_trim[['Info Item','Value','Tickers', 'Date']]
Info=Info_df.append(Info_trim)
Info_gs.set_dataframe(Info,'A1')

#Mutual funds have different information, so it stored in a different file
MInfo_gs=gs.open('Info_mf')
MInfo_gs=MInfo_gs[0]
MInfo_df=MInfo_gs.get_as_df()
MInfo_item_gs=gs.open('MInfo Item')
MInfo_item_gs=MInfo_item_gs[0]
MInfo_item_df=MInfo_item_gs.get_as_df()

df_minfo=pd.DataFrame()
for mticker in mtickers_list:            
    mf=yf.Ticker(mticker)
    minfo_new=mf.info
    df_minfo_new=pd.Series(minfo_new).to_frame()
    df_minfo_new['Tickers']=mticker
    df_minfo=df_minfo.append(df_minfo_new)
df_minfo['Date']=dt.date.today()
df_minfo=df_minfo.reset_index()
df_minfo=df_minfo.set_axis(['Info Item', 'Value', 'Tickers', 'Date'], axis=1, inplace=False)

mInfo_trim=pd.merge(df_minfo,MInfo_item_df,how="inner", on='Info Item' )
mInfo_trim=mInfo_trim[['Info Item','Value','Tickers', 'Date']]
mInfo=MInfo_df.append(mInfo_trim)
MInfo_gs.set_dataframe(mInfo,'A1')

#Download actions
Action_gs=gs.open('Actions')
Action_gs=Action_gs[0]
Action_df=Action_gs.get_as_df()

df_actions=pd.DataFrame()
for sticker in stickers_list:            
    stock=yf.Ticker(sticker)
    df_actions_new=stock.actions
    df_actions_new['Ticker']=sticker
    df_actions=df_actions.append(df_actions_new)
df_actions=df_actions.reset_index()
Action_gs.set_dataframe(df_actions,'A1')

#Download events
Events_gs=gs.open('Events')
Events_gs=Events_gs[0]
Events_df=Events_gs.get_as_df()

df_events=pd.DataFrame()
for sticker in stickers_list:            
    stock=yf.Ticker(sticker)
    df_events_new=stock.calendar
    df_events_new['Ticker']=sticker
    df_events=df_events.append(df_events_new)
df_events=df_events[df_events.index=="Earnings Date"]
df_events=df_events.reset_index()    
Events_gs.set_dataframe(df_events,'A1')
