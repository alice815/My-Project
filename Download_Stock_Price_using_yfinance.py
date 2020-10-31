import pygsheets
import yfinance as yf
import pandas as pd
import datetime as dt

#Read history data saved in google sheet
gc=pygsheets.authorize(service_file=r'google sheet json file location')
gf=gc.open('stocks gs')
gs=gf[0]
stocks=pd.DataFrame(gs)
header=stocks.iloc[0]
stocks=stocks[1:]
stocks.columns=header

# Check date in the history data, define download time period
stocks['Date']=pd.to_datetime(stocks['Date'])
start=stocks.groupby(['name'])['Date'].max()
start=pd.DataFrame(start)+ dt.timedelta(days=1)
end=dt.date.today()

#  Download new data from Yahoo Fiance
tickers = {'Chevron':'cvx', 'Google': 'goog', 'Amazon':'amzn', 'Facebook':'fb', 'Exxon':'xom', 'Tesla': 'tsla', 'Apple': 'aapl',
           'Shell':'rds-a', 'BP':'bp', 'Microsoft':'msft','Honeywell':'hon','Gold':'gc=f','MYR':'myr=x', 
           '10y Treasury':'^TNX', 'Brent':'bz=f', 'Copper':'hg=f', 'Lennar':'len', 'Phillips66':'psxp', 
           'American Airlines': 'AAL', 'SW Airlines':'LUV', 'United Airlines': 'UAL', 'Sunrun': 'Run','Sunnova':'Nova'}

if start['Date'].min() <= end: 
    df_stocks=pd.DataFrame()
    for key, value in tickers.items():            
        download=yf.download(value, start=start.loc[key][0].date(), end=end)
        download = pd.DataFrame(download)
        download['name'] = key
        df_stocks=df_stocks.append(download)
else:
    print('Already have latest data')         

# Remove duplicate data, save data to google sheet
if len(df_stocks)>0:
    df_stocks=df_stocks.reset_index()
    df_stocks=df_stocks[df_stocks['Date']<pd.to_datetime('today').floor('d')]
if len(df_stocks)>0:
    df_stocks['Total']=df_stocks['High']+df_stocks['Volume']
    df_stocks=df_stocks[df_stocks['Total'] == df_stocks.groupby(['name', 'Date'])['Total'].transform('max')]
    df_stocks=df_stocks.drop('Total', axis=1)
    df_stocks=df_stocks.drop_duplicates()
    stocks=stocks.append(df_stocks, sort=False, ignore_index=False)      
    gs.set_dataframe(stocks,'A1')
