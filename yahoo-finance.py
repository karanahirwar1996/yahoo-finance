import gspread
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./original-advice-385307-e221975bf7db.json', scope)
client = gspread.authorize(creds)
gs = client.open('intraday stocks Selection')
Sourcesheet = gs.worksheet('Source-Link')
Sourcesheet_records= Sourcesheet.get_all_records()
source_df=pd.DataFrame(Sourcesheet_records)
def stock_history(name, days=0):
    import yfinance as yf
    days = str(days) + "d"
    symbol = name + ".NS"
    
    try:
        data = yf.download(symbol, period=days, progress=False)
    except KeyError as e:
        print(f"Failed to download data for {symbol}: {str(e)}")
        return None
    
    filtered_data = data[['Open', 'High', 'Low', 'Close', 'Volume']]
    filtered_data['SYMBOL'] = name
    final = filtered_data[['SYMBOL', 'Open', 'Low', 'High', 'Close', 'Volume']].reset_index()
    if len(final)>0:
        result=final
    else:
        result=None
    return result
symbol_list =list(source_df['SYMBOL'])
final = pd.DataFrame(columns=['SYMBOL', 'Open', 'Low', 'High', 'Close', 'Volume'])
for x in symbol_list:
    df = stock_history(x)
    if df is not None:
        final = pd.concat([final, df])
sorted_final=final.sort_values(by='Volume',ascending=False)
merge_df=sorted_final.merge(source_df,on='SYMBOL',how="left")
dailysheet=gs.worksheet("Daily-Update")
set_with_dataframe(dailysheet,merge_df)
