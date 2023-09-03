import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import yfinance as yf
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./original-advice-385307-e221975bf7db.json', scope)
client = gspread.authorize(creds)
gs = client.open('intraday stocks Selection')
Sourcesheet = gs.worksheet('Source-Link')
Sourcesheet_records = Sourcesheet.get_all_records()
source_df = pd.DataFrame(Sourcesheet_records)
source_df=source_df[["SYMBOL","ISIN","URL"]].copy()
def stock_history(name, days=1):
    days = str(days) + "d"
    name=str(name)
    symbol = name + ".NS"
    try:
        data = yf.download(symbol, period=days, progress=False)
        if data.empty:
            print(f"No data available for {symbol}")
            return None
    except Exception as e:
        print(f"Failed to download data for {symbol}: {str(e)}")
        return None

    filtered_data = data[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
    filtered_data['SYMBOL'] = name
    final = filtered_data[['SYMBOL', 'Open', 'Low', 'High', 'Close', 'Volume']].reset_index()
    return final

symbol_list = list(source_df['SYMBOL'])
final = pd.DataFrame(columns=['SYMBOL', 'Open', 'Low', 'High', 'Close', 'Volume'])

for x in symbol_list:
    df = stock_history(x)
    df2 = stock_history(x,5)
    if df2 is not None:
        try:
            second_last_row_df = df2.iloc[[-2]]
            df3 = second_last_row_df[["SYMBOL", "Close"]].copy()
            if df is not None:
                df_merge = df.merge(df3, on='SYMBOL', how='left')
                df_merge.rename(columns={"Close_x": "Close", "Close_y": "Last Day Price"}, inplace=True)
                df_merge["Last Day Price"] = round(df_merge["Last Day Price"], 2)
                final = pd.concat([final, df_merge])
        except IndexError as e:
            print("index error")
sorted_final = final.sort_values(by='Volume', ascending=False)
merge_df = sorted_final.merge(source_df, on='SYMBOL', how="left")
merge_df["Date"] = merge_df["Date"].dt.date
dailysheet = gs.worksheet("DailyUpdate")
dailysheet.clear()
set_with_dataframe(dailysheet, merge_df)
