import datetime
import pandas as pd 
import files_interact
import login
import supertrend
import tsi
import threading
import os
import argparse

data_dict = files_interact.extract()      
client=login.login()
ohlc=['into', 'inth', 'intl', 'intc']


def set_stoploss(temp, side):
    global stoploss
    current_time = temp["time"].iloc[-1]
    comparison_time = current_time.replace(hour=9, minute=25, second=0, microsecond=0)
    if current_time < comparison_time:
        if side == "up":
            stoploss = temp["inth"].iloc[-1] + 10
        else:
            stoploss = temp["intl"].iloc[-1] - 10
    else:
        if temp["inth"].iloc[-2] > temp["inth"].iloc[-3]:
            highest = temp["inth"].iloc[-2]
        else:
            highest = temp["inth"].iloc[-3]
        if temp["intl"].iloc[-2] > temp["intl"].iloc[-3]:
            lowest = temp["intl"].iloc[-2]
        else:
            lowest = temp["intl"].iloc[-3]
        stoploss = (temp["intc"].iloc[-2] + temp["into"].iloc[-3] + highest + lowest) / 4
        float(stoploss)

def append_value(dataframe, column_name, value, index):
    if index >= len(dataframe):
        new_row = pd.DataFrame([{col: (value if col == column_name else None) for col in dataframe.columns}])
        # Check if new_row is empty or has all NA values
        if not new_row.empty and not new_row.isna().all().all():
            # Exclude columns with all NA values before concatenation (optional step based on your requirement)
            new_row = new_row.dropna(axis=1, how='all')
            dataframe = pd.concat([dataframe, new_row], ignore_index=True)
    else:
        dataframe.at[index, column_name] = value
    return dataframe

def confirmation(inner, df, side):
    df_super = supertrend.SuperTrend(df, period= 17, multiplier=inner, ohlc=ohlc)
    df_tsi = df[['intc']]
    df_tsi = tsi.tsi(df_tsi)
    main_signal = False

    if (df_super["STX17_1.5"].iloc[-1] == df_super["STX17_1.5"].iloc[-2]):
        temp1 = df_tsi["TSI_13_25_13"].iloc[-1] - df_tsi["TSI_13_25_13"].iloc[-2]
        temp2 = df_tsi["TSI_13_25_13"].iloc[-2] - df_tsi["TSI_13_25_13"].iloc[-3]
        if side == "up":
            if temp1 > temp2:
                main_signal = True
                set_stoploss(df, side)
        else:
            if temp1 < temp2:
                main_signal = True
                set_stoploss(df, side)
    else:
        print("order came in waiting but got canceled")
    confirmation_waiting = False
    
    return main_signal, confirmation_waiting

def check_trade(outer, inner, df):
    global stoploss
    supertrend_signal = False
    impulse_signal = False
    tsi_waiting = False
    signal = False
    confirmation_waiting = False
    impulse_waiting = False
    
    df_ultimate = supertrend.SuperTrend(df, period= 17, multiplier=outer, ohlc=ohlc)
    df_impulse = df[['time', 'inth', 'intl', 'intc']]
    df_impulse = impulsemacd.macd(df_impulse)
    df_tsi = df[['intc']]
    df_tsi = tsi.tsi(df_tsi)
    current_time = df["time"].iloc[-1]
    comparison_time = current_time.replace(hour=15, minute=00, second=0, microsecond=0)
    bull_or_bear = "none"

    if current_time < comparison_time:        
        if (df_ultimate["STX17_3.0"].iloc[-1] != df_ultimate["STX17_3.0"].iloc[-2]):
            supertrend_signal = True
        else:
            supertrend_signal = False
        
        if (supertrend_signal == True):
            if (bull_or_bear == "up"):
                tsi_temp1 = (df_tsi["TSI_13_25_13"].iloc[-1]) - (df_tsi["TSIs_13_25_13"].iloc[-1])
                tsi_temp2 = (df_tsi["TSI_13_25_13"].iloc[-2]) - (df_tsi["TSIs_13_25_13"].iloc[-2])
                if (tsi_temp2 < 0 and tsi_temp1 > 0):
                    tsi_waiting = True
                elif (tsi_temp1 > tsi_temp2):
                    signal = True               
            else:
                tsi_temp1 = (df_tsi["TSIs_13_25_13"].iloc[-1]) - (df_tsi["TSI_13_25_13"].iloc[-1])
                tsi_temp2 = (df_tsi["TSIs_13_25_13"].iloc[-2]) - (df_tsi["TSI_13_25_13"].iloc[-2])
                if (tsi_temp2 < 0 and tsi_temp1 > 0):
                    tsi_waiting = True
                elif (tsi_temp1 > tsi_temp2):
                    signal = True

            if(impulse_waiting ==True or tsi_waiting == True):
                confirmation_waiting = True    
            elif (signal == True):
                comparison_time = current_time.replace(hour=9, minute=20, second=0, microsecond=0)
                if current_time < comparison_time:
                    confirmation_waiting = True
                else:
                    set_stoploss(df, bull_or_bear)
            
            if (confirmation_waiting == True):
                signal = False
                
    return signal, confirmation_waiting, bull_or_bear
    
def place_order(df):
    time = df["time"].iloc[-1]
    entry_price = df["into"].iloc[-1]
    main_signal = False
    order_placed = True
    return time, entry_price, main_signal, order_placed

def check_exit(df, side):
    df_exit = df[['intc']]
    df_exit = tsi.tsi(df_exit)
    exit_price = 0
    time = df["time"].iloc[-1]
    alarm = False
    order_placed = True
    order_exit = False
    
    if side == "up":
        if (df_exit["TSI_13_25_13"].iloc[-1] < df_exit["TSI_13_25_13"].iloc[-2]):
            alarm = True
            case = 2
    else:
        if (df_exit["TSI_13_25_13"].iloc[-1] > df_exit["TSI_13_25_13"].iloc[-2]):
            alarm = True
            case = 2
    if (df["inth"].iloc[-1] >= stoploss and df["intl"].iloc[-1] <= stoploss):
        alarm = True
        case = 1
    if alarm == True:
        order_placed = False
        order_exit = True
        if case == 1:
            exit_price = stoploss
        if case == 2:
            exit_price = df["intc"].iloc[-1]
    else:
        set_stoploss(df, side)
    return time, exit_price, order_placed, order_exit
    
def main():
    
    token = files_interact.get_token("NSE", "Nifty 50")
    lastBusDay = datetime.datetime.now()-datetime.timedelta(days=16)
    lastBusDay = lastBusDay.replace(hour=0, minute=0, second=0, microsecond=0)
    ret = client.get_time_price_series(exchange="NSE", token = str(int(token)), starttime=int(lastBusDay.timestamp()), interval="5")
    ret_exit = client.get_time_price_series(exchange="NSE", token = str(int(token)), starttime=int(lastBusDay.timestamp()), interval="3")
    ret = pd.DataFrame.from_dict(ret)
    ret_exit = pd.DataFrame.from_dict(ret_exit)
    ret["time"] = pd.to_datetime(ret["time"], dayfirst=True)
    ret_exit["time"] = pd.to_datetime(ret_exit["time"], dayfirst=True)
    ret.sort_values(by='time', ascending=True, inplace=True)
    ret_exit.sort_values(by='time', ascending=True, inplace=True)
    ret.reset_index(inplace=True)
    ret_exit.reset_index(inplace=True)
    for col in ohlc:
        ret[col] = ret[col].astype(float)
        ret_exit[col] = ret_exit[col].astype(float)
    temp = pd.DataFrame()
    temp_exit = pd.DataFrame()
    temp = ret.iloc[:500].copy()
    data_columns = [ 'entry_time', 'entry_price', 'exit_time', 'exit_price']
    trade_data = pd.DataFrame(columns=data_columns)
    ret = ret[500:]
    
    
    
    order_placed = False
    main_signal = False
    confirmation_waiting = False
    last_exit_index = 100
    order_counter = 0
    bull_or_bear = None
    
    for i in range(0, len(ret)):
        
        if (order_placed == True and main_signal == False and confirmation_waiting == False):
            for j in range(last_exit_index + 1, len(ret_exit)):
                if ret_exit["time"].iloc[j] <= entry_time:
                    new_rows = ret_exit.iloc[last_exit_index + 1 : j + 1]
                    temp_exit = pd.concat([temp_exit, new_rows], ignore_index=True)
                    last_exit_index = j
                else:
                    new_rows = ret_exit.iloc[last_exit_index + 1 : j + 1]
                    temp_exit = pd.concat([temp_exit, new_rows], ignore_index=True)
                    last_exit_index = j
                    break
            exit_time, exit_price, order_placed, order_exit = check_exit(temp_exit, bull_or_bear) 
            if order_exit == True:
                trade_data = append_value(trade_data, 'exit_time', exit_time, order_counter)
                trade_data = append_value(trade_data, 'exit_price', exit_price, order_counter)
                order_counter = order_counter + 1
        elif (order_placed == False and main_signal == False and confirmation_waiting == True):
            main_signal, confirmation_waiting = confirmation(1.5, temp, bull_or_bear)
        elif (order_placed == False and main_signal == True and confirmation_waiting == False):
            entry_time , entry_price, main_signal, order_placed = place_order(temp)
            trade_data = append_value(trade_data, 'entry_time', entry_time, order_counter)
            trade_data = append_value(trade_data, 'entry_price', entry_price, order_counter)
        elif (order_placed == False and main_signal == False and confirmation_waiting ==  False):
            main_signal, confirmation_waiting, bull_or_bear = check_trade(3,1.5, temp)
        
        next_row = ret.iloc[[i]]
        temp = pd.concat([temp, next_row], ignore_index=True) 
            
    
    current_directory = os.getcwd()
    df_comb_file = os.path.join(current_directory, 'testing2.csv')
    trade_data.to_csv(df_comb_file, index=False)
    
    
    # df_ultimate = supertrend.SuperTrend(ret, period= 17, multiplier=3, ohlc=ohlc)
    # df_super = supertrend.SuperTrend(ret, period= 17, multiplier=1.5, ohlc=ohlc)
    # df_impulse = ret[['time', 'inth', 'intl', 'intc']]
    # df_impulse = impulsemacd.macd(df_impulse)
    # df_tsi = ret[['intc']]
    # df_tsi = tsi.tsi(df_tsi)
    # result = pd.concat([ret, df_ultimate["STX17_3.0"], df_super["STX17_1.5"], df_impulse, df_tsi ],  axis=1)
    # result = result[500:]
    # df_comb_file = os.path.join(current_directory, 'plssss.csv')
    # result.to_csv(df_comb_file, index=False)
    
    
    
if __name__ == "__main__":
    main()
    
