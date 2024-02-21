import requests
from zipfile import ZipFile
import pandas as pd

data_dict = {}


def extract():
    
    
    global data_dict
    
    NSE_URL = "https://api.shoonya.com/NSE_symbols.txt.zip"
    NFO_URL = "https://api.shoonya.com/NFO_symbols.txt.zip"

    NSE = requests.get(NSE_URL)
    NFO = requests.get(NFO_URL)
    
    NSE_filename = NSE.url[NSE_URL.rfind('/')+1:]
    NFO_filename = NFO.url[NFO_URL.rfind('/')+1:]
    
    with open(NSE_filename, 'wb') as file:
        file.write(NSE.content)
    with open(NFO_filename, 'wb') as file:
        file.write(NFO.content)


    def extract_zip_files(zip_name): 
        with ZipFile(zip_name, 'r') as zip_ref:
            zip_ref.extractall('.')
        return zip_name.replace(".zip", "")
            # return os.path.join(extract_location, zip_name.replace('.zip', ''))

    NSE_data = extract_zip_files(NSE_filename)
    NFO_data = extract_zip_files(NFO_filename)
    NSE_data = pd.read_csv(NSE_data)
    NFO_data = pd.read_csv(NFO_data)

    NFO_data["Expiry"] = pd.to_datetime(NFO_data["Expiry"])

    data_dict = {
        "NSE_data": NSE_data,
        "NFO_data": NFO_data
}
    
    return data_dict


def get_instrument_NSE(symbol):
    res =  data_dict['NSE_data'].query(f'Symbol == "{symbol}"')
    if res.empty:
        raise Exception("No instrument found for Nifty Index in NSE data")
    return res.iloc[0]

def get_instrument_NFO(symbol, side, strike):
    res =  data_dict['NFO_data'].query(f'Symbol == "{symbol}" & OptionType == "{side}" & StrikePrice == {strike}')
    res=res.sort_values("Expiry")
    return res.iloc[0]

def get_token(exchange, symbol, side=None, strike=None):
    if exchange == "NFO":
        index = get_instrument_NFO(symbol, side, strike)
    else:
        index = get_instrument_NSE(symbol)
    
        
    token = index["Token"]
    return token

def get_trading_symbol(exchange, symbol, side, strike):
    if exchange == "NFO":
        index = get_instrument_NFO(symbol, side, strike)
    else:
        index = get_instrument_NSE(symbol)
        
    TradingSymbol = index["TradingSymbol"]
    return TradingSymbol