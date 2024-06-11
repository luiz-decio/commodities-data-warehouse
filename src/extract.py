import pandas as pd
import yfinance as yf
from loguru import logger
import time

start_time = time.time()

# Define the commodities codes
commodities = ["CL=F", "GC=F", "SI=F"] # Crude Oil, Gold, Silver
logger.add("src/logs/extract_{time}.log")

def get_commodities_data(code, period = '5d', interval = '1d'):
    """
    Receive a commoditie code as parameter 
    and returns the price of those commodities
    """
    ticker = yf.Ticker(code)
    data = ticker.history(period, interval)
    data['code'] = code

    return data
    
def get_commodities_data_all(commodities):
    """
    Concatenates all the commodities prices
    """
    all_data = []
    
    for code in commodities:
        commodities_data = get_commodities_data(code)
        all_data.append(commodities_data)

    logger.info(f'\n{all_data}')
    logger.info("--- %s seconds ---" %(time.time() - start_time))
    
    return pd.concat(all_data)

if __name__ == "__main__":
    #commodities_data = get_commodities_data("CL=F")
    get_commodities_data_all = get_commodities_data_all(commodities)