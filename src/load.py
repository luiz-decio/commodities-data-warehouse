import pandas as pd
import sqlite3
import os
import time
from loguru import logger
from extract import get_commodities_data_all 
from transform import transform_extract_data, process_sell_data, merge_data

start_time = time.time()

# Define the commodities codes
commodities = ["CL=F", "GC=F", "SI=F"] # Crude Oil, Gold, Silver

sell_file_path = 'data\external\comodities_sell.csv'


def dw_create(merged_df):
    os.makedirs('data/dw', exist_ok=True)
    conn = sqlite3.connect('data/dw/dw_commodities.db')
    merged_df.to_sql('commodities', conn, if_exists='replace', index=False)
    conn.close()

    logger.info(f'Data Warehouse successfully updated.')
    logger.info("--- %s seconds ---" %(time.time() - start_time))

if __name__ == "__main__":
    all_comodities_data = get_commodities_data_all(commodities)
    transformed_extract_data = transform_extract_data(all_comodities_data)
    transformed_sell_data = process_sell_data(sell_file_path)

    merged_final_data = merge_data(transformed_extract_data, transformed_sell_data)

    dw_create(merged_final_data)