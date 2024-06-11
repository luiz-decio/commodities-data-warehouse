import pandas as pd
import time
from loguru import logger
from extract import get_commodities_data_all

start_time = time.time()

# Define the commodities codes
commodities = ["CL=F", "GC=F", "SI=F"] # Crude Oil, Gold, Silver

sell_file_path = 'data\external\comodities_sell.csv'

def transform_extract_data(extract_data: pd.DataFrame) -> pd.DataFrame:
    extract_data['date'] = pd.to_datetime(extract_data.index, utc=True).date
    return extract_data.reset_index(drop=True)

def process_sell_data(path: str):
    df_sell = pd.read_csv(path)
    df_sell = df_sell.rename(columns={'symbol': 'code'})
    df_sell['date'] = pd.to_datetime(df_sell['date']).dt.date
    return df_sell

def merge_data(comodities_data: pd.DataFrame, sell_data: pd.DataFrame):
    merged_df = comodities_data.merge(
        sell_data,
        on=['date', 'code'],
        how='inner'
    )
    merged_df['value'] = merged_df['quantity'] * merged_df['Close']
    merged_df['balance'] = merged_df.apply(
        lambda row: row['value'] if row['action'] == 'sell' else -row['value'], axis=1
    )

    logger.info(f'{len(merged_df)} final rows transformed.')
    logger.info("--- %s seconds ---" %(time.time() - start_time))

    return merged_df

if __name__ == '__main__':
    all_comodities_data = get_commodities_data_all(commodities)
    transformed_comodities_data = transform_extract_data(all_comodities_data)
    transformed_sell_data = process_sell_data(sell_file_path)
    merged_final_data = merge_data(transformed_comodities_data, transformed_sell_data)