import requests
import awswrangler as wr
import pandas as pd
from datetime import datetime, timedelta

def lambda_handler(event, context):

    # Your API key
    api_key = '088eaf16b9832d99695bda9073fa92e9'

    # Define the file path within AWS S3 for reading
    raw_s3_bucket = 'asset.genie'
    raw_path_dir = 'scout/IBOV.csv'
    raw_path = f"s3://{raw_s3_bucket}/{raw_path_dir}"

    # Load stock tickers from CSV using awswrangler
    stock_tickers_df = wr.s3.read_csv(raw_path, sep=';')
    stock_tickers = stock_tickers_df['code'].tolist()

    # Date range
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')

    # Fetch stock data
    stock_data = fetch_stock_data(api_key, stock_tickers, start_date, end_date)

    # Send final DataFrame to S3 bucket using awswrangler
    output_s3_path = "s3://asset.genie/scout/ibov_data.csv"
    wr.s3.to_csv(stock_data, output_s3_path, index=False)

    return "Data processing completed successfully"

def fetch_stock_data(api_key, tickers, start_date, end_date):
    base_url = 'http://api.marketstack.com/v1/eod'
    all_stocks_data = pd.DataFrame()

    for ticker in tickers:
        # Append .BVMF to each ticker symbol
        formatted_ticker = f"{ticker}.BVMF"
        
        params = {
            'access_key': api_key,
            'symbols': formatted_ticker,
            'date_from': start_date,
            'date_to': end_date
        }
        
        retry_count = 0
        while True:
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                stock_data = pd.json_normalize(data, 'data')
                all_stocks_data = pd.concat([all_stocks_data, stock_data], ignore_index=True)
                break
            elif response.status_code == 429 and retry_count < 1:
                print(f"Rate limit exceeded for {formatted_ticker}. Retrying after 100ms.")
                time.sleep(0.1)
                retry_count += 1
            else:
                print(f"Failed to fetch data for {formatted_ticker}. Status Code: {response.status_code}")
                print("Response:", response.text)
                break
                
    return all_stocks_data
