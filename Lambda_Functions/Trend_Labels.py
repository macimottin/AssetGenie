import awswrangler as wr
import pandas as pd
import datetime

# Defining the function to determine the trend
def determine_trend(row):
    if row['plus_dm'] > row['minus_dm']:
        return 'Uptrend'
    elif row['minus_dm'] > row['plus_dm']:
        return 'Downtrend'
    else:
        return 'Stable'

# Defining the function to determine the trend strength
def determine_trend_strength(adx):
    if adx < 25:
        return 'Absent or Weak Trend'
    elif 25 <= adx < 50:
        return 'Strong Trend'
    elif 50 <= adx < 75:
        return 'Very Strong Trend'
    else:
        return 'Extremely Strong Trend'

# Applying the functions to create new columns
latest_records['trend'] = latest_records.apply(determine_trend, axis=1)
latest_records['trend_strength'] = latest_records['ADX'].apply(determine_trend_strength)

def lambda_handler(event, context):
    # Specify your S3 bucket and file details
    bucket = 'asset.genie'
    input_file_key = 'scout/RSI_ADX_Calc.csv'
    output_file_key = 'scout/latest_trend.csv'

    # Read CSV file from S3
    df = wr.s3.read_csv(f's3://{bucket}/{input_file_key}')

    # Converting date to datetime for proper sorting
    df['date'] = pd.to_datetime(df['date'])

    # Filtering for the latest record for each stock
    latest_records = df.sort_values(by=['symbol', 'date']).groupby('symbol').last().reset_index()
