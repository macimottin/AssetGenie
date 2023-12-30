import awswrangler as wr
import pandas as pd
import datetime

# Defining the function to determine the trend
def determine_trend(row):
    difference = abs(row['plus_dm'] - row['minus_dm'])

    if difference < 5:
        return 'Reversing'
    elif difference >= 5:
        if row['plus_dm'] > row['minus_dm']:
            return 'Uptrend'
        else:
            return 'Downtrend'

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

    # Applying the functions to create new columns
    latest_records['trend'] = latest_records.apply(determine_trend, axis=1)
    latest_records['trend_strength'] = latest_records['ADX'].apply(determine_trend_strength)

    # Write the updated dataframe to a new CSV in S3
    wr.s3.to_csv(latest_records, f's3://{bucket}/{output_file_key}', index=False)

    return {
            'statusCode': 200,
            'body': f'trend and trend_strengh columns were calculated with latest data of each stock and added to the file {output_file_key}'
        }
