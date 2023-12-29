import awswrangler as wr
import pandas as pd
import numpy as np

def check_and_append_data(latest_trend_df, trend_hist_df):

    # Check if the data in latest_trend_df is already in trend_hist_df
    # We use merge with an indicator to find out if the rows are unique to latest_trend_df
    combined_df = pd.merge(latest_trend_df, trend_hist_df, how='outer', indicator=True)
    new_data_df = combined_df[combined_df['_merge'] == 'left_only']

    # Drop the merge indicator column
    new_data_df = new_data_df.drop(columns=['_merge'])

    # Check if there are new rows to add
    if new_data_df.empty:
        return trend_hist_df, "No modification required."
    else:
        # Append new data to trend_hist_df
        updated_trend_hist_df = pd.concat([trend_hist_df, new_data_df])
        return updated_trend_hist_df, "History updated with latest trends."

status_message, updated_trend_hist_df.head()

def lambda_handler(event, context):
    # Specify your S3 bucket and file details
    bucket = 'asset.genie'
    input_file_1 = 'scout/latest_trend.csv'
    input_file_2 = 'scout/trend_hist.csv'
    output_file_key = 'scout/trend_hist.csv'

    # Read the input CSV files from S3
    df1 = wr.s3.read_csv(f's3://{bucket}/{input_file_1}')
    df2 = wr.s3.read_csv(f's3://{bucket}/{input_file_2}')
  
    # Check for new data and append if necessary
    updated_trend_hist_df, status_message = check_and_append_data(df1, df2)

    # Write the updated dataframe to a new CSV in S3
    wr.s3.to_csv(updated_trend_hist_df, f's3://{bucket}/{output_file_key}', index=False)

    return {
                'statusCode': 200,
                'body': f'{status_message}'
            }

