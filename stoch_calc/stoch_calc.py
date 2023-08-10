import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# define work directory as the directory where this script is running
work_dir = os.getcwd()

# change work directory to the directory where the script is running
os.chdir(work_dir)

# read the .csv file named as 'b3test.csv' from the work directory
stock_data = pd.read_csv('b3_db.csv')

# get today date time in format 'YYYY-MM-DD'
today = datetime.today().strftime('%Y-%m-%d')

# remove '.SA' from the Asset_Ticker column
stock_data['Asset_Ticker'] = stock_data['Asset_Ticker'].str[:-3]

# Create an empty list to get the loop results
results = []

for ticker, df in stock_data.groupby('Asset_Ticker'):

    asset_today = df[df['Date'] == today]
    asset_today = asset_today.reset_index()

    # Get last 14 Low registers disregarding today's date
    asset_14days = df.tail(14)

    # Get required data to calculate the stochastic oscillator
    close = asset_today['Close'][0]
    lowest_low   = asset_14days['Low'].min()
    highest_high = asset_14days['High'].max()

    # Calculate stochastic oscillator value for the current day
    stoch_value = ((close - lowest_low) / (highest_high - lowest_low)) * 100

    # Creating a new DataFrame with the results for this asset
    result_df = pd.DataFrame({

                            'Date': today,
                            'Ticker': ticker,
                            'Stoch_value': stoch_value

                            }, index=[0])
    
    # Append each Asset Stoch value in results list
    results.append(result_df)

# Concatenate results list into a dataframe
result_df = pd.concat(results, ignore_index=True)

# Export .csv file with result_df
result_df.to_csv('stoch_calc2.csv', index=False)

# Print message about the end of the script
print('End of the script/')
