#-AssetGenie----------------------------------------------------------

# Developed by GyRo (Maciel Giroletti Mottin)

# About this Script
# This script was developed to get data from the list stock assets on a daily basis
# The data gathered by this script will be appended in a main database in S3 Bucket

# Version 0.10 Features
# - Get assets data from yfinance and create a data frame with all the information

# Import 
import yfinance as yf
import pandas as pd

# Define the list of assets
b3_list = ['PETR4.SA', 'USIM5.SA', 'ELET3.SA']
list_lenght = len(b3_list)

# Create a list to store the data of every asset
df_list = []

def get_history():

    for i in range(list_lenght):

        #get the asset history for the determined period and transform it into a data frame
        asset = yf.Ticker(b3_list[i])
        hist = asset.history(period="today")
        temp_df = pd.DataFrame(hist)

        # Add the asset Ticker in the data frame
        temp_df['Asset_Ticker'] = str(b3_list[i])

        # Append the temp_df into df_list
        df_list.append(temp_df)

# Execute the function to get the history of the assets
get_history()

# Create the final data frame
final_df = pd.concat(df_list)
print(final_df)

