#-S3 testing----------------------

# Import Packages
import yfinance as yf
import pandas as pd

def lambda_handler(event, context):

    feedback = "S3 testing sucessfull"
    return feedback

# Get historical data for PETR4 stock
petr4 = yf.Ticker("PETR4.SA")
hist = petr4.history(period="ytd")

# Transform 'hist' into a pandas df
petr4_df = pd.DataFrame(hist)

# send PETR4 as .csv file to S3 bucket
petr4_df.to_csv('s3://asset.genie/S3_testing/PETR4_hist.csv', index=False)