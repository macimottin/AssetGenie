#-AssetGenie----------------------------------------------------------

# Developed by GyRo (Maciel Giroletti Mottin)

# About this Script
# This program was developed to get data from stock market and perform inteligent statistical analysis for to buy and sell decisions


import yfinance as yf
import pandas as pd

msft = yf.Ticker("PETR4.SA")
hist = msft.history(period="today")
print(hist)