import awswrangler as wr
import pandas as pd
import numpy as np

def calculate_rsi(data, window=14):
    delta = data.diff()
    gain = ((delta + delta.abs()) / 2).fillna(0)
    loss = ((-delta + delta.abs()) / 2).fillna(0)

    avg_gain = gain.rolling(window=window).mean()
    avg_loss = loss.rolling(window=window).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_adx(data, window=14):
    # Copy the data to avoid SettingWithCopyWarning
    data = data.copy()

    # Ensure 'high', 'low', and 'close' are integers
    data.loc[:, 'high'] = data['high'].astype(int)
    data.loc[:, 'low'] = data['low'].astype(int)
    data.loc[:, 'close'] = data['close'].astype(int)

    high = data['high']
    low = data['low']
    close = data['close']

    # Calculate the differences
    delta_high = high.diff()
    delta_low = low.diff()

    # Initialize +DM and -DM
    plus_dm = pd.Series([0] * len(data), index=data.index)
    minus_dm = pd.Series([0] * len(data), index=data.index)

    # Calculate +DM and -DM using iterrows
    for i, row in data.iterrows():
        if i > 0:  # Skip the first row
            if delta_high.loc[i] > 0 and delta_high.loc[i] > delta_low.loc[i]:
                plus_dm.loc[i] = delta_high.loc[i]
            if delta_low.loc[i] < 0 and delta_low.loc[i] < delta_high.loc[i]:
                minus_dm.loc[i] = abs(delta_low.loc[i])

    # Calculate the True Range (TR)
    tr1 = high - low
    tr2 = abs(high - close.shift(1))
    tr3 = abs(low - close.shift(1))
    tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}, index=data.index).max(axis=1)
    atr = tr.rolling(window=window).mean()

    # Calculate +DI and -DI
    plus_di = 100 * (plus_dm.rolling(window=window).mean() / atr)
    minus_di = abs(100 * (minus_dm.rolling(window=window).mean() / atr))

    # Calculate DX and ADX
    dx = (abs(plus_di - minus_di) / abs(plus_di + minus_di)) * 100
    adx = dx.rolling(window=window).mean()

    return adx, plus_dm, minus_dm

def lambda_handler(event, context):
    # Specify your S3 bucket and file details
    bucket = 'asset.genie'
    input_file_key = 'scout/ibov_data.csv'
    output_file_key = 'scout/RSI_ADX_Calc.csv'

    # Read CSV file from S3
    df = wr.s3.read_csv(f's3://{bucket}/{input_file_key}')

    # Initialize empty columns for RSI, ADX, plus_dm, and minus_dm
    df['RSI'] = np.nan
    df['ADX'] = np.nan
    df['plus_dm'] = np.nan
    df['minus_dm'] = np.nan

# Calculate RSI, ADX, plus_dm, and minus_dm for each asset after sorting by date
    for symbol in df['symbol'].unique():
        symbol_data = df[df['symbol'] == symbol].sort_values(by='date')
        df.loc[df['symbol'] == symbol, 'RSI'] = calculate_rsi(symbol_data['close'])
        adx, plus_dm, minus_dm = calculate_adx(symbol_data[['high', 'low', 'close']])
        df.loc[df['symbol'] == symbol, 'ADX'] = adx
        df.loc[df['symbol'] == symbol, 'plus_dm'] = plus_dm
        df.loc[df['symbol'] == symbol, 'minus_dm'] = minus_dm

    # Write the updated dataframe to a new CSV in S3
    wr.s3.to_csv(df, f's3://{bucket}/{output_file_key}', index=False)

    return {
        'statusCode': 200,
        'body': f'Successfully processed and stored RSI, ADX, plus_dm, and minus_dm calculations in {output_file_key}'
    }
