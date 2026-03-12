import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# PSX Bank Stocks (Yahoo Finance tickers)
PSX_TICKERS = [
    'HBL.KA',    # Habib Bank Limited
    'UBL.KA',    # United Bank Limited
    'MCB.KA',    # MCB Bank
    'ABL.KA',    # Allied Bank Limited
    'BAFL.KA',   # Bank Alfalah Limited
    'MEBL.KA',   # Meezan Bank Limited
    'NBP.KA',    # National Bank of Pakistan
    'BAHL.KA',   # Bank Al Habib Limited
    'AKBL.KA',   # Askari Bank Limited
    'BOP.KA',    # Bank of Punjab
    'BOK.KA',    # Bank of Khyber
    'BIPL.KA',   # Bank Islami Pakistan Limited
    'FABL.KA',   # Faysal Bank Limited
    'SNBL.KA',   # Soneri Bank Limited
    'JSBL.KA',   # JS Bank Limited
    'SBL.KA',    # Samba Bank Limited
    'SILK.KA',   # Silk Bank Limited
    'SMBL.KA',   # Summit Bank Limited
    'SCBPL.KA',  # Standard Chartered Bank (Pakistan) Limited
    'HMB.KA'     # Habib Metropolitan Bank Limited
]

def fetch_stock_data(ticker, period='5y'):
    """Fetch historical stock data from Yahoo Finance"""
    print(f"Fetching data for {ticker}...")
    stock = yf.Ticker(ticker)
    df = stock.history(period=period)
    df.reset_index(inplace=True)
    df['Ticker'] = ticker
    df['Date'] = df['Date'].astype(str)
    return df

def save_locally(df, filename):
    """Save dataframe as CSV to local data/raw folder"""
    os.makedirs('data/raw', exist_ok=True)
    filepath = f'data/raw/{filename}'
    df.to_csv(filepath, index=False)
    print(f"Saved {filename} to {filepath}")

def run_ingestion():
    all_data = []
    for ticker in PSX_TICKERS:
        try:
            df = fetch_stock_data(ticker)
            all_data.append(df)
            print(f"✅ {ticker}: {len(df)} records fetched")
        except Exception as e:
            print(f"❌ {ticker} failed: {e}")

    combined = pd.concat(all_data, ignore_index=True)
    timestamp = datetime.now().strftime('%Y%m%d')
    filename = f"psx_raw_{timestamp}.csv"
    save_locally(combined, filename)
    print(f"\nTotal records: {len(combined)}")
    return combined

if __name__ == "__main__":
    run_ingestion()