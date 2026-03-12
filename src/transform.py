from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, min, lag, round,
    when, dayofweek, month, year,
    stddev
)
from pyspark.sql.window import Window
import os
import glob
import pandas as pd
from datetime import datetime

def get_latest_raw_file():
    """Get the latest raw CSV file from local data/raw folder"""
    files = glob.glob('data/raw/psx_raw_*.csv')
    if not files:
        raise FileNotFoundError("No raw data found. Run ingest.py first.")
    latest = sorted(files, key=os.path.getctime)[-1]
    print(f"Using file: {latest}")
    return latest

def save_processed(df, filename):
    """Save processed dataframe locally"""
    os.makedirs('data/processed', exist_ok=True)
    filepath = f'data/processed/{filename}'
    df.to_csv(filepath, index=False)
    print(f"Saved {filename} to {filepath}")

def run_transformations():
    # Start Spark 
    spark = SparkSession.builder \
        .appName("PSX_Pipeline") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Load Data 
    latest_file = get_latest_raw_file()
    df = spark.read.csv(latest_file, header=True, inferSchema=True)
    print(f"Loaded {df.count()} records")
    df.printSchema()

    # Window for time series features 
    window_ticker = Window.partitionBy('Ticker').orderBy('Date')
    window_30d = Window.partitionBy('Ticker').orderBy('Date').rowsBetween(-30, 0)

    # Feature Engineering
    df = df.withColumn('Daily_Return',
            round((col('Close') - col('Open')) / col('Open') * 100, 2)) \
        .withColumn('Price_Range',
            round(col('High') - col('Low'), 2)) \
        .withColumn('Prev_Close',
            lag('Close', 1).over(window_ticker)) \
        .withColumn('Price_Change',
            round(col('Close') - col('Prev_Close'), 2)) \
        .withColumn('MA_30',
            round(avg('Close').over(window_30d), 2)) \
        .withColumn('Volatility_30d',
            round(stddev('Close').over(window_30d), 2)) \
        .withColumn('DayOfWeek', dayofweek('Date')) \
        .withColumn('Month', month('Date')) \
        .withColumn('Year', year('Date')) \
        .withColumn('Trend',
            when(col('Daily_Return') > 0, 'Up')
            .when(col('Daily_Return') < 0, 'Down')
            .otherwise('Flat'))

    # Monthly Aggregations 
    monthly_agg = df.groupBy('Ticker', 'Year', 'Month').agg(
        round(avg('Close'), 2).alias('Avg_Close'),
        round(avg('Volume'), 0).alias('Avg_Volume'),
        round(avg('Daily_Return'), 2).alias('Avg_Return'),
        round(max('High'), 2).alias('Monthly_High'),
        round(min('Low'), 2).alias('Monthly_Low')
    )

    # Save Locally 
    processed_pd = df.toPandas()
    monthly_pd = monthly_agg.toPandas()

    save_processed(processed_pd, 'psx_processed.csv')
    save_processed(monthly_pd, 'psx_monthly_agg.csv')

    spark.stop()
    print("\n✅ Transformations complete!")
    return processed_pd, monthly_pd

if __name__ == "__main__":
    run_transformations()