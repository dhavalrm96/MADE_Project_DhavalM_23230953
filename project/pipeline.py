import os
import pandas as pd
import sqlite3
from kaggle.api.kaggle_api_extended import KaggleApi
import requests

# Constants
DATA_DIR = "C:/Users/mulan/OneDrive/Desktop/MADE/MADE_Project_DhavalM_23230953/data"
DB_FILE = os.path.join(DATA_DIR, "data.db")
KAGGLE_DATASET = "microize/newyork-yellow-taxi-trip-data-2020-2019"
WEATHER_DATA_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=EWKX6KNV5Q2BQAP4XU4KATLT8&taskId=ac3d670f558e7e7a7ec766dcbf2b5711&zip=false"
WEATHER_FILE = os.path.join(DATA_DIR, "weather_data.csv")
REQUIRED_FILES = [f"yellow_tripdata_2019-{str(i).zfill(2)}.csv" for i in range(1, 13)]

os.makedirs(DATA_DIR, exist_ok=True)

# 1. Download all CSV files
def download_kaggle_dataset(dataset, output_dir):
    print(f"Downloading Kaggle dataset: {dataset}")
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset, path=output_dir, unzip=True)
    print(f"Dataset downloaded and extracted to: {output_dir}")

# 2. Process files in chunks and merge results
def process_csv_files_in_chunks(data_dir, required_files):
    print("Processing all CSV files in chunks...")
    result_dfs = []
    for file in required_files:
        file_path = os.path.join(data_dir, file)
        if os.path.exists(file_path):
            print(f"Processing file: {file}")
            chunks = []
            for chunk in pd.read_csv(file_path, chunksize=500000, usecols=['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count'], low_memory=False):
                # Calculate trip time and extract the date
                chunk['tpep_pickup_datetime'] = pd.to_datetime(chunk['tpep_pickup_datetime'])
                chunk['tpep_dropoff_datetime'] = pd.to_datetime(chunk['tpep_dropoff_datetime'])
                chunk['trip_time_minutes'] = (chunk['tpep_dropoff_datetime'] - chunk['tpep_pickup_datetime']).dt.total_seconds() / 60
                chunk['date'] = chunk['tpep_pickup_datetime'].dt.date

                # Append the processed chunk
                chunks.append(chunk)

            # Combine chunks and group by date
            file_df = pd.concat(chunks)
            grouped = file_df.groupby('date').agg(
                total_passenger_count=('passenger_count', 'sum'),
                total_trip=('date', 'size'),
                average_trip_time=('trip_time_minutes', 'mean')
            ).reset_index()
            result_dfs.append(grouped)
        else:
            print(f"File not found: {file}")
    
    # Combine results from all files
    final_df = pd.concat(result_dfs, ignore_index=True)
    final_df = final_df.groupby('date').agg(
        total_passenger_count=('total_passenger_count', 'sum'),
        total_trip=('total_trip', 'sum'),
        average_trip_time=('average_trip_time', 'mean')
    ).reset_index()
    print(f"All files processed. Final DataFrame shape: {final_df.shape}")
    return final_df

# 3. Save the processed data to the SQLite database
def save_taxi_data_to_database(grouped_df, conn):
    print("Saving taxi data to SQLite database...")
    grouped_df.to_sql("taxi_data", conn, if_exists="replace", index=False)
    print("Taxi data saved successfully.")

# 4. Download and save weather data
def download_weather_data(url, output_file):
    print(f"Downloading weather data from {url}")
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(output_file, "wb") as file:
            file.write(response.content)
        print(f"Weather data saved to {output_file}")
    except Exception as e:
        print(f"Error downloading weather data: {e}")

def save_weather_data(weather_file, conn):
    if os.path.exists(weather_file):
        print(f"Processing weather data: {weather_file}")
        try:
            df = pd.read_csv(weather_file, low_memory=False)
            print(f"Loaded weather data with shape: {df.shape}")
            df.to_sql("weather_data", conn, if_exists="replace", index=False)
            print("Weather data saved successfully.")
        except Exception as e:
            print(f"Error processing weather data: {e}")
    else:
        print(f"Weather data file not found: {weather_file}")

# Main Pipeline
def main():
    # Step 1: Download data
    download_kaggle_dataset(KAGGLE_DATASET, DATA_DIR)

    # Step 2: Process all taxi files in chunks
    grouped_df = process_csv_files_in_chunks(DATA_DIR, REQUIRED_FILES)

    # Step 3: Save taxi data to the database
    conn = sqlite3.connect(DB_FILE)
    save_taxi_data_to_database(grouped_df, conn)

    # Step 4: Download and save weather data
    download_weather_data(WEATHER_DATA_URL, WEATHER_FILE)
    save_weather_data(WEATHER_FILE, conn)

    conn.close()
    print(f"All data saved to SQLite database at {DB_FILE}")

if __name__ == "__main__":
    main()
