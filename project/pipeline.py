import os
import zipfile
import pandas as pd
import sqlite3
from kaggle.api.kaggle_api_extended import KaggleApi
import requests

DATA_DIR = "C:/Users/mulan/OneDrive/Desktop/MADE/MADE_Project_DhavalM_23230953/data"
DB_FILE = os.path.join(DATA_DIR, "data.db")
KAGGLE_DATASET = "microize/newyork-yellow-taxi-trip-data-2020-2019"
DATA_ZIP_FILE = os.path.join(DATA_DIR, "newyork_taxi_data.zip")
WEATHER_DATA_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/retrievebulkdataset?&key=EWKX6KNV5Q2BQAP4XU4KATLT8&taskId=6121779de1626d8757e8255faf6e524e&zip=false"
WEATHER_FILE = os.path.join(DATA_DIR, "weather_data.csv")
REQUIRED_FILES = [
    "yellow_tripdata_2020-01.csv",
    "yellow_tripdata_2020-02.csv",
    "yellow_tripdata_2020-03.csv",
    "yellow_tripdata_2020-04.csv",
    "yellow_tripdata_2020-05.csv",
    "yellow_tripdata_2020-06.csv",
]

os.makedirs(DATA_DIR, exist_ok=True)

def download_kaggle_dataset(dataset, output_dir):
    print(f"Downloading Kaggle dataset: {dataset}")
    api = KaggleApi()
    api.authenticate()
    # Download the dataset as a zip file
    api.dataset_download_files(dataset, path=output_dir, unzip=False)
    print(f"Dataset downloaded to: {output_dir}")

def extract_specific_files(zip_file, required_files, extract_dir):
    print(f"Extracting required files from {zip_file}")
    try:
        with zipfile.ZipFile(zip_file, 'r') as zf:
            for file in required_files:
                if file in zf.namelist():
                    zf.extract(file, path=extract_dir)
                    print(f"Extracted: {file}")
                else:
                    print(f"File not found in archive: {file}")
    except zipfile.BadZipFile as e:
        print(f"Error: The zip file is corrupted or invalid. {e}")

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

def merge_and_save_taxi_data(data_dir, required_files, conn):
    print("Starting to merge taxi data files...")
    taxi_data = pd.DataFrame()  

    for file in required_files:
        file_path = os.path.join(data_dir, file)
        if os.path.exists(file_path):
            print(f"Found file: {file}")  
            try:
                df = pd.read_csv(file_path, low_memory=False)
                print(f"Loaded {file} with shape: {df.shape}") 
                taxi_data = pd.concat([taxi_data, df], ignore_index=True)
                print(f"After merging {file}, merged data shape: {taxi_data.shape}")  
            except Exception as e:
                print(f"Error processing file {file}: {e}")
        else:
            print(f"File not found: {file}")  

    
    if not taxi_data.empty:
        print("Saving merged taxi data to table 'taxi_data'")  
        taxi_data.to_sql("taxi_data", conn, if_exists="replace", index=False)
        print("Taxi data saved successfully.")  
    else:
        print("No data available to save to 'taxi_data'")  

def save_weather_data(weather_file, conn):
    """Save the weather data file as 'weather_data' table."""
    if os.path.exists(weather_file):
        print(f"Processing weather data: {weather_file}")
        try:
            # Load the weather data
            df = pd.read_csv(weather_file, low_memory=False)
            print(f"Loaded weather data with shape: {df.shape}")  
            
            # Save to SQLite
            table_name = "weather_data"
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            print(f"Weather data saved to table {table_name}")
        except Exception as e:
            print(f"Error processing weather data: {e}")
    else:
        print(f"Weather data file not found: {weather_file}")

def main():
    download_kaggle_dataset(KAGGLE_DATASET, DATA_DIR)

    zip_file_path = os.path.join(DATA_DIR, "newyork-yellow-taxi-trip-data-2020-2019.zip")
    extract_specific_files(zip_file_path, REQUIRED_FILES, DATA_DIR)

    download_weather_data(WEATHER_DATA_URL, WEATHER_FILE)

    conn = sqlite3.connect(DB_FILE)
    merge_and_save_taxi_data(DATA_DIR, REQUIRED_FILES, conn)

    save_weather_data(WEATHER_FILE, conn)
    
    conn.close()
    print(f"All data saved to SQLite database at {DB_FILE}")

if __name__ == "__main__":
    main()
