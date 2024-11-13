import os
import requests
import zipfile
import sqlite3
import pandas as pd


DATA_DIR = '../data'  
ZIP_FILE_PATH = os.path.join(DATA_DIR, 'dataset.zip')
DB_PATH = os.path.join(DATA_DIR, 'data.sqlite')  
DATA_URL = 'https://prod-dcd-datasets-cache-zipfiles.s3.eu-west-1.amazonaws.com/dzz48mvjht-1.zip'


os.makedirs(DATA_DIR, exist_ok=True)


def download_data(url, save_path):
    print("Downloading dataset...")
    response = requests.get(url, stream=True)
    with open(save_path, 'wb') as file:
        file.write(response.content)
    print("Download complete.")

def unzip_data(zip_path, extract_to):
    print("Extracting data...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    print("Extraction complete.")

def find_csv_file(data_dir):
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith('.csv'):
                return os.path.join(root, file)
    return None

def load_and_clean_data(csv_file):
    print("Loading and cleaning data...")
    df = pd.read_csv(csv_file)
    df.dropna(inplace=True)
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    print("Data loaded and cleaned.")
    return df

def save_to_sqlite(df, db_path):
    print("Saving data to SQLite database...")
    conn = sqlite3.connect(db_path)
    df.to_sql('cardio_data', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Data saved to SQLite at {db_path}.")

def main():
    download_data(DATA_URL, ZIP_FILE_PATH)
    unzip_data(ZIP_FILE_PATH, DATA_DIR)
    csv_file_path = find_csv_file(DATA_DIR)
    if not csv_file_path:
        print("No CSV file found in the extracted data.")
        return

    df = load_and_clean_data(csv_file_path)
    save_to_sqlite(df, DB_PATH)
    os.remove(ZIP_FILE_PATH)

if __name__ == '__main__':
    main()
