#!/bin/bash

# Stop the script if any command fails
set -e

# Change to the project directory
cd "$(dirname "$0")"

# File paths
DATA_DIR="C:/Users/mulan/OneDrive/Desktop/MADE/MADE_Project_DhavalM_23230953/data"
DB_FILE="${DATA_DIR}/data.db"
WEATHER_FILE="${DATA_DIR}/weather_data.csv"
TAXI_TABLE="taxi_data"
WEATHER_TABLE="weather_data"

# Step 1: Run the pipeline
echo "Running the pipeline..."
python pipeline.py  # Ensure 'python' is in PATH

# Step 2: Check if database file exists
if [[ -f "$DB_FILE" ]]; then
    echo "Database file found: $DB_FILE"
else
    echo "Error: Database file not found!"
    exit 1
fi

# Step 3: Validate SQLite tables
echo "Validating database content..."
sqlite3 "$DB_FILE" "SELECT name FROM sqlite_master WHERE type='table';" | grep -q "$TAXI_TABLE"
if [[ $? -eq 0 ]]; then
    echo "Taxi table exists in the database."
else
    echo "Error: Taxi table not found in the database!"
    exit 1
fi

sqlite3 "$DB_FILE" "SELECT name FROM sqlite_master WHERE type='table';" | grep -q "$WEATHER_TABLE"
if [[ $? -eq 0 ]]; then
    echo "Weather table exists in the database."
else
    echo "Error: Weather table not found in the database!"
    exit 1
fi

# Step 4: Check if weather data file exists
if [[ -f "$WEATHER_FILE" ]]; then
    echo "Weather data file found: $WEATHER_FILE"
else
    echo "Error: Weather data file not found!"
    exit 1
fi

echo "All tests passed successfully!"
