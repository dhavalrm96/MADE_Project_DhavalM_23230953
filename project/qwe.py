import sqlite3
import pandas as pd

# Path to the SQLite database
DB_FILE = "C:/Users/mulan/OneDrive/Desktop/MADE/MADE_Project_DhavalM_23230953/data/data.db"

# Connect to the database
conn = sqlite3.connect(DB_FILE)

# Query the data
query = "SELECT * FROM taxi_data_grouped LIMIT 10"
df = pd.read_sql_query(query, conn)

# Display the results
print(df)

# Close the connection
conn.close()
