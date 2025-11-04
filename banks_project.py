# === Imports ===
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
from io import StringIO


# === Constants ===
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
csv_path = './Largest_banks_data.csv'
db_name = 'banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'


# === TASK 1: Logging Function ===
def log_progress(message):
    """Logs timestamped messages into code_log.txt"""
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp} : {message}\n")


# === TASK 2: Extraction Function ===
def extract(url, table_attribs):
    """Extracts the largest banks table from the given Wikipedia URL"""
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, 'html.parser')

    # Locate the 'By market capitalization' table (index 0)
    table = soup.find_all('table')[0]
    df = pd.read_html(StringIO(str(table)))[0]

    # Clean and rename columns
    df['Market cap (US$ billion)'] = (
        df['Market cap (US$ billion)']
        .astype(str)
        .str.replace('\n', '')
        .astype(float)
    )
    df.rename(columns={'Market cap (US$ billion)': 'MC_USD_Billion'}, inplace=True)

    log_progress("Data extraction complete. Initiating Transformation process")
    return df


# === TASK 3: Transformation Function ===
def transform(df):
    """Reads exchange rate CSV and adds converted currency columns"""
    exchange_df = pd.read_csv('./exchange_rate.csv')
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']

    # Add 3 new columns
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    log_progress("Data transformation complete. Initiating Loading process")
    return df


# === TASK 4(a): Load to CSV ===
def load_to_csv(df, output_path):
    """Saves the transformed DataFrame to a CSV file"""
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file")


# === TASK 4(b): Load to Database ===
def load_to_db(df, sql_connection, table_name):
    """Loads the DataFrame into the specified SQL table"""
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")


# === TASK 5: Run Queries ===
def run_queries(query_statement, sql_connection):
    """Executes and prints SQL queries with their outputs"""
    print("\nExecuting query:\n", query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    log_progress("Process Complete")


# === MAIN ETL PIPELINE ===
def main():
    log_progress("Preliminaries complete. Initiating ETL process")

    # Step 1: Extract
    df = extract(url, table_attribs)

    # Step 2: Transform
    df = transform(df)

    # Step 3: Load to CSV
    load_to_csv(df, csv_path)

    # Step 4: Connect to DB and Load
    log_progress("SQL Connection initiated")
    conn = sqlite3.connect(db_name)
    load_to_db(df, conn, table_name)

    # Step 5: Run Queries
    query1 = "SELECT * FROM Largest_banks"
    query2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    query3 = "SELECT [Bank Name] FROM Largest_banks LIMIT 5"

    run_queries(query1, conn)
    run_queries(query2, conn)
    run_queries(query3, conn)

    # Step 6: Close connection
    conn.close()
    log_progress("Server Connection closed")


# === RUN SCRIPT ===
if __name__ == "__main__":
    main()
