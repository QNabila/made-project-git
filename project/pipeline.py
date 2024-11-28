import os
import pandas as pd
import requests
import sqlite3
# Directory setup
DATA_DIR = "/data"
os.makedirs(DATA_DIR, exist_ok=True)

# Data sources
ATTENDANCE_URL = "https://data.cityofnewyork.us/api/views/gqq2-hgxd/rows.csv?accessType=DOWNLOAD"
ATTENDANCE_FILE = f"{DATA_DIR}/attendance.csv"
VACCINATION_URL = "https://data.cityofnewyork.us/api/views/q5xz-reje/rows.csv?accessType=DOWNLOAD"
VACCINATION_FILE = f"{DATA_DIR}/vaccination.csv"
DB_FILE = f"{DATA_DIR}/attendance_vaccination.db"

def download_file(url, file_path):
    """Download a file from the given URL."""
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_path, "wb") as file:
            file.write(response.content)
        print(f"Downloaded: {file_path}")
    else:
        raise Exception(f"Failed to download file from {url}")

def process_attendance_data(file_path):
    """Process the school attendance data."""
    # Load the CSV file
    data = pd.read_csv(file_path)
    print("Attendance Columns:", data.columns)

    # Select and rename relevant columns
    data = data.rename(columns={
        "School Year": "school_year",
        "DBN": "dbn",
        "Chronic Absenteeism %": "chronic_absenteeism_rate",
    })

    # Handle missing or inconsistent values
    data = data.dropna(subset=["dbn", "chronic_absenteeism_rate"])
    data["chronic_absenteeism_rate"] = (
        data["chronic_absenteeism_rate"]
        .str.replace("%", "")
        .astype(float)
    )

    return data[["dbn", "school_year", "chronic_absenteeism_rate"]]

def process_vaccination_data(file_path):
    """Process the student vaccination data."""
    # Load the CSV file
    data = pd.read_csv(file_path)
    print("Vaccination Columns:", data.columns)

    # Select and rename relevant columns
    data = data.rename(columns={
        "DBN": "dbn",
        "Partially Vaccinated %": "partially_vaccinated_rate",
        "Fully Vaccinated %": "fully_vaccinated_rate",
    })

    # Handle missing or inconsistent values
    data = data.dropna(subset=["dbn", "partially_vaccinated_rate", "fully_vaccinated_rate"])
    data["partially_vaccinated_rate"] = (
        data["partially_vaccinated_rate"]
        .str.replace("%", "")
        .astype(float)
    )
    data["fully_vaccinated_rate"] = (
        data["fully_vaccinated_rate"]
        .str.replace("%", "")
        .astype(float)
    )

    return data[["dbn", "partially_vaccinated_rate", "fully_vaccinated_rate"]]

def merge_data(attendance_data, vaccination_data):
    """Merge attendance and vaccination data on DBN."""
    merged = pd.merge(attendance_data, vaccination_data, on="dbn", how="inner")
    return merged

def save_to_database(data, db_path, table_name):
    """Save the merged data to an SQLite database."""
    conn = sqlite3.connect(db_path)
    data.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
    print(f"Data saved to database: {db_path}, table: {table_name}")

def main():
    print("Starting pipeline...")

    # Step 1: Download datasets
    print("Downloading attendance data...")
    download_file(ATTENDANCE_URL, ATTENDANCE_FILE)

    print("Downloading vaccination data...")
    download_file(VACCINATION_URL, VACCINATION_FILE)

    # Step 2: Process datasets
    print("Processing attendance data...")
    attendance_data = process_attendance_data(ATTENDANCE_FILE)

    print("Processing vaccination data...")
    vaccination_data = process_vaccination_data(VACCINATION_FILE)

    # Step 3: Merge datasets
    print("Merging datasets...")
    merged_data = merge_data(attendance_data, vaccination_data)

    # Step 4: Save merged data to database
    print("Saving merged data to database...")
    save_to_database(merged_data, DB_FILE, "attendance_vaccination_data")

    print("Pipeline completed successfully!")

if __name__ == "__main__":
    main()
