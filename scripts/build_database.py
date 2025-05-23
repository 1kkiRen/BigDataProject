"""
Module for managing and processing data, including database operations and data preprocessing.

This module connects to a PostgreSQL database, creates necessary tables, imports data,
and preprocess dataset. It includes functions to handle database connections, execute SQL commands,
and ensure data integrity before loading it into the database.

Modules:
    sys: For system-specific parameters and functions.
    pathlib.Path: For handling filesystem paths.
    pandas as pd: For data manipulation and analysis.
    psycopg2 as psql: For PostgreSQL database operations.

Constants:
    SQL_DIR (Path): Directory path for SQL scripts.
    DATA_DIR (Path): Directory path for data files.
    DB_HOST (str): Database host address.
    DB_USER (str): Database username.
    DB_NAME (str): Database name.
    DB_PORT (int): Database port number.
    NAME_MAPPING (dict): Mapping of column names for dataset renaming.

Functions:
    connect(): Establishes a connection to the PostgreSQL database using credentials from a file.
    create_tables(conn): Creates the necessary tables in the database by executing an SQL script.
    import_data(conn): Imports data from CSV files into the database using SQL commands.
    test_db(conn): Executes test queries on the database and prints the results.
    load_data(): Loads the dataset from a CSV file, drops unnecessary columns, and renames columns.
    check_on(records): Filters the dataset based on specified conditions to ensure data integrity.
    preprocess_data(): Preprocesses the dataset by loading, cleaning, and saving it to CSV files.
    main(): The main function to execute the data processing and database operations workflow.

Usage:
    This module is intended to be run as a script.
    It connects to the database, preprocesses the data,
    creates the necessary tables, imports the data, and handles any exceptions that may occur.
"""

import sys
from pathlib import Path

import pandas as pd
import psycopg2 as psql
from huggingface_hub import hf_hub_download

SQL_DIR = Path("sql")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

DB_HOST = "hadoop-04.uni.innopolis.ru"
DB_USER = "team29"
DB_NAME = "team29_projectdb"
DB_PORT = 5432

NAME_MAPPING = {
    "StationID": "station_id",
    "Latitude_x": "latitude",
    "Longitude_x": "longitude",
    "AirNOW_O3": "airnow_ozone",
    "CMAQ12KM_O3(ppb)": "cmaq_ozone",
    "CMAQ12KM_NO2(ppb)": "cmaq_no2",
    "CMAQ12KM_CO(ppm)": "cmaq_co",
    "CMAQ_OC(ug/m3)": "cmaq_oc",
    "PRSFC(Pa)": "pressure",
    "PBL(m)": "pbl",
    "TEMP2(K)": "temperature",
    "WSPD10(m/s)": "wind_speed",
    "WDIR10(degree)": "wind_direction",
    "RGRND(W/m2)": "radiation",
    "CFRAC": "cloud_fraction",
    "month": "month",
    "day": "day",
    "hours": "hour",
}


def connect():
    """
    Establish a database connection.
    :return conn: Connection to the PostgreSQL database.
    """
    file = Path("secrets") / ".psql.pass"
    try:
        with open(file, "r", encoding="utf-8") as file:
            password = file.read().rstrip()
    except FileNotFoundError:
        print("Secret file .psql.pass not found!")
        password = input("Enter password:\n>")

    conn_string = (
        f"host={DB_HOST} port={DB_PORT} "
        f"dbname={DB_NAME} user={DB_USER} "
        f"password={password}"
    )

    try:
        conn = psql.connect(conn_string)
    except psql.Error as err:
        print("Connection error:", err)
        sys.exit(1)

    print("Connected!")
    return conn


def create_tables(conn):
    """
    Calls SQL commands to create tables.
    :param conn: Connection to database.
    """
    cur = conn.cursor()
    with open(SQL_DIR / "create_tables.sql", encoding="utf-8") as file:
        cur.execute(file.read())
    conn.commit()

    print("Tables created!")


def import_data(conn):
    """
    Calls SQL commands to import data into Postgres.
    :param conn: Connection to database.
    """
    data_files = ["stations.csv", "records.csv"]

    # Check files exist
    for file in data_files:
        if not (DATA_DIR / file).exists():
            raise FileNotFoundError("Not found:", file)

    # Read commands
    with open(SQL_DIR / "import_data.sql", encoding="utf-8") as file:
        commands = [cmd.strip() for cmd in file.read().split(';') if cmd.strip()]

    cur = conn.cursor()
    # Put all data
    for cmd, filename in zip(commands, data_files):
        with open(DATA_DIR / filename, "r", encoding="utf-8") as data_file:
            cur.copy_expert(cmd, data_file)

    conn.commit()
    cur.close()
    print("Data imported!")


def test_db(conn):
    """
    Test SQL queries completed successfully.
    :param conn: Connection to database.
    """
    print("Testing!")
    with open(SQL_DIR / "test_database.sql", encoding="utf-8") as file:
        commands = file.readlines()

        for command in commands:
            result = pd.read_sql(command, conn)
            print(result, end="\n\n")


def load_data() -> pd.DataFrame:
    """
    Loads dataset from CSV file and drops redundant columns.
    :return: DataFrame of whole dataset.
    """
    # Downloaded dataset from HF: https://huggingface.co/datasets/Geoweaver/ozone_training_data
    # dataset = pd.read_csv(DATA_DIR / "training_data.csv")
    # dataset = pd.read_csv("hf://datasets/Geoweaver/ozone_training_data/training_data.csv")
    filepath = hf_hub_download("datasets/Geoweaver/ozone_training_data", "training_data.csv")
    dataset = pd.read_csv(filepath)

    dataset.drop(columns=[
        "Lat_airnow", "Lon_airnow",
        "Lat_cmaq", "Lon_cmaq",
        "Latitude_y", "Longitude_y"
    ], inplace=True)

    dataset.rename(columns=NAME_MAPPING, inplace=True)
    print("Dataset Prepared!")
    return dataset


def apply_constraints(records) -> pd.DataFrame:
    """
    Check that column values are following set constraints.
    :param records: DataFrame of records.
    :return: processed DataFrame of records.
    """

    constraints = [
        "airnow_ozone >= 0",
        "cmaq_ozone >= 0",
        "cmaq_no2 >= 0",
        "cmaq_co >= 0",
        "cmaq_oc >= 0",
        "pressure > 0",
        "pbl >= 0",
        "temperature > 0",
        "wind_speed >= 0",
        "wind_direction >= 0 & wind_direction <= 360",
        "radiation >= 0",
        "cloud_fraction >= 0 & cloud_fraction <= 1",
    ]

    full_query = " & ".join(constraints)
    records = records.query(full_query, engine='python')

    return records


def preprocess_data(force: bool):
    """
    Preprocess the dataset before SQL ingestion.
    Rename columns for better readability.
    Drop redundant, duplicated and not following constraints columns.
    Split data and saves into two CSV files.
    :param force: Whether to overwrite existing CSV files.
    """

    # Check if preprocessed files are already exists.
    # force == True will preprocess data in any case.
    if not force and (DATA_DIR / "stations.csv").exists() and (DATA_DIR / "records.csv").exists():
        print("Data already processed! Use processed files.")
        return

    dataset = load_data()

    stations = dataset[["station_id", "latitude", "longitude"]]
    records = dataset.drop(columns=["latitude", "longitude"])

    # Remove duplicates
    stations = stations.drop_duplicates(subset="station_id")
    records = records.drop_duplicates(subset=["station_id", "month", "day", "hour"])

    # Remove data that not follow constraints
    records = apply_constraints(records)

    # Save to csv
    stations.to_csv(DATA_DIR / "stations.csv", index=False)
    records.to_csv(DATA_DIR / "records.csv", index=False)
    print("Data saved in csv!")


def main():
    """Main function."""

    # Step 1: Preprocess data
    preprocess_data(False)

    # Establish connection
    conn = connect()

    try:
        create_tables(conn)  # Step 2: Create tables
        import_data(conn)  # Step 3: Ingest data
        test_db(conn)  # Step 4: Test
    except (psql.Error, OSError) as err:
        print("Error:", err)
        conn.rollback()
    finally:
        conn.close()


if __name__ == '__main__':
    main()
