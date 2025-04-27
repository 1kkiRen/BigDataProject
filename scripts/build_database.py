from pathlib import Path

import pandas as pd
import psycopg2 as psql
from pprint import pprint

SQL_DIR = Path("sql")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

DB_HOST = "hadoop-04.uni.innopolis.ru"
DB_USER = "team29"
DB_NAME = "team29_projectdb"

# DB_HOST = "localhost"
# DB_USER = "myuser"
# DB_NAME = "mydatabase"

DB_PORT = 5432

NAME_MAPPING = {
	"StationID": "station_id",
	"Latitude_x": "latitude",
	"Longitude_x": "longitude",
	"AirNOW_O3": "airnow_ozone",
	"CMAQ12KM_O3(ppb)": "cmaq_ozone",
	"CMAQ12KM_NO2(ppb)": "cmaq_no2",
	"CMAQ12KM_CO(ppm)": "cmaq_co",
	"CMAQ_OC(ug/m3)": "cmaq_organic_carbon",
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


# TODO: fill None cells

def connect():
	"""Establish a database connection."""
	file = Path("secrets") / ".psql.pass"
	with open(file, "r") as file:
		password = file.read().rstrip()

	conn_string = (
		f"host={DB_HOST} port={DB_PORT} "
		f"dbname={DB_NAME} user={DB_USER} "
		f"password={password}"
	)

	try:
		conn = psql.connect(conn_string)
	except psql.Error as e:
		print(e)
		exit(1)

	print("Connected!")
	return conn


def create_tables(conn):
	cur = conn.cursor()
	with open(SQL_DIR / "create_tables.sql") as file:
		cur.execute(file.read())
	conn.commit()

	print("Tables created!")


def import_data(conn):
	data_files = ["stations.csv", "records.csv"]

	# Check files exist
	for file in data_files:
		if not (DATA_DIR / file).exists():
			raise FileNotFoundError("Not found:", file)

	# Read commands
	with open(SQL_DIR / "import_data.sql") as file:
		commands = [cmd.strip() for cmd in file.read().split(';') if cmd.strip()]

	cur = conn.cursor()
	# Put all data
	for cmd, file in zip(commands, data_files):
		with open((DATA_DIR / file), "r") as data_file:
			cur.copy_expert(cmd, data_file)

	conn.commit()
	print("Data imported!")


def test_db(conn):
	cur = conn.cursor()
	with open(SQL_DIR / "test_database.sql") as file:
		commands = file.readlines()

		for command in commands:
			cur.execute(command)
			pprint(cur.fetchall())


def convert_types(records):
	columns_type_swap = [
		"airnow_ozone", "cmaq_ozone", "cmaq_no2", "cmaq_co", "cmaq_organic_carbon",
		"pressure", "pbl", "temperature", "wind_speed", "wind_direction", "radiation",
	]

	for col in columns_type_swap:
		records[col] = records[col].astype(int)
	return records


def load_data():
	df = pd.read_csv(DATA_DIR / "training_data.csv")

	df.drop(columns=[
		"Lat_airnow", "Lon_airnow",
		"Lat_cmaq", "Lon_cmaq",
		"Latitude_y", "Longitude_y"
	], inplace=True)

	df.rename(columns=NAME_MAPPING, inplace=True)
	return df


def preprocess_data():
	ds = load_data()
	print("Load dataset!")

	stations = ds[["station_id", "latitude", "longitude"]]
	records = ds[ds.columns[~ds.columns.isin(["latitude", "longitude"])]]

	# Remove duplicates and convert data to int
	stations = stations.drop_duplicates(subset="station_id")
	records = records.drop_duplicates(subset=["station_id", "month", "day", "hour"])
	records = convert_types(records)

	# Save to csv
	stations.to_csv(DATA_DIR / "stations.csv", index=False)
	records.to_csv(DATA_DIR / "records.csv", index=False)
	print("Data saved in csv!")


def main():
	conn = connect()

	try:
		preprocess_data()
		create_tables(conn)
		import_data(conn)
		test_db(conn)
	except Exception as e:
		print("Error:", e)
		conn.rollback()
	finally:
		conn.close()


if __name__ == '__main__':
	main()
