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


def load_data():
	# Downloaded dataset from HF: https://huggingface.co/datasets/Geoweaver/ozone_training_data
	df = pd.read_csv(DATA_DIR / "training_data.csv")

	df.drop(columns=[
		"Lat_airnow", "Lon_airnow",
		"Lat_cmaq", "Lon_cmaq",
		"Latitude_y", "Longitude_y"
	], inplace=True)

	df.rename(columns=NAME_MAPPING, inplace=True)
	return df


def check_on(records):
	query_parts = []  # List of query conditions

	def add_query_condition(column_name, condition_string):
		if column_name not in records.columns:
			print(f"Warning: Column '{column_name}' not in dataset. Skipping this condition.")
			return

		query_parts.append(condition_string)

	# Add conditions for each constraint
	add_query_condition("airnow_ozone", "airnow_ozone >= 0")
	add_query_condition("cmaq_ozone", "cmaq_ozone >= 0")
	add_query_condition("cmaq_no2", "cmaq_no2 >= 0")
	add_query_condition("cmaq_co", "cmaq_co >= 0")
	add_query_condition("cmaq_oc", "cmaq_oc >= 0")
	add_query_condition("pressure", "pressure > 0")
	add_query_condition("pbl", "pbl >= 0")
	add_query_condition("temperature", "temperature > 0")
	add_query_condition("wind_speed", "wind_speed >= 0")

	add_query_condition("wind_direction", "(wind_direction >= 0 & wind_direction <= 360)")
	add_query_condition("radiation", "radiation >= 0")
	add_query_condition("cloud_fraction", "(cloud_fraction >= 0 & cloud_fraction <= 1)")

	if query_parts:
		full_query = " & ".join(query_parts)
		records = records.query(full_query, engine='python')
	return records


def preprocess_data():
	ds = load_data()
	print("Dataset Prepared!")

	stations = ds[["station_id", "latitude", "longitude"]]
	records = ds.drop(columns=["latitude", "longitude"])

	# Remove duplicates
	stations = stations.drop_duplicates(subset="station_id")
	records = records.drop_duplicates(subset=["station_id", "month", "day", "hour"])

	# Remove data that not follow constraints
	records = check_on(records)

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
	except Exception as e:
		print("Error:", e)
		conn.rollback()
	finally:
		conn.close()


if __name__ == '__main__':
	main()
