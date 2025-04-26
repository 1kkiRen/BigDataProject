from pathlib import Path
import psycopg2 as psql
from datasets import load_dataset
from pprint import pprint

SQL_DIR = Path("sql")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# DB_HOST = hadoop-04.uni.innopolis.ru
# DB_USER = team29
# DB_NAME = team29_projectdb

DB_HOST = "localhost"
DB_USER = "myuser"
DB_NAME = "mydatabase"

DB_PORT = 5432


# TODO fill None cells
# TODO add pipeline to put new data (if so, file for preprocessing)

def connect():
	"""Establish a database connection."""
	#TODO: file with password
	# file = os.path.join("secrets", ".psql.pass")
	# with open(file, "r") as file:
	# 	password = file.read().rstrip()

	password = "mypassword"

	conn_string = (
		f"host={DB_HOST} "
		f"port={DB_PORT} "
		f"user={DB_USER} "
		f"dbname={DB_NAME} "
		f"password={password}"
	)

	conn = psql.connect(conn_string)
	print("Connected!")
	return conn


def create_tables(conn):
	cur = conn.cursor()
	with open(SQL_DIR / "create_tables.sql") as file:
		content = file.read()
		cur.execute(content)
	conn.commit()

	print("Tables created!")


def import_data(conn):
	data_files = ["stations.csv", "records.csv"]

	# Check that files exist
	for file in data_files:
		if not (DATA_DIR / file).exists():
			raise FileNotFoundError("Not found:", file)

	# Read commands
	with open(SQL_DIR / "import_data.sql") as file:
		commands = file.readlines()

	cur = conn.cursor()

	# Put all data
	for cmd, file in zip(commands, data_files):
		with open((DATA_DIR / file), "r") as data_file:
			cur.copy_expert(cmd, data_file)
		print(file)

	conn.commit()
	print("Data imported!")


def test_db(conn):
	cur = conn.cursor()
	with open(SQL_DIR / "test_database.sql") as file:
		commands = file.readlines()

		for command in commands:
			cur.execute(command)
			pprint(cur.fetchall())


def preprocess_data():
	ds = load_dataset("Geoweaver/ozone_training_data", split="train")
	print("Loaded dataset")

	# Remove useless columns
	ds = ds.remove_columns([
		"Lat_airnow", "Lon_airnow",
		"Lat_cmaq", "Lon_cmaq",
		"Latitude_y", "Longitude_y"
	])

	# raname columns
	name_mapping = {
		"StationID": "station_id",
		"Latitude_x": "latitude",
		"Longitude_x": "longitude",
		"AirNOW_O3": "airnow_ozon",
		"CMAQ12KM_O3(ppb)": "cmaq_ozon",
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
	ds = ds.rename_columns(name_mapping)

	stations = ds.select_columns(["station_id", "latitude", "longitude"]).to_pandas()
	records = ds.remove_columns(["latitude", "longitude"]).to_pandas()

	stations = stations.drop_duplicates(subset="station_id")

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
