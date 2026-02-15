import logging

import duckdb
import requests
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"


def download_and_convert_files():
    data_dir = Path("data") / "fhv"
    data_dir.mkdir(exist_ok=True, parents=True)

    for month in range(1, 13):
        parquet_filename = f"fhv_tripdata_2019-{month:02d}.parquet"
        parquet_filepath = data_dir / parquet_filename

        if parquet_filepath.exists():
            logger.info(f"Skipping {parquet_filename} (already exists)")
            continue

        # Download CSV.gz file
        csv_gz_filename = f"fhv_tripdata_2019-{month:02d}.csv.gz"
        csv_gz_filepath = data_dir / csv_gz_filename

        logger.info(f"Downloading {csv_gz_filename}...")
        response = requests.get(
            f"{BASE_URL}/fhv/{csv_gz_filename}", stream=True
        )
        response.raise_for_status()

        with open(csv_gz_filepath, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Converting {csv_gz_filename} to Parquet...")
        con = duckdb.connect()
        con.execute(f"""
            COPY (SELECT * FROM read_csv_auto('{csv_gz_filepath}'))
            TO '{parquet_filepath}' (FORMAT PARQUET)
        """)
        con.close()

        # Remove the CSV.gz file to save space
        csv_gz_filepath.unlink()
        logger.info(f"Completed {parquet_filename}")


if __name__ == "__main__":
    download_and_convert_files()

    logger.info("Loading FHV data into DuckDB...")
    con = duckdb.connect("taxi_rides_ny.duckdb")
    con.execute("CREATE SCHEMA IF NOT EXISTS prod")

    logger.info("Creating table prod.fhv_tripdata...")
    con.execute("""
        CREATE OR REPLACE TABLE prod.fhv_tripdata AS
        SELECT * FROM read_parquet('data/fhv/*.parquet', union_by_name=true)
    """)

    con.close()
    logger.info("Done.")
