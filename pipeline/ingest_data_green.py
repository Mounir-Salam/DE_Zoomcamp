#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import click
import pyarrow.parquet as pq
import requests
from sqlalchemy import create_engine
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-password', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')

def run(pg_user, pg_password, pg_host, pg_port, pg_db, year, month, target_table):

    prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    url = f"{prefix}/green_tripdata_{year}-{month:02d}.parquet"

    # Define a local filename
    local_filename = "output_temp.parquet"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    # Step 1: Download the file
    print(f"Downloading {url}...")
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(local_filename, 'wb') as f:
            f.write(response.content)
    else:
        print(f"Error: Could not download file. Status code: {response.status_code}")
        return

    engine = create_engine(f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}")

    # Step 2: Open the local file (No more seek errors!)
    parquet_file = pq.ParquetFile(local_filename)
    print(f"File has {parquet_file.num_row_groups} row groups.")
    
    first = True
    for i in tqdm(range(parquet_file.num_row_groups)):
        df_chunk = parquet_file.read_row_group(i).to_pandas()
    
        # Optional: Manually handle types if the Parquet schema doesn't match your DB needs
        # df_chunk = df_chunk.astype(dtype) 

        if first:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
                index=False # Recommended unless you want the pandas index as a column
            )
            first = False
    
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False,
            method='multi' # Optional: Speeds up inserts for Postgres
        )
    
        print(f"Loaded {len(df_chunk)} records from row group {i} into database")

if __name__ == "__main__":
    run()