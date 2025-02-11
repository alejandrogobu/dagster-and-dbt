import os
import duckdb
access_key = os.getenv('R2_AWS_ACCESS_KEY_ID')
secret_key = os.getenv('R2_AWS_SECRET_ACCESS_KEY')
account_id = os.getenv('R2_ACCOUNT_ID')
motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
conn = duckdb.connect(f"md:my_db?motherduck_token={motherduck_token}")
conn.sql(f"CREATE SECRET IF NOT EXISTS IN MOTHERDUCK (TYPE R2, KEY_ID '{access_key}', SECRET '{secret_key}', ACCOUNT_ID'{account_id}')");
conn.sql("INSTALL httpfs;")
conn.sql("LOAD httpfs;;")
conn.sql("SELECT count(*) FROM 'r2://cityofnewyork/taxi_trips_2023-01.parquet'").show()