import os
import duckdb
access_key = os.getenv('R2_AWS_ACCESS_KEY_ID')
secret_key = os.getenv('R2_AWS_SECRET_ACCESS_KEY')
account_id = os.getenv('R2_ACCOUNT_ID')
duckdb_connection = os.getenv('DUCKDB_DATABASE')
conn = duckdb.connect(duckdb_connection)
conn.sql("CREATE SCHEMA IF NOT EXISTS RAW;")
conn.sql(f"CREATE SECRET IF NOT EXISTS IN MOTHERDUCK (TYPE R2, KEY_ID '{access_key}', SECRET '{secret_key}', ACCOUNT_ID'{account_id}')");
conn.sql("INSTALL httpfs;")
conn.sql("LOAD httpfs;")
conn.close()
