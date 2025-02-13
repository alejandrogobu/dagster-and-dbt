from io import BytesIO
import pandas as pd
import requests
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset, EnvVar
from dagster_duckdb import DuckDBResource
from dagster_aws.s3 import S3Resource
from ..partitions import monthly_partition
import os
import requests
import pandas as pd
from io import BytesIO
from dagster import MetadataValue, MaterializeResult, AssetExecutionContext
from dagster_aws.s3 import S3Resource

@asset(
    partitions_def=monthly_partition,
    group_name="raw_files",
    compute_kind="DuckDB",
)
def taxi_trips_file(context: AssetExecutionContext, r2: S3Resource) -> MaterializeResult:
    """Fetch NYC taxi trip data, count rows, and upload it directly to R2 without saving locally."""

    # Obtener la fecha de la partición
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]  # Formato YYYY-MM

    # Descargar el archivo Parquet
    response = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )
    response.raise_for_status()  # Asegurar que la solicitud fue exitosa

    # Crear un objeto BytesIO para manejar el archivo en memoria
    parquet_data = BytesIO(response.content)

    # Contar registros sin escribir en disco
    num_rows = len(pd.read_parquet(parquet_data))

    # **Reiniciar el cursor antes de subirlo a R2**
    parquet_data.seek(0)

    # Subir el archivo a R2 directamente desde memoria
    s3_client = r2.get_client()
    s3_client.upload_fileobj(parquet_data, os.getenv("R2_BUCKET"), f"taxi_trips_{month_to_fetch}.parquet")

    return MaterializeResult(metadata={"Number of records": MetadataValue.int(num_rows)})


@asset(
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition,
    group_name="ingested",
    compute_kind="DuckDB",
)
def taxi_trips(context: AssetExecutionContext, database:DuckDBResource):
    """El conjunto de datos de viajes en taxi, cargado en una base de datos MotherDuck, particionado por mes."""
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]  # YYYY-MM
    bucket_name = os.getenv("R2_BUCKET")
    file_path = f"r2://{bucket_name}/taxi_trips_{month_to_fetch}.parquet"
    query = f"""
        CREATE TABLE IF NOT EXISTS trips (
            vendor_id INTEGER,
            pickup_zone_id INTEGER,
            dropoff_zone_id INTEGER,
            rate_code_id DOUBLE,
            payment_type INTEGER,
            dropoff_datetime TIMESTAMP,
            pickup_datetime TIMESTAMP,
            trip_distance DOUBLE,
            passenger_count DOUBLE,
            total_amount DOUBLE,
            partition_date VARCHAR
        );

        DELETE FROM trips WHERE partition_date = '{month_to_fetch}';

        INSERT INTO trips
        SELECT
            VendorID,
            PULocationID,
            DOLocationID,
            RatecodeID,
            payment_type,
            tpep_dropoff_datetime,
            tpep_pickup_datetime,
            trip_distance,
            passenger_count,
            total_amount,
            '{month_to_fetch}' AS partition_date
        FROM '{file_path}';
    """

    # Ejecutar la consulta
    with database.get_connection() as conn:
        conn.execute(query)