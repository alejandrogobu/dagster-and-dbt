from datetime import datetime, timedelta
import logging
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_duckdb import DuckDBResource
from dlt import pipeline
from dlt_sources.chicago_crimes import chicago_crimes_source
from ..partitions import daily_partition

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")

# Definimos un pipeline estático para que Dagster lo acepte
default_pipeline = pipeline(
    pipeline_name="chicago_crimes_default",  # Nombre requerido por el decorador
    dataset_name="chicago_crimes",
    destination="motherduck",
    progress="log",
)

@dlt_assets(
    dlt_source=chicago_crimes_source(),
    partitions_def=daily_partition,
    dlt_pipeline=default_pipeline,  # Se define un valor estático aquí
    name="chicago",
    group_name="chicago",
)
def dagster_chicago_crimes_assets(
    context: AssetExecutionContext, 
    dlt: DagsterDltResource, 
    database: DuckDBResource
):
    """Ejecuta el asset usando la fecha de la partición."""

    # Obtener la fecha de la partición desde Dagster
    partition_date = context.partition_key  # Formato: "YYYY-MM-DD"

    # Generar un valor aleatorio para el pipeline
    pipeline_name = f"chicago_crimes_{partition_date}"

    # Sobreescribimos el pipeline con un nombre dinámico
    dlt_pipeline = pipeline(
        pipeline_name=pipeline_name,
        dataset_name="chicago_crimes",
        destination="motherduck",
        progress="log",
    )

    logging.info(f"Ejecutando pipeline: {pipeline_name}")

    # Convertir la fecha de partición a datetime y calcular el rango del día
    start_datetime = datetime.strptime(partition_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0)
    end_datetime = start_datetime + timedelta(hours=23, minutes=59, seconds=59)

    # Formatear como cadenas ISO
    date_start_iso = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    date_end_iso = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

    # DELETE statement para eliminar datos existentes en la partición
    delete_query = f"DELETE FROM chicago_crimes.crimes WHERE updated_on BETWEEN '{date_start_iso}' AND '{date_end_iso}';"

    # Verificar si la tabla existe antes de intentar eliminar datos
    check_table_query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'crimes' and table_catalog= 'dev_db';"

    try:
        with database.get_connection() as conn:
            result = conn.execute(check_table_query).fetchone()  
            if result[0] > 0:
                conn.execute(delete_query)
                logging.info(f"Deleted data from {date_start_iso} to {date_end_iso} in chicago_crimes.crimes")
            else:
                logging.info("Table chicago_crimes.crimes does not exist. Skipping delete.")
    except Exception as e:
        logging.warning(f"Error while checking/deleting data for partition {partition_date}: {e}")
        raise 
    
    # Ejecutar el pipeline con el nombre aleatorio
    yield from dlt.run(context=context, dlt_pipeline=dlt_pipeline, dlt_source=chicago_crimes_source(start_date=partition_date))