from datetime import datetime, timedelta
import logging
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_duckdb import DuckDBResource
from dlt import pipeline
from dlt_sources.chicago_crimes import chicago_crimes_source
from ..partitions import daily_partition

@dlt_assets(
    dlt_source=chicago_crimes_source(),
    partitions_def=daily_partition,  # Usar particiones diarias
    dlt_pipeline=pipeline(
        pipeline_name="chicago_crimes",
        dataset_name="chicago_crimes",
        destination="motherduck",
        progress="log",
    ),
    name="chicago",
    group_name="chicago",
)
def dagster_chicago_crimes_assets(context: AssetExecutionContext, dlt: DagsterDltResource, database: DuckDBResource):
    """Ejecuta el asset usando la fecha de la partición."""
    
    # Get partition date from Dagster
    partition_date = context.partition_key  # Format: "YYYY-MM-DD"
    
    # Convert partition date to datetime object at midnight (00:00:00)
    start_datetime = datetime.strptime(partition_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0)
    
    # Calculate end of the day (23:59:59)
    end_datetime = start_datetime + timedelta(hours=23, minutes=59, seconds=59)
    
    # Format as ISO strings
    date_start_iso = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    date_end_iso = end_datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    # DELETE statement for DuckDB
    delete_query = f"DELETE FROM chicago_crimes.crimes WHERE updated_on BETWEEN '{date_start_iso}' AND '{date_end_iso}';"

    # Check if the table exists before attempting DELETE
    check_table_query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'crimes';"

    try:
        with database.get_connection() as conn:
            result = conn.execute(check_table_query).fetchone()  # Check if table exists
            if result[0] > 0:
                conn.execute(delete_query)
                logging.info(f"Deleted data from {date_start_iso} to {date_end_iso} in chicago_crimes.crimes")
            else:
                logging.info("Table chicago_crimes.crimes does not exist. Skipping delete.")
    except Exception as e:
        logging.warning(f"Error while checking/deleting data for partition {partition_date}: {e}")
    
    # Run the dlt pipeline
    yield from dlt.run(context=context, dlt_source=chicago_crimes_source(start_date=partition_date))