import dlt
import requests
from datetime import datetime, timedelta

@dlt.source
def chicago_crimes_source(start_date: str = None):
    """Source para extraer datos de crímenes en Chicago en un rango de 24 horas."""

    @dlt.resource(
        table_name="crimes",
        write_disposition="append",
    )
    def get_crimes():
        """Carga incremental basada en un rango de 24 horas."""
        # Convert to datetime object
        start_datetime = datetime.strptime(start_date, "%Y-%m-%d")

        # Add 23 hours, 59 minutes, and 59 seconds
        end_datetime = start_datetime + timedelta(hours=23, minutes=59, seconds=59)

        # Convert back to string for API request
        date_start_iso = start_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        date_end_iso = end_datetime.strftime("%Y-%m-%dT%H:%M:%S")

        # Construir la URL con el rango de fechas
        url = f"https://data.cityofchicago.org/resource/crimes.json?$limit=20000000&$where=updated_on between '{date_start_iso}' and '{date_end_iso}'"
        print(url)
        # Hacer la solicitud a la API
        response = requests.get(url)
        response.raise_for_status()

        yield response.json()

    return get_crimes