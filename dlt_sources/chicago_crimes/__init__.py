import dlt
import requests
from datetime import datetime, timedelta

@dlt.source
def chicago_crimes_source(start_date: str = None):
    """Source to extract crime data in Chicago within a 24-hour range."""

    @dlt.resource(
        table_name="crimes",
        write_disposition="append",
    )
    def get_crimes():
        """Incremental load based on a 24-hour range."""
        # Convert start date to datetime object
        start_datetime = datetime.strptime(start_date, "%Y-%m-%d")

        # Add 23 hours, 59 minutes, and 59 seconds to get the end of the day
        end_datetime = start_datetime + timedelta(hours=23, minutes=59, seconds=59)

        # Convert back to string for API request
        date_start_iso = start_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        date_end_iso = end_datetime.strftime("%Y-%m-%dT%H:%M:%S")

        # Construct the API URL with the date range
        url = f"https://data.cityofchicago.org/resource/crimes.json?$limit=20000000&$where=updated_on between '{date_start_iso}' and '{date_end_iso}'"
        print(url)

        # Make the API request
        response = requests.get(url)
        response.raise_for_status()

        yield response.json()

    return get_crimes