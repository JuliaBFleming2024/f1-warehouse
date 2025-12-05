import fastf1
import dlt
import datetime
from dagster import get_dagster_logger

logger = get_dagster_logger()

current_year = datetime.date.today().year

## all the events per year
@dlt.resource(table_name="Events")
def events_schedule(year):
    
    year_cursor = dlt.sources.incremental(
        "year", initial_value=2018
    )
    for year in range(year_cursor.last_value, current_year+1):

        print(f"Loading Events Schedule For {year} Season")
        schedule =fastf1.get_event_schedule(year)
        yield schedule

pipeline = dlt.pipeline(
    pipeline_name="fastf1",
    destination="postgres", 
    dataset_name="raw_f1_data"
)

pipeline.run(events_schedule(2025))