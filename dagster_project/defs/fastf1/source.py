import fastf1
import dlt
from dagster import get_dagster_logger
from dlt import pipeline
import pandas as pd

logger = get_dagster_logger()


@dlt.source
def fastf1_source(partition_key=0):
    @dlt.resource(
        name="schedule", write_disposition="merge", primary_key=["year", "round_number"]
    )
    def events_schedule():
        logger.info(f"Loading events schedule for {partition_key} F1 season")
        raw = fastf1.get_event_schedule(int(partition_key))
        events = pd.DataFrame(raw).reset_index(drop=True)
        events["year"] = partition_key
        yield events

    return events_schedule


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="fastf1",
        destination="postgres",
    )
    load_info = pipeline.run(fastf1_source(partition_key=2020))
    print(f"Load info: {load_info}")
