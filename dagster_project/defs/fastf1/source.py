import fastf1
import dlt
from dagster import get_dagster_logger
from dlt import pipeline
import pandas as pd


logger = get_dagster_logger()


def convert_durations_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts all columns with pandas timedelta64[ns] dtype to string type
    to bypass casting errors caused by NaNs.
    """
    for col_name, dtype in df.dtypes.items():
        if pd.api.types.is_timedelta64_dtype(dtype):
            # Convert timedelta to total nanoseconds (float64 if NaNs exist)
            nanoseconds_float = df[col_name].dt.total_seconds() * 1e9

            # Convert the resulting float/NaN series to string type
            df[col_name] = nanoseconds_float.astype(str)
    return df


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

    @dlt.resource(
        name="race_results",
        write_disposition="merge",
        # write_disposition="append"
        primary_key=["year", "location", "driver_number"],
    )
    def race_results():
        schedule = fastf1.get_event_schedule(int(partition_key))
        events = pd.DataFrame(schedule)

        for _, race in events.iterrows():
            location = race["Location"]
            event_name = race["EventName"]

            logger.info(f"Loading Race Results from {location} {partition_key}")

            session = fastf1.get_session(int(partition_key), event_name, "Race")

            session.load()
            results = pd.DataFrame(session.results).reset_index(drop=True)
            results["year"] = partition_key
            results["location"] = event_name
            yield results

    return events_schedule, race_results.add_map(convert_durations_to_string)


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="fastf1",
        destination="postgres",
    )
    load_info = pipeline.run(fastf1_source(partition_key=2020))
    print(f"Load info: {load_info}")

# TODO (incremental ingestion):
#   Replace season-based partitioning with true incremental loads driven by race timestamps.
#   Instead of iterating all events for a given year, persist the latest ingested race/session
#   timestamp and only pull races whose scheduled start time is > last_loaded_ts.
#   Apply a safety buffer (e.g., load races 24 hours after scheduled start) to ensure results
#   are finalized before ingestion.
