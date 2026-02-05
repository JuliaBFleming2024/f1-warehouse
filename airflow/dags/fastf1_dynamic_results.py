from datetime import datetime

import pendulum
import fastf1
import dlt
import pandas as pd

from airflow.decorators import dag, task


def convert_durations_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts timedelta64[ns] columns to string to avoid NaN casting issues.
    """
    for col_name, dtype in df.dtypes.items():
        if pd.api.types.is_timedelta64_dtype(dtype):
            ns_float = df[col_name].dt.total_seconds() * 1e9
            df[col_name] = ns_float.astype(str)
    return df


@dag(
    dag_id="fastf1_dynamic_results",
    start_date=datetime(2024, 1, 1),
    schedule="0 * * * *",
    catchup=True,
    tags=["f1", "dlt", "postgres"],
)
def fastf1_dynamic_results():

    SEASON_YEAR = 2025

    @task
    def get_schedule_for_season(year: int) -> list[dict]:
        raw = fastf1.get_event_schedule(year)
        df = pd.DataFrame(raw).reset_index(drop=True)
        df["year"] = year
        return df.to_dict(orient="records")

    @task
    def get_completed_races(schedule: list[dict]) -> list[dict]:
        now = pendulum.now("UTC")
        completed = []

        for race in schedule:
            race_start = race.get("Session5Date") or race.get("EventDate")
            if race_start is None:
                continue

            race_start_dt = pendulum.instance(
                pd.to_datetime(race_start).to_pydatetime()
            )

            if race_start_dt <= now:
                completed.append(
                    {
                        "year": race["year"],
                        "event_name": race["EventName"],
                        "location": race.get("Location"),
                        "race_start_utc": race_start_dt.isoformat(),
                    }
                )

        return completed

    @task
    def fetch_race_results(race: dict) -> list[dict]:
        session = fastf1.get_session(
            race["year"], race["event_name"], "Race"
        )
        session.load()

        df = pd.DataFrame(session.results).reset_index(drop=True)
        df["year"] = race["year"]
        df["location"] = race.get("location") or race["event_name"]
        df["race_start_utc"] = race["race_start_utc"]

        df = convert_durations_to_string(df)
        return df.to_dict(orient="records")

    @task
    def load_to_postgres(
        schedule_rows: list[dict],
        race_results: list[list[dict]],
    ):
        schedule_df = pd.DataFrame(schedule_rows)
        results_df = pd.DataFrame(
            row for rows in race_results for row in rows
        )

        @dlt.resource(
            name="schedule",
            write_disposition="merge",
            primary_key=["year", "round_number"],
        )
        def schedule():
            yield schedule_df

        @dlt.resource(
            name="race_results",
            write_disposition="merge",
            primary_key=["year", "location", "driver_number"],
        )
        def race_results_resource(
            rows=results_df.to_dict(orient="records"),
            race_start_utc=dlt.sources.incremental(
                "race_start_utc",
                initial_value="1970-01-01T00:00:00Z",
            ),
        ):
            yield rows

        pipeline = dlt.pipeline(
            pipeline_name="fastf1_airflow_dynamic",
            destination="postgres",
        )

        pipeline.run([schedule, race_results_resource])

    schedule = get_schedule_for_season(SEASON_YEAR)
    completed = get_completed_races(schedule)
    results = fetch_race_results.expand(race=completed)
    load_to_postgres(schedule, results)


dag = fastf1_dynamic_results()
    