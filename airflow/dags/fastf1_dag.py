from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import fastf1
import pandas as pd
import dlt
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_f1_schedule(**context):
    """
    Load F1 schedule data from 2020 to current year.
    dlt handles incremental loading automatically.
    """
    START_YEAR = 2020
    current_year = datetime.now().year
    
    logger.info(f"Loading F1 schedule from {START_YEAR} to {current_year}")
    
    @dlt.resource(
        name="schedule",
        write_disposition="merge",
        primary_key=["year", "round_number"]
    )
    def events_schedule():
        for year in range(START_YEAR, current_year + 1):
            try:
                logger.info(f"Fetching year {year}")
                schedule = fastf1.get_event_schedule(year)
                
                if schedule is not None and len(schedule) > 0:
                    events = pd.DataFrame(schedule).reset_index(drop=True)
                    events["year"] = year
                    logger.info(f"✓ Found {len(events)} events for {year}")
                    yield events
                else:
                    logger.info(f"✗ No data for year {year}")
                    
            except Exception as e:
                logger.warning(f"✗ Error fetching year {year}: {str(e)}")
                # If current year fails, that's expected - just skip it
                if year == current_year:
                    logger.info(f"Current year {year} not available yet")
                continue
    
    # Run dlt pipeline - merge handles upserts automatically
    pipeline = dlt.pipeline(
        pipeline_name="fastf1_to_postgres",
        destination="snowflake",
        dataset_name="f1_data"
    )
    
    load_info = pipeline.run(events_schedule())
    logger.info(f"Pipeline completed: {load_info}")
    
    return "Load complete"


with DAG(
    'fastf1_schedule_sync',
    default_args=default_args,
    description='Sync F1 schedule data to Snowflake with merge',
    schedule_interval='@daily',
    catchup=False,
    tags=['f1', 'postgres', 'ingest'],
) as dag:
    
    sync_task = PythonOperator(
        task_id='sync_f1_schedule',
        python_callable=load_f1_schedule,
        provide_context=True,
    )