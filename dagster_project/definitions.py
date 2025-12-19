from dagster import Definitions
from dagster_dlt import DagsterDltResource
from .defs.fastf1.asset import fastf1_assets

defs = Definitions(
    assets=[fastf1_assets],
    resources={
        "dlt": DagsterDltResource(),
    },
)
