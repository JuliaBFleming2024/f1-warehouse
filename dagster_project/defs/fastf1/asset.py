from dagster import AssetExecutionContext, DynamicPartitionsDefinition
from dagster_dlt import dlt_assets, DagsterDltResource
from dlt import pipeline
from .source import fastf1_source


partitions = DynamicPartitionsDefinition(name="year")


@dlt_assets(
    dlt_source=fastf1_source(),
    dlt_pipeline=pipeline(
        pipeline_name="fastf1",
        destination="postgres",
    ),
    partitions_def=partitions,
    group_name="fastf1",
)
def fastf1_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(
        context=context,
        dlt_source=fastf1_source(partition_key=context.partition_key),
    )
