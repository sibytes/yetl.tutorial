from yetl.flow import (
    yetl_flow,
    IDataflow,
    IContext,
    Timeslice,
    TimesliceUtcNow,
    Save,
)
from pyspark.sql.functions import *
from typing import Type

_PROJECT = "demo"
_PIPELINE_NAME = "landing_to_raw"

@yetl_flow(project=_PROJECT, pipeline_name=_PIPELINE_NAME)
def landing_to_raw(
    table: str,
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:
    """Load raw delta tables"""

    source_table = f"{_PROJECT}_landing.{table}"
    df = dataflow.source_df(source_table)

    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )

    destination_table = f"{_PROJECT}_raw.{table}"
    dataflow.destination_df(destination_table, df, save=save)
