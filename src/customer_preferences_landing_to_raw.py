from yetl.flow import (
    yetl_flow,
    IDataflow,
    IContext,
    Timeslice,
    TimesliceUtcNow,
    OverwriteSave,
    Save,
)
from pyspark.sql.functions import *
from typing import Type
import json


@yetl_flow(project="demo")
def customer_preferences_landing_to_raw(
    context: IContext,
    dataflow: IDataflow,
    timeslice: Timeslice = TimesliceUtcNow(),
    save: Type[Save] = None,
) -> dict:
    """Load the demo customer_preferences data as is into a raw delta hive registered table.
    """

    df = dataflow.source_df(f"{context.project}_landing.customer_preferences")

    context.log.info("Joining customers with customer_preferences")
    df = df.join(df, "id", "inner")
    df = df.withColumn(
        "_partition_key", date_format("_timeslice", "yyyyMMdd").cast("integer")
    )

    dataflow.destination_df(f"{context.project}_raw.customer_preferences", df, save=save)


# incremental load
# timeslice = Timeslice(2021, 1, 1)
# timeslice = Timeslice(2022, 7, 12)
# timeslice = Timeslice(2022, 7, "*")
# results = customer_preferences_landing_to_raw(timeslice=timeslice)
# print(results)

# reload load
timeslice = Timeslice(2022, "*", "*")
results = customer_preferences_landing_to_raw(timeslice=timeslice, save=OverwriteSave)
results = json.dumps(results, indent=4, default=str)
print(results)
