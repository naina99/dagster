from dagster import Definitions, AssetSpec, asset, external_asset_from_spec, AssetSpec, define_asset_job
import subprocess
import time

import os
import urllib.request

# Create a new 'nasa' directory if needed
dir_name = "nasa"
if not os.path.exists(dir_name):
    os.makedirs(dir_name)

from dagster import AssetExecutionContext, DailyPartitionsDefinition, asset, HourlyPartitionsDefinition
from datetime import datetime, timezone, timedelta

partition_interval="HOURLY"
partition_start_date = datetime.now(timezone.utc) - timedelta(days=1)


@asset(partitions_def=HourlyPartitionsDefinition(str(partition_start_date.strftime('%Y-%m-%d-%H:%M%z'))))
def my_daily_partitioned_asset(context) -> None:
    print(context.partition_time_window.start)
    #TimeWindow(start=DateTime(2024, 8, 1, 0, 0, 0, tzinfo=Timezone('UTC')), end=DateTime(2024, 8, 2, 0, 0, 0, tzinfo=Timezone('UTC')))
    # where date>=2024-08-01 00:00:00 and date<2024-08-02 00:00:00
    # batchid - convert start to batchid
    # see 15 mins interval.
    print(context.partition_key)
    #2024-08-01
    print(context.partition_keys)
    #['2024-08-01']
    print(context.partition_key_range)
    #PartitionKeyRange(start='2024-08-01', end='2024-08-01')
    partition_date_str = context.partition_key

    url = f"https://api.nasa.gov/planetary/apod?api_key=DEMO_KEY&date={partition_date_str}"
    target_location = f"nasa/{partition_date_str}.csv"

    urllib.request.urlretrieve(url, target_location)

processed_logs = AssetSpec("processed_logs")
#Define any assets updated by the streaming pipeline as external assets;
defs = Definitions(
    assets=[my_daily_partitioned_asset, processed_logs]
)

