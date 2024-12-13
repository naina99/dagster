from dagster import Definitions, asset, external_asset_from_spec, AssetSpec, define_asset_job
import subprocess
import psutil
import time

@asset
def start_spark_streaming(context):
    command = "../../../../Downloads/spark-3.0.0-bin-hadoop2.7/bin/spark-submit --master local --class spark.streaming.Test /Users/A117983734/code/poc/spark/spark-streaming-java-examples/target/spark-streaming-examples-1.0.jar"

    result = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE)
    pid = result.pid
    context.log.info("Spark streaming job started")
    context.log.info(result)
    context.log.info(pid)
    time.sleep(60)
    if(result.poll() == 1):
        raise Exception(f"Process with PID {pid} is not running.")

    # This operation completes immediately after starting the stream
    return "Spark stream started"

streaming_target = AssetSpec("streaming_target", deps=[start_spark_streaming])
spark_streaming = define_asset_job(name="spark_streaming")

#Define any assets updated by the streaming pipeline as external assets;
defs = Definitions(
    assets=[external_asset_from_spec(streaming_target), start_spark_streaming],
    jobs = [spark_streaming]
)

