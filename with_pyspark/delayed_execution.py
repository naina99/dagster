from datetime import timedelta

from dagster import asset, AutomationCondition, DailyPartitionsDefinition, Definitions, AssetExecutionContext

x = 5
# select the partition that is x days old
# We do this by selecting all partition x days old and removing all partition x-1 days old
x_day_ago = AutomationCondition.in_latest_time_window(
    timedelta(days=x)
) & ~AutomationCondition.in_latest_time_window(timedelta(days=(x-1)))

# the default on_cron implementation filters the view to just the latest time window,
# so we get rid of that and add in the x_days_ago condition
condition = x_day_ago & AutomationCondition.on_cron("*/3 * * * *").without(
    AutomationCondition.in_latest_time_window()
).with_label("delayed_cron")


@asset(
    partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
    automation_condition=condition,
)
def the_asset(context: AssetExecutionContext):
    first_partition, last_partition = context.asset_partitions_time_window_for_output(
        list(context.selected_output_names)[0]
    )
    print(f"Hello! I am running a partition of date: {first_partition}" )


defs = Definitions(
    assets=[the_asset]
)