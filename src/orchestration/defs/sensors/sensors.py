import dagster as dg
from ..jobs.pipelines import (
    odds_job, 
    # staging_job, 
    # fact_job, 
    fixtures_pipeline_job
)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[fixtures_pipeline_job],
    request_job=odds_job,
)
def trigger_odds_pipeline_from_fixtures(context):
    partition_keys = odds_job.partitions_def.get_partition_keys()
    return [
        dg.RunRequest(run_key=f"{context.dagster_run.run_id}_{pk}", partition_key=pk)
        for pk in partition_keys
    ]



# @dg.asset_sensor(asset_key=dg.AssetKey("upsert_odds_asset"), job=staging_job)
# def trigger_staging_pipeline(asset_context, event_log_entry):
#     dagster_event = event_log_entry.dagster_event

#     # Only trigger on materializations
#     if dagster_event.event_type_value != "ASSET_MATERIALIZATION":
#         return None

#     # Partition key if available
#     partition_key = getattr(
#         dagster_event.event_specific_data,
#         "partition_key",
#         None
#     )

#     # Use the EventLogEntry timestamp, not dagster_event.timestamp
#     run_key = partition_key or f"{dagster_event.asset_key.path[-1]}_{event_log_entry.timestamp}"

#     return dg.RunRequest(run_key=str(run_key))


# @dg.asset_sensor(asset_key=dg.AssetKey("stage_odds_data"), job=fact_job)
# def trigger_fact_pipeline(asset_context, event_log_entry):
#     dagster_event = event_log_entry.dagster_event

#     # Only trigger on materializations
#     if dagster_event.event_type_value != "ASSET_MATERIALIZATION":
#         return None

#     # Partition key if available
#     partition_key = getattr(
#         dagster_event.event_specific_data,
#         "partition_key",
#         None
#     )

#     # Use the EventLogEntry timestamp, not dagster_event.timestamp
#     run_key = partition_key or f"{dagster_event.asset_key.path[-1]}_{event_log_entry.timestamp}"

#     return dg.RunRequest(run_key=str(run_key))

