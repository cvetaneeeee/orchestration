from typing import Union
from orchestration.defs.jobs.pipelines import fixtures_pipeline_job

import dagster as dg


# @dg.schedule(cron_schedule="@daily", target="*")
# def schedules(context: dg.ScheduleEvaluationContext) -> Union[dg.RunRequest, dg.SkipReason]:
#     return dg.SkipReason("Skipping. Change this to return a RunRequest to launch a run.")


@dg.schedule(
    cron_schedule="0 2 * * *",  # every day at 2 AM
    job=fixtures_pipeline_job,
    execution_timezone="Europe/Sofia"
)
def fixtures_daily_schedule(context):
    """
    Schedule to run the fixtures pipeline every day at 2 AM EEST.
    """
    # Optionally, you can set run config or tags here
    run_config = {
        # Example: pass config if needed
        # "resources": {
        #     "config": {"some_setting": "value"}
        # }
    }
    return run_config
