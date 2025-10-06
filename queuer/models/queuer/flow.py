from datetime import timedelta
from typing import Any, Dict, List

from temporalio import activity, workflow

from external.temporal.client import TemporalClient

from models.logger import logger
from models.etl.input import ETLInput
from models.queuer.input import QueuerInput
from models.queuer.inputs_strategy import TransformInputsStrategy
from models.queuer.params_strategy import ExtractParamsStrategy


@workflow.defn
class QueuerFlow:
    """Workflow to queue ETL jobs based on input parameters."""

    @staticmethod
    def get_activities():
        return [get_metadata, extract_parameters, create_jobs]

    @workflow.run
    async def run(self, input: QueuerInput) -> Dict[str, Any]:
        metadata = await workflow.execute_activity(
            get_metadata,
            input,
            start_to_close_timeout=timedelta(minutes=1),
        )
        logger.info(
            f"Queuer flow {workflow.info().workflow_id} metadata: {
                '\n  ' + '\n  '.join(f'{k}: {v}' for k, v in metadata.items())
            }"
        )

        summary = {
            **metadata,
            "workflow_id": workflow.info().workflow_id,
            "queued_workflows": 0,
        }

        params = await workflow.execute_activity(
            extract_parameters,
            input,
            start_to_close_timeout=timedelta(hours=1),
        )
        logger.info(f"Extracted parameters for: {summary['workflow_id']}")

        queued_count = await workflow.execute_activity(
            create_jobs,
            params,
            start_to_close_timeout=timedelta(hours=3),
        )
        summary["queued_workflows"] = queued_count

        return summary


@activity.defn
async def get_metadata(input: QueuerInput) -> Dict[str, Any]:
    return {
        "source_kind_id": input.source_kind_id,
        "date_start": input.date_start,
        "date_end": input.date_end,
    }


@activity.defn
async def extract_parameters(input: QueuerInput) -> Dict[str, Any]:
    params_cmd = ExtractParamsStrategy.create(input.source_kind_id)
    return params_cmd(input)


@activity.defn
async def create_jobs(params: Dict[str, Any]) -> int:
    inputs_cmd = TransformInputsStrategy.create(params["source_kind_id"])
    queued_count = await TemporalClient.queue_jobs(params, inputs_cmd)
    logger.info(f"Total workflows queued: {queued_count}")

    return queued_count
