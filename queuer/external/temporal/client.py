import uuid

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleSpec,
    ScheduleRange,
    ScheduleCalendarSpec,
)
from temporalio.worker import Worker

from external.temporal.config import TemporalConfig

from models.logger import logger
from models.date_utils import get_week_day
from models.queuer.input import QueuerInput
from models.queuer.inputs_strategy import InputMethodType


class TemporalClient:
    """API class for interacting with the Temporal server."""

    @classmethod
    async def _create_client(cls):
        return await Client.connect(
            target_host=TemporalConfig.host,
            namespace=TemporalConfig.namespace,
        )

    @classmethod
    async def create_worker(cls, workflows, activities):
        client = await cls._create_client()

        logger.info("Starting Temporal worker...")
        logger.info("Listening on task queue: %s", TemporalConfig.queue)

        return Worker(
            client=client,
            task_queue=TemporalConfig.queue,
            workflows=workflows,
            activities=activities,
        )

    @classmethod
    async def queue_jobs(cls, params: dict, inputs_cmd: InputMethodType) -> int:
        client = await cls._create_client()
        queued_workflows = 0
        for input in inputs_cmd(params):
            await client.start_workflow(
                "ETLFlow",
                input,
                id=input.job_id,
                task_queue=TemporalConfig.etl_queue,
            )
            queued_workflows += 1
        logger.info(f"Queued workflows: {queued_workflows}")

        return queued_workflows

    @classmethod
    async def create_schedule(
        cls, flow: type, source: str, day_of_week: int = 0, hour: int = 0
    ):
        client = await cls._create_client()

        input = QueuerInput(source)
        if input.date_start or input.date_end:
            input.date_start = None
            input.date_end = None
            logger.warning(
                "date_start and date_end should not be set in the QueuerInput for scheduling. "
                "They will be ignored."
            )

        logger.info(f"Creating new schedule for {input.source_kind_id}...")

        day_name = get_week_day(day_of_week)
        calendar_spec = ScheduleCalendarSpec(
            day_of_week=[ScheduleRange(day_of_week)],
            hour=[ScheduleRange(hour)],
            comment=f"Every {day_name} at {hour}:00",
        )
        logger.info(f"Calendar spec: {calendar_spec.comment}")

        await client.create_schedule(
            f"{input.source_kind_id}-schedule",
            Schedule(
                action=ScheduleActionStartWorkflow(
                    flow.run,
                    input,
                    id=f"{input.source_kind_id}-queuer-schedule",
                    task_queue=TemporalConfig.queue,
                ),
                spec=ScheduleSpec(calendars=[calendar_spec]),
            ),
        )
        logger.info(
            f"Schedule created successfully for {input.source_kind_id}. "
            f"Date start: {input.date_start}, Date end: {input.date_end}. "
            f"It will run every {day_name} at {hour}:00."
        )
