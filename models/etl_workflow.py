import logging
from datetime import timedelta

from temporalio import workflow

from models.query import QueryFactory, IQuery


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@workflow.defn
class ETLWorkflow:
    """
    Workflow for processing Launchpad data through ETL pipeline.
    """
    @workflow.run
    async def run(self, input: dict) -> dict:
        """
        Run the workflow to scrape data from Launchpad.

        :param launchpad: The Launchpad API client instance.
        :param query: information about the data to extract.
        """
        query = QueryFactory.create(input["query_type"], args=input["args"])

        summary = query.to_summary_base()
        summary["workflow_id"] = workflow.info().workflow_id

        extracted = await workflow.execute_activity(
            "extract_data",
            query,
            start_to_close_timeout=timedelta(minutes=5),
        )

        transformed = await workflow.execute_activity(
            "transform_data",
            extracted,
            start_to_close_timeout=timedelta(seconds=30),
        )
        summary["items_processed"] = len(transformed)

        inserted = await workflow.execute_activity(
            "load_data",
            transformed,
            start_to_close_timeout=timedelta(minutes=1),
        )
        summary["items_inserted"] = inserted

        logger.info(f"Data import workflow completed: {summary}")

        return summary
