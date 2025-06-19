import logging
from datetime import timedelta
from typing import Dict, Any

from temporalio import workflow

from models.query import QueryFactory
from models.flow import Flow


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@workflow.defn
class ETLFlow(Flow):
    """
    Temporal workflow for processing Launchpad data through an ETL pipeline.
    """
    
    @workflow.run
    async def run(self, input: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute the ETL workflow pipeline.
        
        Args:
            input: Dictionary containing workflow parameters with structure:
                - type: Query type identifier (e.g., "LaunchpadQuery")
                - args: Dictionary of query-specific arguments
                
        Returns:
            Dictionary containing workflow execution summary
                
        Raises:
            ActivityError: If any activity fails after exhausting retries
            ValueError: If input parameters are invalid or missing
        """
        query = QueryFactory.create(input["type"], args=input["args"])

        summary = query.to_summary_base()
        summary["workflow_id"] = workflow.info().workflow_id

        extracted = await workflow.execute_activity(
            "extract_data", query,
            start_to_close_timeout=timedelta(minutes=10),
        )

        transformed = await workflow.execute_activity(
            "transform_data", extracted,
            start_to_close_timeout=timedelta(minutes=1),
        )
        summary["items_processed"] = len(transformed)

        inserted = await workflow.execute_activity(
            "load_data", transformed,
            start_to_close_timeout=timedelta(minutes=3),
        )
        summary["items_inserted"] = inserted

        return summary