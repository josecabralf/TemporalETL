import logging
from datetime import timedelta
from typing import Dict, List, Any

from temporalio import workflow

from models.query import QueryFactory


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@workflow.defn
class ETLFlow:
    """
    Temporal workflow for processing Launchpad data through an ETL pipeline.
    """
    queue_name: str

    @staticmethod
    def get_activities() -> List[Any]:
        """
        Return a list of activity functions to register with the Temporal worker.
        
        The activities should be defined in the same file as the workflow class
        or imported from related modules.
        
        Returns:
            List of activity function references that will be registered
            with the Temporal worker.
            
        Raises:
            NotImplementedError: If not implemented by concrete class.
            
        Example:
            @staticmethod
            def get_activities() -> List[Any]:
                return [extract_data, transform_data, load_data]
        """
        raise NotImplementedError("Subclasses must implement get_activities() method.")

    
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