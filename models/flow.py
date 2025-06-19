from typing import List, Dict, Any
from temporalio import workflow


@workflow.defn
class Flow:
    """
    Abstract base class for Temporal workflow definitions.
    
    All concrete workflow implementations should inherit from this class and
    implement both the get_activities() static method and the run() workflow method.
    
    Attributes:
        queue_name (str): The name of the task queue for this workflow type.
                         This should be unique across different workflow types
                         to ensure proper task routing.
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
        Execute the workflow with the provided input parameters.
        
        Args:
            input: Dictionary containing workflow-specific parameters and configuration.
                  The structure depends on the specific workflow implementation but
                  typically includes query parameters and execution options.
                  
        Returns:
            Dictionary containing the results of workflow execution, typically
            including summary information, processed item counts, and any
            relevant metadata.
            
        Raises:
            NotImplementedError: If not implemented by concrete class.
            ActivityError: If any activity execution fails.
            WorkflowError: If workflow execution encounters an error.
        """
        raise NotImplementedError("Subclasses must implement run() method.")