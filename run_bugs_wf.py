import asyncio
import uuid
import os

from temporalio.client import Client

from models.etl.flow import ETLFlow
from models.etl.flow_input import ETLFlowInput

async def main():
    # Connect to Temporal server
    client = await Client.connect(TEMPORAL_HOST)
    
    # Define multiple workflow configurations
    workflow_configs = [
        {
            "application_name": LP_APP_ID,
            "service_root": LP_WEB_ROOT,
            "version": "devel",
            "member": MEMBER,
            "data_date_start": FROM_DATE,
            "data_date_end": TO_DATE,

            "source_kind_id": SOURCE_KIND_ID,
            "event_type": EVENT_TYPE,
        }
    ]
    
    # Start multiple workflows
    workflow_handles = []
    
    for i, config in enumerate(workflow_configs):
        workflow_id = f"launchpad-workflow-{i+1}-{uuid.uuid4()}"
        
        input = ETLFlowInput(
            query_type=config["source_kind_id"],
            extract_strategy=EXTRACT_STRATEGY,
            args=config
        )
        
        handle = await client.start_workflow(
            workflow=ETLFlow.run,
            args=(input,),
            id=workflow_id,
            task_queue=ETLFlow.queue_name,
        )
        
        workflow_handles.append(handle)
        print(f"Started workflow {i+1} with ID: {workflow_id}")
    
    # Wait for all workflows to complete
    print(f"\nWaiting for {len(workflow_handles)} workflows to complete...")
    
    results = await asyncio.gather(*[handle.result() for handle in workflow_handles])
    
    print("\nAll workflows completed!")
    for i, result in enumerate(results):
        print(f"Workflow {i+1} result: {result}")


if __name__ == "__main__":
    LP_APP_ID = os.getenv("LP_APP_ID", "my-app")
    LP_WEB_ROOT = os.getenv("LP_WEB_ROOT", "production")
    LP_API_VERSION = os.getenv("LP_API_VERSION", "devel")

    FROM_DATE = os.getenv("FROM_DATE", "2023-01-01")
    TO_DATE = os.getenv("TO_DATE", "2023-03-31")

    TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")

    MEMBER = 'member_1'

    EVENT_TYPE = 'bugs'
    SOURCE_KIND_ID = 'launchpad'
    EXTRACT_STRATEGY = 'launchpad-bugs'

    print(f"Starting workflows with parameters:\n"
          f"  Application ID: {LP_APP_ID}\n"
          f"  Service Root: {LP_WEB_ROOT}\n"
          f"  Date Range: {FROM_DATE} to {TO_DATE}\n"
          f"  Temporal Host: {TEMPORAL_HOST}\n"
          f"  Member: {MEMBER}\n"
          f"  Event Type: {EVENT_TYPE}\n"
          f"  Source Kind ID: {SOURCE_KIND_ID}\n"
          f"  Task Queue: {ETLFlow.queue_name}\n")

    asyncio.run(main())