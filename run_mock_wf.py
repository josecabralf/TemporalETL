import asyncio
import uuid
import os

from temporalio.client import Client

from models.flow_input import FlowInput

from launchpad.query import LaunchpadQuery
from launchpad.flows.mock import MockFlow

async def main():
    # Connect to Temporal server
    client = await Client.connect(TEMPORAL_HOST)
    
    # Define multiple workflow configurations
    workflow_configs = [
        {
            "application_name": LP_APP_ID,
            "service_root": "production",
            "version": "devel",
            "member": "member_1",
            "data_date_start": "2023-01-01",
            "data_date_end": "2023-03-31"
        },
        {
            "application_name": LP_APP_ID,
            "service_root": "production",
            "version": "devel",
            "member": "member_2",
            "data_date_start": "2023-04-01",
            "data_date_end": "2023-06-30"
        },
        {
            "application_name": LP_APP_ID,
            "service_root": "production",
            "version": "devel",
            "member": "member_3",
            "data_date_start": "2023-07-01",
            "data_date_end": "2023-09-30"
        },
        {
            "application_name": LP_APP_ID,
            "service_root": "production",
            "version": "devel",
            "member": "member_4",
            "data_date_start": "2023-10-01",
            "data_date_end": "2023-12-31"
        }
    ]
    
    # Start multiple workflows
    workflow_handles = []
    
    for i, config in enumerate(workflow_configs):
        workflow_id = f"launchpad-workflow-{i+1}-{uuid.uuid4()}"
        
        input = FlowInput(
            type=LaunchpadQuery,
            args=config
        )
        
        handle = await client.start_workflow(
            workflow=MockFlow.run,
            args=(input,),
            id=workflow_id,
            task_queue="launchpad-mock-task-queue",
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
    TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")

    asyncio.run(main())