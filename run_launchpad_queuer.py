"""
This script is used to add all launchpad workflows to the ETL queue for the Temporal ETL system to process.
The idea is that this script will be run periodically to ensure that all workflows are queued for processing.

What it does:
1. Connects to launchpad.
2. Seeks all members of the desired launchpad team.
3. Connects to the Temporal server.
4. For each member: 
    4.1 Creates a flow input for bugs.
    4.2 Creates a flow input for merge proposals.
    4.3 Creates a flow input for questions.
    4.4 Queues the workflows for each flow input.
    4.5 Logs the queued workflows ("Queued workflow for member: {member} - [{wf1}, {wf2}, {wf3}]").
5. Exits (does not wait for execution of the workflows).
"""
import os
import logging
import uuid

from launchpadlib.launchpad import Launchpad

from temporalio.client import Client

from models.etl_flow import ETLFlow
from models.flow_input import FlowInput


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


SOURCE_KIND_ID = 'launchpad'
EVENT_TYPES = [
    'bugs',
    'merge_proposals',
    'questions',
]

EVENTS_AND_STRATS = {
    'bugs': f"launchpad-bugs",
    'merge_proposals': f"launchpad-merge_proposals",
    'questions': f"launchpad-questions",
}


async def queue_workflows():
    launchpad = Launchpad.login_anonymously(
        consumer_name=LP_APP_ID,
        service_root=LP_WEB_ROOT,
        version=LP_API_VERSION
    )

    team = launchpad.people(LP_TEAM_NAME).members_details # type: ignore
    if not team:
        logger.info(f"No members found in team {LP_TEAM_NAME}. Exiting.")
        return
    logger.info(f"Found {len(team)} members in team {LP_TEAM_NAME}.")

    client = await Client.connect(TEMPORAL_HOST)

    queued_workflows = []
    
    for member in team:
        member_workflows = []
        
        for event_type, strategy in EVENTS_AND_STRATS.items():
            input = FlowInput(
                query_type=SOURCE_KIND_ID,
                extract_strategy=strategy,
                args={
                    "application_name": LP_APP_ID,
                    "service_root": LP_WEB_ROOT,
                    "version": LP_API_VERSION,
                    "member": member.name,
                    "data_date_start": FROM_DATE,
                    "data_date_end": TO_DATE,
                    "event_type": event_type,
                    "source_kind_id": SOURCE_KIND_ID,
                }
            )

            workflow_id = f"{SOURCE_KIND_ID}-{event_type}-{member.name}-{uuid.uuid4()}"
            
            # Start the workflow without waiting for it to complete
            await client.start_workflow(
                ETLFlow.run,  # Assuming ETLFlow.run is your workflow method
                input,
                id=workflow_id,
                task_queue="etl-task-queue",  # Replace with your actual task queue name
            )
            
            member_workflows.append(workflow_id)
        
        queued_workflows.extend(member_workflows)
        logger.info(f"Queued workflow for member: {member.name} - {member_workflows}")
    
    logger.info(f"Total workflows queued: {len(queued_workflows)}")


if __name__ == "__main__":
    LP_APP_ID = os.getenv("LP_APP_ID", "my-app")
    LP_WEB_ROOT = os.getenv("LP_WEB_ROOT", "production")
    LP_API_VERSION = os.getenv("LP_API_VERSION", "devel")
    LP_TEAM_NAME = os.getenv("LP_TEAM_NAME", "my-team")

    FROM_DATE = os.getenv("FROM_DATE", "2023-01-01")
    TO_DATE = os.getenv("TO_DATE", "2023-03-31")

    TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")

    import asyncio
    asyncio.run(queue_workflows())