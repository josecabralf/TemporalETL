#!/usr/bin/env python3
"""
Streaming ETL Worker - Run streaming ETL workflows with optimized memory usage.

This script demonstrates how to use the streaming ETL pipeline for processing
large datasets with better memory efficiency.
"""

import asyncio
import logging
from datetime import datetime, timedelta

from temporalio.client import Client
from temporalio.worker import Worker

from models.etl.streaming_etl_flow import StreamingETLFlow, StreamingConfig
from models.etl.flow_input import ETLFlowInput


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_streaming_worker():
    """Run the streaming ETL worker."""
    # Connect to Temporal
    client = await Client.connect("localhost:7233")
    
    # Create worker for streaming ETL
    worker = Worker(
        client,
        task_queue=StreamingETLFlow.queue_name,
        workflows=[StreamingETLFlow],
        activities=StreamingETLFlow.get_activities(),
    )
    
    logger.info(f"Starting streaming ETL worker on queue: {StreamingETLFlow.queue_name}")
    await worker.run()


async def run_bugs_streaming_workflow_example():
    """Example of running a streaming ETL workflow."""
    # Connect to Temporal
    client = await Client.connect("localhost:7233")
    
    # Configure streaming parameters
    config = StreamingConfig(
        extract_chunk_size=100,      # Process 100 items per chunk
        transform_batch_size=500,    # Transform 500 events per batch
        load_batch_size=1000,        # Load 1000 events per database batch
        max_concurrent_chunks=3,     # Process up to 3 chunks concurrently
        memory_threshold_mb=500      # Memory threshold for backpressure
    )
    
    # Create workflow input
    flow_input = ETLFlowInput(
        query_type="launchpad",
        extract_strategy="launchpad-bugs-streaming",
        args={
            "source_kind_id": "launchpad",
            "event_type": "bugs",
            "member": MEMBER,
            "data_date_start": "2023-09-01",
            "data_date_end": "2025-03-04",
            "application_name": "temporal-etl-streaming",
            "service_root": "production",
            "version": "devel"
        }
    )
    
    # Start workflow
    logger.info("Starting streaming ETL workflow...")
    handle = await client.start_workflow(
        StreamingETLFlow.run,
        args=[flow_input, config],
        id=f"streaming-etl-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        task_queue=StreamingETLFlow.queue_name,
    )
    
    logger.info(f"Started workflow: {handle.id}")
    
    # Wait for completion
    result = await handle.result()
    logger.info(f"Workflow completed with result: {result}")
    
    return result


async def run_questions_streaming_example():
    """Example of running a streaming ETL workflow for questions."""
    client = await Client.connect("localhost:7233")
    
    config = StreamingConfig(
        extract_chunk_size=100,       # Smaller chunks for questions (they have more nested data)
        transform_batch_size=300,
        load_batch_size=800,
        max_concurrent_chunks=2,     # Conservative concurrency for questions
        memory_threshold_mb=400
    )
    
    flow_input = ETLFlowInput(
        query_type="launchpad",
        extract_strategy="launchpad-questions-streaming",
        args={
            "source_kind_id": "launchpad",
            "event_type": "questions",
            "member": MEMBER,
            "data_date_start": "2023-01-01",
            "data_date_end": "2024-12-31",
            "application_name": "temporal-etl-questions-streaming",
            "service_root": "production",
            "version": "devel"
        }
    )
    
    logger.info("Starting streaming questions ETL workflow...")
    handle = await client.start_workflow(
        StreamingETLFlow.run,
        args=[flow_input, config],
        id=f"streaming-questions-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        task_queue=StreamingETLFlow.queue_name,
    )
    
    result = await handle.result()
    logger.info(f"Questions workflow completed: {result}")
    return result


async def run_merge_proposals_streaming_example():
    """Example of running a streaming ETL workflow for merge proposals."""
    client = await Client.connect("localhost:7233")
    
    config = StreamingConfig(
        extract_chunk_size=100,       # Even smaller chunks for merge proposals (complex nested data)
        transform_batch_size=200,
        load_batch_size=600,
        max_concurrent_chunks=2,
        memory_threshold_mb=350
    )
    
    flow_input = ETLFlowInput(
        query_type="launchpad",
        extract_strategy="launchpad-merge_proposals-streaming",
        args={
            "source_kind_id": "launchpad",
            "event_type": "merge_proposals",
            "member": MEMBER,
            "data_date_start": "2024-01-01",
            "data_date_end": "2024-12-31",
            "application_name": "temporal-etl-mp-streaming",
            "service_root": "production",
            "version": "devel"
        }
    )
    
    logger.info("Starting streaming merge proposals ETL workflow...")
    handle = await client.start_workflow(
        StreamingETLFlow.run,
        args=[flow_input, config],
        id=f"streaming-mp-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
        task_queue=StreamingETLFlow.queue_name,
    )
    
    result = await handle.result()
    logger.info(f"Merge proposals workflow completed: {result}")
    return result


async def main():
    """Main function to run worker or example workflows."""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "worker":
        await run_streaming_worker()
    elif len(sys.argv) > 1 and sys.argv[1] == "bugs":
        await run_bugs_streaming_workflow_example()
    elif len(sys.argv) > 1 and sys.argv[1] == "questions":
        await run_questions_streaming_example()
    elif len(sys.argv) > 1 and sys.argv[1] == "merge-proposals":
        await run_merge_proposals_streaming_example()
    else:
        print("Usage:")
        print("  python run_streaming_etl.py worker           # Run the streaming ETL worker")
        print("  python run_streaming_etl.py bugs             # Run bugs streaming workflow example")
        print("  python run_streaming_etl.py questions        # Run questions streaming workflow example")
        print("  python run_streaming_etl.py merge-proposals  # Run merge proposals streaming workflow example")


if __name__ == "__main__":
    MEMBER = "member_1"

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Worker stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
