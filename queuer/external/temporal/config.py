import os


class TemporalConfig:
    """Configuration class for the Temporal worker."""

    queue = os.getenv("TEMPORAL_QUEUE", "etl-queuer-queue")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    host = os.getenv("TEMPORAL_HOST", "localhost:7233")
    etl_queue = os.getenv("TEMPORAL_ETL_QUEUE", "etl-worker-queue")
