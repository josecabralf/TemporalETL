import os


class TemporalConfig:
    """Configuration class for the Temporal worker."""

    queue = os.getenv("TEMPORAL_QUEUE", "etl-worker-queue")
    namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
    host = os.getenv("TEMPORAL_HOST", "localhost:7233")
