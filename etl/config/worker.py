import os


class WorkerConfig:
    """Configuration class for the Temporal worker."""

    def __init__(self):
        self.host = os.getenv("TEMPORAL_HOST", "localhost:7233")
        if not self.host:
            raise ValueError(
                "TEMPORAL_HOST environment variable is required for Temporal worker"
            )

        self.namespace = os.getenv("TEMPORAL_NAMESPACE", "default")
        self.queue = os.getenv("TEMPORAL_QUEUE", "etl-queue")
