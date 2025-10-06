from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class ETLInput:
    """Standardized input container for workflow parameters."""
    query_type: str
    args: Dict[str, Any]
    job_id: str | None = None