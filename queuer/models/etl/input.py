from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class ETLInput:
    """Standardized input container for workflow parameters."""
    job_id: str # Prevents jobs from duplicating
    query_type: str
    args: Dict[str, Any]
