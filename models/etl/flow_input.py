from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class ETLFlowInput:
    """
    Standardized input container for workflow parameters.
    
    The FlowInput structure separates the concern of identifying the operation type
    from the specific parameters, allowing workflows to be more generic and reusable
    across different data sources and query types.
    """
    query_type: str
    extract_strategy: str
    args: Dict[str, Any]

    def __init__(self, query_type: str, extract_strategy:str, args: Dict[str, Any]) -> None:
        self.query_type = query_type
        self.extract_strategy = extract_strategy
        self.args = args