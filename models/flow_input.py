from typing import Type, Dict, Any
from dataclasses import dataclass


@dataclass
class FlowInput:
    """
    Standardized input container for workflow parameters.
    
    The FlowInput structure separates the concern of identifying the operation type
    from the specific parameters, allowing workflows to be more generic and reusable
    across different data sources and query types.
    
    Attributes:
        type (str): String identifier for the query/operation type, used by
                   QueryFactory to create appropriate query instances
        args (Dict[str, Any]): Dictionary containing all parameters specific
                              to the query type
    """
    query_type: str
    extract_strategy: str
    args: Dict[str, Any]

    def __init__(self, query_type: str, extract_strategy:str, args: Dict[str, Any]) -> None:
        self.query_type = query_type
        self.extract_strategy = extract_strategy
        self.args = args