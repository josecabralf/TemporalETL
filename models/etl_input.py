from dataclasses import dataclass


@dataclass
class ETLInput:
    """
    Class to hold parameters for ETL workflows.

    :param query_type: Type of the query to be executed, e.g. "LaunchpadQuery", etc.
    :param args: Dictionary of arguments for the ETL workflow.
    """
    args: dict
    query_type: str

    def __init__(self, args: dict, query_type: type):
        self.args = args
        self.query_type = query_type.__name__