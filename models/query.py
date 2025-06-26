from dataclasses import dataclass
from importlib import import_module
from typing import Dict, Any


@dataclass
class Query:
    """
    Abstract base class for all ETL query operations.
    
    Query objects encapsulate the parameters and logic needed to extract data 
    from specific sources like Launchpad APIs, databases, or other systems.
    """
    source_kind_id: str
    event_type: str

    def __init__(self, source_kind_id: str, event_type: str) -> None:
        """
        Initialize the query with source kind and event type.
        
        Args:
            source_kind_id: Identifier for the data source kind (e.g., "launchpad")
            event_type: Type of event being queried (e.g., "bug", "question")
        """
        self.source_kind_id = source_kind_id
        self.event_type = event_type

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Query":
        """
        Create a query instance from a dictionary of parameters.
        
        Args:
            data: Dictionary containing query-specific parameters and configuration
            
        Returns:
            Fully configured query instance ready for execution
            
        Raises:
            NotImplementedError: If not implemented by concrete class
            ValueError: If required parameters are missing or invalid
        """
        raise NotImplementedError("Subclasses must implement from_dict() method.")

    def to_summary_base(self) -> Dict[str, Any]:
        """
        Convert the query to a summary dictionary for reporting and logging.
        
        Returns:
            Dictionary containing summarized query information suitable for logging
            
        Raises:
            NotImplementedError: If not implemented by concrete class
        """
        raise NotImplementedError("Subclasses must implement to_summary_base() method.")


class QueryFactory:
    """
    Factory class for creating query instances dynamically based on type identifiers.
    
    Class Attributes:
        queryTypes (Dict[str, str]): Registry mapping query type names to
                                   their module.class paths
    """

    # Registry of available query types mapped to their module paths
    # Format: "QueryTypeName": "module.path.ClassName"
    queryTypes: Dict[str, str] = {
        # identifier    # class module path
        "launchpad":    "launchpad.query.LaunchpadQuery",
        # Add additional query types here as they are implemented
    }

    @staticmethod
    def create(query_type: str, args: Dict[str, Any]) -> Query:
        """
        Create a query instance of the specified type with the given arguments.

        Args:
            query_type: String identifier for the query type (must be registered
                       in queryTypes dictionary)
            args: Dictionary of arguments to pass to the query's from_dict() method
            
        Returns:
            Configured query instance ready for use in ETL workflows
            
        Raises:
            ValueError: If query_type is not registered in queryTypes
            TypeError: If the loaded class is not a Query subclass
            ImportError: If the specified module cannot be imported
            AttributeError: If the specified class cannot be found in the module
        """
        if query_type not in QueryFactory.queryTypes:
            raise ValueError("Unknown query type: %s", query_type)

        module_name, class_name = QueryFactory.queryTypes[query_type].rsplit('.', 1)
        
        try:
            module = import_module(module_name)
            query_class = getattr(module, class_name)
        except ImportError as e:
            raise ImportError("Cannot import module '%s': %s", module_name, e)
        except AttributeError as e:
            raise AttributeError("Module '%s' does not have class '%s': %s", module_name, class_name, e)

        if not issubclass(query_class, Query):
            raise TypeError("Class '%s' is not a subclass of Query", query_class.__name__)

        return query_class.from_dict(args)