import os
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Dict, Type

from models.file_utils import find_project_root
from models.logger import logger


@dataclass
class Query:
    """Abstract base class for all ETL query operations.

    It encapsulates the parameters and logic needed to extract data
    from specific sources.
    """

    source_kind_id: str
    event_type: str

    def __init__(self, source_kind_id: str, event_type: str) -> None:
        """Initialize the query with source kind and event type."""
        self.source_kind_id = source_kind_id
        self.event_type = event_type

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Query":
        """Create a query instance from a dictionary of parameters."""
        raise NotImplementedError("Subclasses must implement from_dict() method.")

    def to_summary_base(self) -> Dict[str, Any]:
        """Convert the query to a summary dictionary for reporting and logging."""
        raise NotImplementedError("Subclasses must implement to_summary_base() method.")


# Global registry for query types
_query_type_registry: Dict[str, Type["Query"]] = {}


def query_type(query_type_id: str):
    """Decorator to register query types automatically.

    Args:
        query_type_id: String identifier for the query type

    Returns:
        Decorated class that is registered in the global registry
    """

    def decorator(cls: Type) -> Type:
        # Check if the class already inherits from Query
        if not issubclass(cls, Query):
            raise TypeError(
                f"Class {cls.__name__} must inherit from Query base class to be registered as a query."
            )
        _query_type_registry[query_type_id] = cls
        return cls

    return decorator


class QueryFactory:
    """Factory class for creating query instances dynamically based on type identifiers.
    Uses decorator-based registration for automatic discovery of query types.
    """

    _project_root = None
    _modules_imported = False

    @staticmethod
    def _discover_query_directories():
        """Automatically discover all directories that might contain query modules."""
        sources_path = os.path.join(QueryFactory._project_root, "sources")  # type: ignore
        logger.info("Discovering query directories in sources path: %s", sources_path)
        query_directories = []

        # Check if sources directory exists
        if not os.path.isdir(sources_path):
            logger.warning("Sources directory not found: %s", sources_path)
            return query_directories

        # Walk through all directories in sources
        for item in os.listdir(sources_path):
            item_path = os.path.join(sources_path, item)

            # Skip hidden directories, __pycache__, .venv, etc.
            if (
                item.startswith(".")
                or item.startswith("__")
                or item in ["venv", ".venv", "node_modules"]
            ):
                continue

            if os.path.isdir(item_path):
                # Check if directory contains query.py
                query_file = os.path.join(item_path, "query.py")
                if os.path.isfile(query_file):
                    query_directories.append(f"sources.{item}")
                    logger.info("Found query module: sources.%s.query", item)

        return query_directories

    @staticmethod
    def _discover_and_import_modules():
        """Auto-discover and import all query modules to trigger decorator registration."""
        if QueryFactory._modules_imported:
            return

        if not QueryFactory._project_root:
            QueryFactory._project_root = find_project_root()

        # Dynamically discover query directories
        query_directories = QueryFactory._discover_query_directories()

        for query_module_base in query_directories:
            query_module = f"{query_module_base}.query"

            try:
                import_module(query_module)
                logger.info("Successfully imported query module: %s", query_module)
            except ImportError as e:
                logger.warning("Failed to import query module %s: %s", query_module, e)
            except Exception as e:
                logger.error("Error importing query module %s: %s", query_module, e)

        logger.info(
            f"Successfully imported {len(_query_type_registry.keys())} query modules"
        )
        QueryFactory._modules_imported = True

    @staticmethod
    def create(query_type: str, args: Dict[str, Any]) -> Query:
        """Create a query instance of the specified type with the given arguments.
        Auto-discovers and imports query modules, then checks the decorator registry.

        Args:
            query_type: String identifier for the query type (must be registered via decorator)
            args: Dictionary of arguments to pass to the query's from_dict() method

        Returns:
            Configured query instance ready for use in ETL workflows

        Raises:
            ValueError: If query_type is not registered in the decorator registry
        """
        # Auto-discover and import all query modules
        QueryFactory._discover_and_import_modules()

        # Check if the query type is registered via decorator
        if query_type in _query_type_registry:
            query_class = _query_type_registry[query_type]
            return query_class.from_dict(args)

        raise ValueError(f"Query type '{query_type}' not found in registry")
