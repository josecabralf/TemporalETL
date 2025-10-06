from importlib import import_module
import os
import pkgutil
from typing import Any, Awaitable, Callable, Dict, List

from models.file_utils import find_project_root
from models.logger import logger


# Global registry for extract methods
ExtractMethodType = Callable[..., Awaitable[List[Dict[str, Any]]]]
_extract_method_registry: Dict[str, ExtractMethodType] = {}


def extract_method(name: str):
    """Decorator to register extract methods automatically.

    Args:
        extract_cmd_type: String identifier for the extract command type
    """

    def decorator(func: ExtractMethodType) -> ExtractMethodType:
        _extract_method_registry[name] = func
        return func

    return decorator


class ExtractStrategy:
    _project_root = None
    _modules_imported = False

    @staticmethod
    def _discover_flow_directories():
        """Automatically discover all directories that contain 'extract' subdirectories."""
        sources_path = os.path.join(ExtractStrategy._project_root, "sources")  # type: ignore
        extract_directories = []

        # Check if sources directory exists
        if not os.path.isdir(sources_path):
            logger.warning("Sources directory not found: %s", sources_path)
            return extract_directories

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
                extracts_path = os.path.join(item_path, "extract")
                if os.path.isdir(extracts_path):
                    extract_directories.append(f"sources/{item}/extract")

        return extract_directories

    @staticmethod
    def _discover_and_import_modules():
        """Auto-discover and import all flow modules to trigger decorator registration."""
        if ExtractStrategy._modules_imported:
            return

        if not ExtractStrategy._project_root:
            ExtractStrategy._project_root = find_project_root()

        # Dynamically discover flow directories
        successful_imports = 0
        for flow_dir in ExtractStrategy._discover_flow_directories():
            flow_path = os.path.join(ExtractStrategy._project_root, flow_dir)

            # Convert path to module path
            module_prefix = flow_dir.replace("/", ".").replace("\\", ".")
            try:
                # Import all Python files in the directory
                for _, name, ispkg in pkgutil.iter_modules([flow_path]):
                    if not ispkg:  # Only import .py files, not packages
                        module_name = f"{module_prefix}.{name}"
                        try:
                            import_module(module_name)
                            successful_imports += 1
                        except ImportError as e:
                            logger.warning(
                                "Failed to import flow module %s: %s", module_name, e
                            )
                        except Exception as e:
                            logger.error(
                                "Error importing flow module %s: %s", module_name, e
                            )

            except Exception as e:
                logger.error("Error scanning flow directory %s: %s", flow_path, e)

        logger.info(
            f"Successfully imported {successful_imports}/{len(_extract_method_registry.keys())} flow modules"
        )
        ExtractStrategy._modules_imported = True

    @staticmethod
    def create(extract_cmd_type: str) -> ExtractMethodType:
        """Create an extract command function based on the specified type.
        Auto-discovers and imports flow modules, then checks the decorator registry.

        Args:
            extract_cmd_type: String identifier for the extract command type

        Returns: Callable function that implements the extract logic

        Raises:
            ValueError: If the extract command type is not recognized
        """
        # Auto-discover and import all flow modules
        ExtractStrategy._discover_and_import_modules()

        # Check if the method is registered via decorator
        if extract_cmd_type in _extract_method_registry:
            return _extract_method_registry[extract_cmd_type]

        raise ValueError(
            f"Extract command type '{extract_cmd_type}' not found in registry"
        )
