from importlib import import_module
import os
from typing import Any, Callable, Dict

from models.file_utils import find_project_root
from models.logger import logger


# Global registry for params methods
ParamsMethodType = Callable[..., Dict[str, Any]]
_params_method_registry: Dict[str, ParamsMethodType] = {}


def params_method(name: str):
    """Decorator to register params methods automatically."""

    def decorator(func: ParamsMethodType) -> ParamsMethodType:
        _params_method_registry[name] = func
        return func

    return decorator


class ExtractParamsStrategy:
    _project_root = None
    _modules_imported = False

    @staticmethod
    def _discover_params_directories():
        """Automatically discover all directories that might contain params modules."""
        sources_path = os.path.join(ExtractParamsStrategy._project_root, "sources")  # type: ignore
        params_directories = []

        # Check if sources directory exists
        if not os.path.isdir(sources_path):
            logger.warning("Sources directory not found: %s", sources_path)
            return params_directories

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
                # Check if directory contains params.py
                params_file = os.path.join(item_path, "params.py")
                if os.path.isfile(params_file):
                    params_directories.append(f"sources.{item}")

        return params_directories

    @staticmethod
    def _discover_and_import_modules():
        """Auto-discover and import all params modules to trigger decorator registration."""
        if ExtractParamsStrategy._modules_imported:
            return

        if not ExtractParamsStrategy._project_root:
            ExtractParamsStrategy._project_root = find_project_root()

        # Dynamically discover params directories
        params_directories = ExtractParamsStrategy._discover_params_directories()

        for params_module_base in params_directories:
            module = f"{params_module_base}.params"

            try:
                import_module(module)
            except ImportError as e:
                logger.warning("Failed to import params module %s: %s", module, e)
            except Exception as e:
                logger.error("Error importing params module %s: %s", module, e)

        logger.info(
            f"Successfully imported {len(_params_method_registry.keys())} params modules"
        )
        ExtractParamsStrategy._modules_imported = True

    @staticmethod
    def create(params_cmd_type: str) -> ParamsMethodType:
        # Auto-discover and import all params modules
        ExtractParamsStrategy._discover_and_import_modules()

        # Check if the method is registered via decorator
        if params_cmd_type in _params_method_registry:
            return _params_method_registry[params_cmd_type]

        raise ValueError(
            f"Params command type '{params_cmd_type}' not found in registry"
        )
