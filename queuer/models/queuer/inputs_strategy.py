from importlib import import_module
import os
from typing import Any, Callable, Dict, Iterator

from models.file_utils import find_project_root
from models.logger import logger
from models.etl.input import ETLInput


# Global registry for inputs methods
InputMethodType = Callable[..., Iterator[ETLInput]]
_inputs_method_registry: Dict[str, InputMethodType] = {}


def transform_inputs_method(name: str):
    """Decorator to register transform inputs methods automatically."""

    def decorator(func: InputMethodType) -> InputMethodType:
        _inputs_method_registry[name] = func
        return func

    return decorator


class TransformInputsStrategy:
    _project_root = None
    _modules_imported = False

    @staticmethod
    def _discover_inputs_directories():
        """Automatically discover all directories that might contain inputs modules."""
        sources_path = os.path.join(TransformInputsStrategy._project_root, "sources")  # type: ignore
        inputs_directories = []

        # Check if sources directory exists
        if not os.path.isdir(sources_path):
            logger.warning("Sources directory not found: %s", sources_path)
            return inputs_directories

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
                # Check if directory contains inputs.py
                inputs_file = os.path.join(item_path, "inputs.py")
                if os.path.isfile(inputs_file):
                    inputs_directories.append(f"sources.{item}")

        return inputs_directories

    @staticmethod
    def _discover_and_import_modules():
        """Auto-discover and import all inputs modules to trigger decorator registration."""
        if TransformInputsStrategy._modules_imported:
            return

        if not TransformInputsStrategy._project_root:
            TransformInputsStrategy._project_root = find_project_root()

        # Dynamically discover inputs directories
        inputs_directories = TransformInputsStrategy._discover_inputs_directories()

        for inputs_module_base in inputs_directories:
            inputs_module = f"{inputs_module_base}.inputs"

            try:
                import_module(inputs_module)
            except ImportError as e:
                logger.warning(
                    "Failed to import inputs module %s: %s", inputs_module, e
                )
            except Exception as e:
                logger.error("Error importing inputs module %s: %s", inputs_module, e)

        logger.info(
            f"Successfully imported {len(_inputs_method_registry.keys())} inputs modules"
        )
        TransformInputsStrategy._modules_imported = True

    @staticmethod
    def create(inputs_cmd_type: str) -> InputMethodType:
        # Auto-discover and import all inputs modules
        TransformInputsStrategy._discover_and_import_modules()

        # Check if the method is registered via decorator
        if inputs_cmd_type in _inputs_method_registry:
            return _inputs_method_registry[inputs_cmd_type]

        raise ValueError(
            f"Inputs command type '{inputs_cmd_type}' not found in registry"
        )
