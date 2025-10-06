from importlib import import_module
import os
from typing import Callable, Dict, List

from models.file_utils import find_project_root
from models.logger import logger
from models.event import Event


# Global registry for transform methods
TransformMethodType = Callable[..., List[Event]]
_transform_method_registry: Dict[str, TransformMethodType] = {}


def transform_method(name: str):
    """Decorator to register transform methods automatically."""

    def decorator(func: Callable) -> Callable:
        _transform_method_registry[name] = func
        return func

    return decorator


class TransformStrategy:
    _project_root = None
    _modules_imported = False

    @staticmethod
    def _discover_transform_files():
        """Automatically discover all directories that might contain transform modules."""
        sources_path = os.path.join(TransformStrategy._project_root, "sources")  # type: ignore
        transform_files = []

        # Check if sources directory exists
        if not os.path.isdir(sources_path):
            logger.warning("Sources directory not found: %s", sources_path)
            return transform_files

        # Walk through all directories in sources
        for item in os.listdir(sources_path):
            item_path = os.path.join(sources_path, item)

            # Skip hidden directories, __pycache__, .venv, etc.
            if item.startswith(".") or item.startswith("__"):
                continue

            if os.path.isdir(item_path):
                # Check if directory contains transform.py
                transform_file = os.path.join(item_path, "transform.py")
                if os.path.isfile(transform_file):
                    transform_files.append(f"sources.{item}")

        return transform_files

    @staticmethod
    def _discover_and_import_modules():
        """Auto-discover and import all transform modules to trigger decorator registration."""
        if TransformStrategy._modules_imported:
            return

        if not TransformStrategy._project_root:
            TransformStrategy._project_root = find_project_root()

        # Dynamically discover transform files
        for transform_module_base in TransformStrategy._discover_transform_files():
            transform_module = f"{transform_module_base}.transform"

            try:
                import_module(transform_module)
            except ImportError as e:
                logger.warning(
                    "Failed to import transform module %s: %s", transform_module, e
                )
            except Exception as e:
                logger.error(
                    "Error importing transform module %s: %s", transform_module, e
                )

        logger.info(
            f"Successfully imported {len(_transform_method_registry.keys())} transform modules"
        )

        TransformStrategy._modules_imported = True

    @staticmethod
    def create(transform_cmd_type: str) -> TransformMethodType:
        # Auto-discover and import all flow modules
        TransformStrategy._discover_and_import_modules()

        # Check if the method is registered via decorator
        if transform_cmd_type in _transform_method_registry:
            return _transform_method_registry[transform_cmd_type]

        raise ValueError(
            f"Transform command type '{transform_cmd_type}' not found in registry"
        )
