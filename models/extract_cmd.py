from importlib import import_module
import logging
import os
import pkgutil
from typing import Dict, Callable


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Global registry for extract methods
_extract_method_registry: Dict[str, Callable] = {}


def extract_method(name: str):
    """
    Decorator to register extract methods automatically.
    
    Args:
        extract_cmd_type: String identifier for the extract command type
        
    Returns:
        Decorated function that is registered in the global registry
    """
    def decorator(func: Callable) -> Callable:
        _extract_method_registry[name] = func
        return func
    
    return decorator


class ExtractMethodFactory:
    _modules_imported = False

    @staticmethod
    def _discover_flow_directories():
        """
        Automatically discover all directories that contain 'flows' subdirectories.
        
        Returns:
            List of flow directory paths (e.g., ['launchpad/flows', 'github/flows', 'jira/flows'])
        """
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        flow_directories = []
        
        # Walk through all directories in project root
        for item in os.listdir(project_root):
            item_path = os.path.join(project_root, item)
            
            # Skip hidden directories, __pycache__, .venv, etc.
            if item.startswith('.') or item.startswith('__') or item in ['venv', '.venv', 'node_modules']:
                continue
                
            if os.path.isdir(item_path):
                flows_path = os.path.join(item_path, 'flows')
                if os.path.isdir(flows_path):
                    flow_directories.append(f"{item}/flows")
        
        return flow_directories

    @staticmethod
    def _discover_and_import_modules():
        """Auto-discover and import all flow modules to trigger decorator registration."""
        if ExtractMethodFactory._modules_imported:
            return
            
        # Get project root directory
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        
        # Dynamically discover flow directories
        flow_directories = ExtractMethodFactory._discover_flow_directories()
        
        for flow_dir in flow_directories:
            flow_path = os.path.join(project_root, flow_dir)
            
            # Convert path to module path
            module_prefix = flow_dir.replace("/", ".").replace("\\", ".")
            
            try:
                # Import all Python files in the directory
                for finder, name, ispkg in pkgutil.iter_modules([flow_path]):
                    if not ispkg:  # Only import .py files, not packages
                        module_name = f"{module_prefix}.{name}"
                        try:
                            import_module(module_name)
                        except ImportError as e:
                            logger.warning("Failed to import flow module %s: %s", module_name, e)
                        except Exception as e:
                            logger.error("Error importing flow module %s: %s", module_name, e)
                            
            except Exception as e:
                logger.error("Error scanning flow directory %s: %s", flow_path, e)
        
        ExtractMethodFactory._modules_imported = True

    @staticmethod
    def create(extract_cmd_type: str) -> Callable:
        """
        Create an extract command function based on the specified type.
        Auto-discovers and imports flow modules, then checks the decorator registry.
        
        Args:
            extract_cmd_type: String identifier for the extract command type

        Returns: Callable function that implements the extract logic
        
        Raises:
            ValueError: If the extract command type is not recognized
        """
        # Auto-discover and import all flow modules
        ExtractMethodFactory._discover_and_import_modules()
        
        # Check if the method is registered via decorator
        logger.info("Registered extract command types: %s", list(_extract_method_registry.keys()))
        if extract_cmd_type in _extract_method_registry:
            extract_cmd_function = _extract_method_registry[extract_cmd_type]
            return extract_cmd_function
        
        raise ValueError(f"Extract command type '{extract_cmd_type}' not found in registry")

    @staticmethod
    def get_registered_types() -> list:
        """
        Get all registered extract command types from decorator registry.
        
        Returns:
            List of all available extract command types
        """
        ExtractMethodFactory._discover_and_import_modules()
        return list(_extract_method_registry.keys())