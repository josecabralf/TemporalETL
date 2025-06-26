from typing import Dict, Callable
from importlib import import_module
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExtractMethodFactory:
    method_name = "extract_data"
    extractCmdTypes: Dict[str, str] = {
        # identifier                 # module path
        "launchpad-bugs":            "launchpad.flows.bugs",
        "launchpad-merge_proposals": "launchpad.flows.merge_proposals",
        "launchpad-questions":       "launchpad.flows.questions",
        # Add additional extract command types here as they are implemented
    }

    @staticmethod
    def create(extract_cmd_type: str) -> Callable:
        """
        Create an extract command function based on the specified type.
        
        Args:
            extract_cmd_type: String identifier for the extract command type

        Returns: Callable function that implements the extract logic
        
        Raises:
            ValueError: If the extract command type is not recognized
            TypeError: If the resolved function is not callable
            ImportError: If the module or function cannot be imported
            AttributeError: If the specified method does not exist in the module    
        """
        if extract_cmd_type not in ExtractMethodFactory.extractCmdTypes:
            raise ValueError("Unknown extract command type: %s", extract_cmd_type)

        module_name = ExtractMethodFactory.extractCmdTypes[extract_cmd_type]

        try:
            module = import_module(module_name)
            extract_cmd_function = getattr(module, ExtractMethodFactory.method_name)
            logger.info("Successfully imported '%s' function from module '%s'", ExtractMethodFactory.method_name, module_name)
        except ImportError as e:
            raise ImportError("Cannot import module '%s': %s", module_name, e)
        except AttributeError as e:
            raise AttributeError("Module '%s' does not have '%s' function: %s", module_name, ExtractMethodFactory.method_name, e)

        if not callable(extract_cmd_function):
            raise TypeError("'%s' is not a callable function", extract_cmd_function.__name__)

        return extract_cmd_function