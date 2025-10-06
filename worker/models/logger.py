import logging
import sys


class ColoredFormatter(logging.Formatter):
    # ANSI escape codes
    BOLD = "\033[1m"
    RESET = "\033[0m"
    COLORS = {
        "DEBUG": "\033[37m",  # White
        "INFO": "\033[34m",  # Blue
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[41m",  # Red background
    }

    def format(self, record):
        levelname = record.levelname
        color = self.COLORS.get(levelname, "")
        padded_level = f"{levelname:<8}"  # Align level names
        # Apply color and bold to log level only
        record.levelname = f"{self.BOLD}{color}{padded_level}{self.RESET}"
        return super().format(record)


formatter = ColoredFormatter(
    fmt="%(asctime)s|%(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.handlers.clear()
logger.addHandler(handler)
logger.propagate = False
