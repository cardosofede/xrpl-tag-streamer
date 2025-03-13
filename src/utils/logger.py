"""
Logging configuration for the application.
"""

import logging
from typing import Optional

from src.config import LOG_LEVEL, DATA_DIR, LOG_DIR


# Configure the root logger
def setup_logging(
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
) -> logging.Logger:
    """
    Configure the logging system for the application.
    
    Args:
        log_level: The logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Path to log file, or None to log to console only
        
    Returns:
        The configured logger instance
    """
    # Clean the log level string by removing comments
    log_level_value = log_level or LOG_LEVEL
    if "#" in log_level_value:
        log_level_value = log_level_value.split("#")[0].strip()
    
    level = getattr(logging, log_level_value.upper())
    
    # Create logs directory if it doesn't exist
    log_dir = LOG_DIR
    log_dir.mkdir(exist_ok=True, parents=True)
    
    # Default log file if not specified
    log_file_path = log_file or str(log_dir / "xrpl_tag_streamer.log")
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file_path),
        ],
    )
    
    # Create and return the logger for the application
    logger = logging.getLogger("xrpl_tag_streamer")
    
    return logger 