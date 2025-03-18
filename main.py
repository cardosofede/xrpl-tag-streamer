"""
Main entry point for the XRPL Tag Collector application.
"""

import logging
import sys

from src.collector import run_collector_sync
from src.utils.logger import setup_logging

logger = logging.getLogger(__name__)

def main():
    """Main entry point."""
    setup_logging()
    
    logger.info("Starting XRPL Tag Collector")
    
    try:
        # Run the collector
        run_collector_sync()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 