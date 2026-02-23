"""Shared logging configuration for the TalaSuri pipeline."""

import logging
import sys


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the pipeline.

    Args:
        verbose: If True, set DEBUG level. Otherwise INFO.
    """
    level = logging.DEBUG if verbose else logging.INFO
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s [%(levelname)-7s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    # Configure root logger for backend.*
    root = logging.getLogger("backend")
    root.setLevel(level)
    root.addHandler(handler)
