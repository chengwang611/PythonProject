"""Logging configuration for Databricks pipelines."""

from __future__ import annotations

import logging
import sys
from typing import Optional


def setup_logging(
    level: int = logging.INFO,
    fmt: Optional[str] = None,
    stream: Optional[object] = None,
) -> None:
    """Configure root logger with a consistent format.

    Args:
        level: Logging level (default INFO).
        fmt: Custom format string.
        stream: Output stream (default stderr).
    """
    if fmt is None:
        fmt = (
            "%(asctime)s [%(levelname)s] %(name)s | %(funcName)s:%(lineno)d | %(message)s"
        )

    stream = stream or sys.stderr

    logging.basicConfig(
        level=level,
        format=fmt,
        stream=stream,
    )

    # Reduce noise from verbose libraries
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    logger = logging.getLogger(__name__)
    logger.info("Logging configured at level %s", logging.getLevelName(level))
