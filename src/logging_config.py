#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Logging configuration helpers for the data pipeline."""

import logging


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Configure and return the shared pipeline logger.

    Args:
        level: Logging level name (for example ``"INFO"`` or ``"DEBUG"``).

    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger("cct_data_pipeline")
    logger.setLevel(level)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
