#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging


def setup_logging(level: str = "INFO") -> logging.Logger:
    """
    Configure and return a root logger for the pipeline.
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