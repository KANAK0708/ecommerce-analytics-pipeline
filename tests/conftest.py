"""Shared pytest fixtures: one SparkSession for the whole test run (startup is slow)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(scope="session")
def spark():
    from src.utils.spark_session import get_spark_session, stop_spark_session

    session = get_spark_session("pytest-ecommerce", master="local[2]", shuffle_partitions=4)
    yield session
    stop_spark_session()
