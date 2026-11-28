"""
Spark session utility for local development (and, later, AWS Glue).

Why this module exists
----------------------
Every ETL stage (Silver, Gold, pipeline, tests) needs a SparkSession. Building it in
one place guarantees they all share the same settings:

* Parquet files are written with Snappy compression (approved in docs/decisions.md).
* Timestamps are interpreted in UTC, so results don't depend on the laptop's timezone.
* Partition overwrites are "dynamic": re-running a partitioned write only replaces the
  partitions present in the new data, which keeps reruns idempotent.
* On a local machine the driver binds to 127.0.0.1 (avoids Windows/VPN hostname issues)
  and Python workers use the project's .venv interpreter (avoids "Python worker failed
  to connect back" errors caused by a different python.exe on PATH).

How it works
------------
``SparkSession.builder.getOrCreate()`` returns the already-running session if one
exists, so calling ``get_spark_session()`` many times is cheap and safe. Environment
variables are applied *before* the JVM starts, because Spark reads them only once.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]
DEFAULT_APP_NAME: str = "EcommerceMedallionETL"
DEFAULT_MASTER: str = "local[*]"
DRIVER_HOST: str = "127.0.0.1"


def resolve_project_python() -> str:
    """Return the project's .venv Python executable, or the current interpreter.

    The .venv interpreter is preferred so Spark's Python workers import exactly the
    same packages as the driver. When no .venv exists (e.g. CI or Glue), the current
    interpreter (``sys.executable``) is used instead.
    """
    candidates = [
        PROJECT_ROOT / ".venv" / "Scripts" / "python.exe",  # Windows
        PROJECT_ROOT / ".venv" / "bin" / "python",  # Linux / macOS
    ]
    for candidate in candidates:
        if candidate.is_file():
            return str(candidate)
    logger.warning("Project .venv not found under %s; using %s", PROJECT_ROOT, sys.executable)
    return sys.executable


def configure_local_environment() -> str:
    """Point PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON at the project interpreter.

    Must run before the first SparkSession is created. Returns the interpreter path.
    On Windows, also warns early if HADOOP_HOME is missing (Parquet writes would fail).
    """
    python_exe = resolve_project_python()
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

    if os.name == "nt" and not os.environ.get("HADOOP_HOME"):
        logger.warning(
            "HADOOP_HOME is not set. On Windows, Spark needs %%HADOOP_HOME%%\\bin\\winutils.exe "
            "and hadoop.dll to write Parquet files."
        )
    return python_exe


def get_spark_session(
    app_name: str = DEFAULT_APP_NAME,
    master: Optional[str] = None,
    shuffle_partitions: Optional[int] = None,
) -> SparkSession:
    """Create (or reuse) a configured SparkSession.

    Args:
        app_name: Name shown in Spark logs / UI.
        master: Spark master URL. Defaults to env var ``SPARK_MASTER`` or ``local[*]``.
            On AWS Glue the cluster supplies the master, so pass nothing there.
        shuffle_partitions: Optional override for ``spark.sql.shuffle.partitions``.
            Small local datasets run much faster with a small value (e.g. 4).

    Returns:
        The active SparkSession.

    Raises:
        ValueError: If ``app_name`` is empty or ``shuffle_partitions`` is not positive.
    """
    if not app_name or not app_name.strip():
        raise ValueError("app_name must be a non-empty string")
    if shuffle_partitions is not None and shuffle_partitions < 1:
        raise ValueError("shuffle_partitions must be a positive integer")

    configure_local_environment()
    resolved_master = master or os.environ.get("SPARK_MASTER", DEFAULT_MASTER)

    builder = (
        SparkSession.builder.appName(app_name)
        .master(resolved_master)
        .config("spark.driver.host", DRIVER_HOST)
        .config("spark.driver.bindAddress", DRIVER_HOST)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if shuffle_partitions is not None:
        builder = builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))

    logger.info("Starting SparkSession app=%s master=%s", app_name, resolved_master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark_session() -> None:
    """Stop the active SparkSession if there is one (safe to call repeatedly)."""
    spark = SparkSession.getActiveSession()
    if spark is not None:
        logger.info("Stopping SparkSession")
        spark.stop()
