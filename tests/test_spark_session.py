"""Tests for src/utils/spark_session.py."""

from __future__ import annotations

import os

import pytest

from src.utils import spark_session as ss


def test_resolve_project_python_prefers_venv():
    exe = ss.resolve_project_python()
    assert os.path.isfile(exe)
    venv_dir = ss.PROJECT_ROOT / ".venv"
    if venv_dir.is_dir():
        assert str(venv_dir) in exe


def test_configure_sets_pyspark_env_vars():
    exe = ss.configure_local_environment()
    assert os.environ["PYSPARK_PYTHON"] == exe
    assert os.environ["PYSPARK_DRIVER_PYTHON"] == exe


def test_session_configuration(spark):
    conf = spark.conf
    assert conf.get("spark.driver.host") == "127.0.0.1"
    assert conf.get("spark.sql.parquet.compression.codec") == "snappy"
    assert conf.get("spark.sql.session.timeZone") == "UTC"
    assert conf.get("spark.sql.sources.partitionOverwriteMode") == "dynamic"


def test_get_spark_session_reuses_active_session(spark):
    assert ss.get_spark_session("pytest-ecommerce") is spark


@pytest.mark.parametrize("kwargs", [{"app_name": ""}, {"app_name": "  "}, {"shuffle_partitions": 0}])
def test_invalid_arguments_rejected(kwargs):
    with pytest.raises(ValueError):
        ss.get_spark_session(**kwargs)


def test_python_worker_runs(spark):
    from pyspark.sql.functions import udf

    double = udf(lambda x: x * 2, "long")
    assert [r[0] for r in spark.range(3).select(double("id")).collect()] == [0, 2, 4]


def test_parquet_snappy_round_trip(spark, tmp_path):
    out = str(tmp_path / "roundtrip")
    spark.range(5).write.mode("overwrite").parquet(out)
    assert spark.read.parquet(out).count() == 5
    files = [f for f in os.listdir(out) if f.endswith(".parquet")]
    assert files and all(".snappy." in f for f in files)
