"""Tests for src/pipeline.py (local end-to-end run)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src import pipeline

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


def test_parse_args_defaults():
    args = pipeline.parse_args([])
    assert Path(args.raw_dir) == pipeline.PROJECT_ROOT / "data" / "raw"
    assert args.reference_date == "2026-11-19"


def test_missing_raw_dir_returns_error_code(spark, tmp_path):
    assert pipeline.main(["--raw-dir", str(tmp_path / "missing"), "--output-dir", str(tmp_path)], stop_spark=False) == 1


def test_output_dir_cannot_be_raw_dir(spark, tmp_path):
    with pytest.raises(ValueError, match="must differ"):
        pipeline.run_pipeline(tmp_path, tmp_path)


@pytest.mark.skipif(not (RAW_DIR / "orders.csv").is_file(), reason="Phase 1 raw data not generated")
def test_end_to_end_phase1(spark, tmp_path, capsys):
    rc = pipeline.main(["--raw-dir", str(RAW_DIR), "--output-dir", str(tmp_path)], stop_spark=False)
    assert rc == 0
    report = capsys.readouterr().out
    assert "orders" in report and "gold_daily_sales" in report

    summary = pipeline.run_pipeline(RAW_DIR, tmp_path)  # rerun: same answer, no double count
    assert summary.silver == {
        "customers": {"raw": 1010, "silver": 985, "quarantine": 25},
        "inventory": {"raw": 300, "silver": 294, "quarantine": 6},
        "orders": {"raw": 10000, "silver": 9700, "quarantine": 300},
    }
    assert summary.gold["gold_inventory_health"] == 294
    assert summary.gold["gold_customer_metrics"] == 985
    for sub in ("silver/orders", "silver/quarantine/orders", "gold/gold_daily_sales"):
        assert (tmp_path / sub).is_dir()
