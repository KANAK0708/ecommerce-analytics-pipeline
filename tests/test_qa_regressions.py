"""Regression tests for the four Codex Phase 2 QA findings."""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from src import pipeline
from src.etl import gold_aggregate as ga
from src.etl import silver_clean as sc
from src.utils.paths import ensure_no_overlap, paths_overlap

HEADER = ",".join(sc.ORDER_COLUMNS)


def _write(path: Path, lines: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _clean_orders(spark, tmp_path, lines):
    path = _write(tmp_path / "orders.csv", [HEADER] + lines)
    return sc.clean_orders(sc.read_raw_csv(spark, path, sc.ORDER_COLUMNS))


# ---------------------------------------------------------------------------
# FIX 1: strict quantity / unit_price validation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "quantity, price, reason",
    [
        ("0", "10.00", "zero_quantity"),
        ("-3", "10.00", "negative_quantity"),
        ("1", "-5.00", "negative_unit_price"),
        ("1", "0.00", "zero_unit_price"),
        ("1", "1.999", "excess_unit_price_precision"),
        ("1", "12.3456", "excess_unit_price_precision"),
        ("1", "abc", "invalid_unit_price"),
    ],
)
def test_fix1_invalid_quantity_and_price_quarantined(spark, tmp_path, quantity, price, reason):
    result = _clean_orders(spark, tmp_path, [f"ORD_100001,CUST_10001,2026-05-01,PROD_1001,{quantity},{price}"])
    assert result.silver_count == 0
    row = result.quarantine.first()
    assert row["rejection_reason"] == reason
    assert (row["quantity"], row["unit_price"]) == (quantity, price)  # original values preserved


def test_fix1_valid_precision_accepted(spark, tmp_path):
    result = _clean_orders(
        spark,
        tmp_path,
        ["ORD_100001,CUST_10001,2026-05-01,PROD_1001,1,5", "ORD_100002,CUST_10001,2026-05-01,PROD_1001,1,5.9"],
    )
    assert result.silver_count == 2 and result.quarantine_count == 0


# ---------------------------------------------------------------------------
# FIX 2: CSV row-width validation
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "bad_line",
    [
        "ORD_100002,CUST_10001,2026-05-01,PROD_1001,1,9.99,EXTRA",  # extra field
        "ORD_100002,CUST_10001,2026-05-01,PROD_1001,1",  # missing field
    ],
)
def test_fix2_wrong_field_count_aborts_before_any_write(spark, tmp_path, bad_line):
    raw_dir = tmp_path / "raw"
    good = "ORD_100001,CUST_10001,2026-05-01,PROD_1001,1,9.99"
    _write(raw_dir / "customers.csv", ["customer_id,signup_date,country", "CUST_10001,2025-01-01,US"])
    _write(raw_dir / "inventory.csv", [",".join(sc.INVENTORY_COLUMNS), "PROD_1001,Mug,Home & Kitchen,5,10"])
    orders = _write(raw_dir / "orders.csv", [HEADER, good, bad_line])
    before = orders.read_bytes()

    with pytest.raises(ValueError, match="wrong number of fields"):
        sc.run_silver(spark, raw_dir, tmp_path / "out" / "silver")

    assert not (tmp_path / "out").exists()  # nothing published
    assert orders.read_bytes() == before  # Bronze untouched


def test_fix2_quoted_comma_is_not_malformed(spark, tmp_path):
    path = _write(
        tmp_path / "inventory.csv",
        [",".join(sc.INVENTORY_COLUMNS), 'PROD_1001,"Mug, Large",Home & Kitchen,5,10'],
    )
    df = sc.read_raw_csv(spark, path, sc.INVENTORY_COLUMNS)
    assert df.first()["product_name"] == "Mug, Large"


# ---------------------------------------------------------------------------
# FIX 3: output path safety
# ---------------------------------------------------------------------------
def test_fix3_overlap_detection(tmp_path):
    raw = tmp_path / "data" / "raw"
    assert paths_overlap(raw, raw)
    assert paths_overlap(raw, raw / "silver")  # output nested inside raw
    assert paths_overlap(raw, tmp_path / "data")  # output contains raw
    assert paths_overlap(raw, tmp_path / "data" / "x" / ".." / "raw" / "gold")  # .. normalized
    assert not paths_overlap(raw, tmp_path / "data" / "silver")  # sibling is fine
    assert not paths_overlap(raw, tmp_path / "data" / "raw_copy")  # prefix-only match is fine


@pytest.mark.parametrize("output", ["raw", "raw/nested", "raw/../raw"])
def test_fix3_pipeline_rejects_overlapping_output_before_writes(spark, tmp_path, output):
    raw = tmp_path / "raw"
    raw.mkdir()
    with pytest.raises(ValueError, match="Unsafe output path"):
        pipeline.run_pipeline(raw, tmp_path / output)
    assert sorted(p.name for p in raw.iterdir()) == []  # nothing written into raw


def test_fix3_silver_and_gold_stages_check_paths(spark, tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    with pytest.raises(ValueError, match="Unsafe output path"):
        sc.run_silver(spark, raw, raw / "silver")
    with pytest.raises(ValueError, match="Unsafe output path"):
        ga.run_gold(spark, tmp_path / "silver", tmp_path / "silver" / "gold")
    with pytest.raises(ValueError, match="Unsafe output path"):
        ensure_no_overlap(tmp_path / "a" / "b", tmp_path / "a", "raw", "out")


# ---------------------------------------------------------------------------
# FIX 4: UTC quarantine timestamps
# ---------------------------------------------------------------------------
def test_fix4_rejected_at_persists_exact_utc_instant(spark, tmp_path):
    ist = dt.timezone(dt.timedelta(hours=5, minutes=30))
    moment = dt.datetime(2026, 11, 28, 12, 0, 0, tzinfo=ist)  # == 06:30:00 UTC
    expected_epoch = int(moment.timestamp())

    path = _write(tmp_path / "orders.csv", [HEADER, "ORD_100001,,2026-05-01,PROD_1001,1,9.99"])
    result = sc.clean_orders(sc.read_raw_csv(spark, path, sc.ORDER_COLUMNS), rejected_at=moment)
    out = tmp_path / "q"
    result.quarantine.write.mode("overwrite").parquet(str(out))

    row = (
        spark.read.parquet(str(out))
        .selectExpr("unix_seconds(rejected_at) AS epoch", "date_format(rejected_at, 'yyyy-MM-dd HH:mm:ss') AS utc")
        .first()
    )
    assert row["epoch"] == expected_epoch
    assert row["utc"] == "2026-11-28 06:30:00"  # session timezone is UTC


def test_fix4_default_timestamp_is_timezone_aware_utc():
    ts = sc._default_rejected_at(None)
    assert ts.tzinfo is not None and ts.utcoffset() == dt.timedelta(0)
    naive = sc._default_rejected_at(dt.datetime(2026, 1, 1, 0, 0))
    assert naive.tzinfo == dt.timezone.utc
