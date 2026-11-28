"""Tests for src/etl/silver_clean.py.

Small hand-checkable CSVs cover each approved rule; one integration test checks the
exact reconciliation counts on the approved Phase 1 data in data/raw/.
"""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import pytest
from chispa import assert_df_equality
from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, StructField, StructType

from src.etl import silver_clean as sc

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
RUN_TS = dt.datetime(2026, 11, 28, 6, 0, 0)


def _csv(tmp_path: Path, name: str, lines: list[str]) -> Path:
    path = tmp_path / name
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _reasons(result: sc.CleanResult) -> dict[str, int]:
    return result.reason_counts()


# ---------------------------------------------------------------------------
# Reading / validation
# ---------------------------------------------------------------------------
def test_missing_file_raises(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        sc.read_raw_csv(spark, tmp_path / "nope.csv", sc.CUSTOMER_COLUMNS)


def test_header_mismatch_raises(spark, tmp_path):
    path = _csv(tmp_path, "customers.csv", ["customer_id,country,signup_date", "CUST_10001,US,2026-01-01"])
    with pytest.raises(ValueError, match="Header mismatch"):
        sc.read_raw_csv(spark, path, sc.CUSTOMER_COLUMNS)


def test_bad_reference_date_rejected(spark, tmp_path):
    path = _csv(tmp_path, "customers.csv", ["customer_id,signup_date,country"])
    raw = sc.read_raw_csv(spark, path, sc.CUSTOMER_COLUMNS)
    with pytest.raises(ValueError, match="reference_date"):
        sc.clean_customers(raw, reference_date="19/11/2026")


def test_reconcile_detects_lost_rows():
    with pytest.raises(ValueError, match="Reconciliation failed"):
        sc.reconcile("orders", 10, 7, 2)


def test_empty_input_produces_empty_outputs(spark, tmp_path):
    path = _csv(tmp_path, "orders.csv", [",".join(sc.ORDER_COLUMNS)])
    result = sc.clean_orders(sc.read_raw_csv(spark, path, sc.ORDER_COLUMNS), rejected_at=RUN_TS)
    assert (result.raw_count, result.silver_count, result.quarantine_count) == (0, 0, 0)


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------
def test_customers_rules_and_dedup(spark, tmp_path):
    path = _csv(
        tmp_path,
        "customers.csv",
        [
            "customer_id,signup_date,country",
            "CUST_10001,2026-01-01,",  # blank country
            "CUST_10002,2026-11-20,US",  # future vs reference 2026-11-19
            "CUST_10003,2026-11-19,GB",  # exactly reference date -> valid
            "CUST_10004,2025-05-05,IN",
            "CUST_10004,2025-05-05,IN",  # duplicate replay
            "CUST_10005,2026-13-40,FR",  # unparseable date (schema check)
        ],
    )
    result = sc.clean_customers(sc.read_raw_csv(spark, path, sc.CUSTOMER_COLUMNS), rejected_at=RUN_TS)

    expected_schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("signup_date", DateType(), True),
            StructField("country", StringType(), True),
        ]
    )
    expected = spark.createDataFrame(
        [("CUST_10003", dt.date(2026, 11, 19), "GB"), ("CUST_10004", dt.date(2025, 5, 5), "IN")],
        expected_schema,
    )
    assert_df_equality(result.silver, expected, ignore_row_order=True, ignore_nullable=True)
    assert _reasons(result) == {
        "blank_country": 1,
        "future_signup_date": 1,
        "duplicate_customer_id": 1,
        "invalid_signup_date": 1,
    }


def test_quarantine_keeps_original_columns_plus_audit_fields(spark, tmp_path):
    path = _csv(tmp_path, "customers.csv", ["customer_id,signup_date,country", "CUST_10001,2026-12-01,US"])
    result = sc.clean_customers(sc.read_raw_csv(spark, path, sc.CUSTOMER_COLUMNS), rejected_at=RUN_TS)
    assert result.quarantine.columns == sc.CUSTOMER_COLUMNS + ["rejection_reason", "rejected_at"]
    row = result.quarantine.first()
    # The raw string value is preserved exactly, not the parsed date.
    assert row["signup_date"] == "2026-12-01"
    assert result.quarantine.selectExpr("unix_seconds(rejected_at)").first()[0] == int(
        RUN_TS.replace(tzinfo=dt.timezone.utc).timestamp()
    )


def test_dedup_is_deterministic_keeps_first_occurrence(spark, tmp_path):
    path = _csv(
        tmp_path,
        "customers.csv",
        ["customer_id,signup_date,country", "CUST_10009,2025-01-01,US", "CUST_10009,2025-02-02,CA"],
    )
    for _ in range(2):  # same answer on every run
        result = sc.clean_customers(sc.read_raw_csv(spark, path, sc.CUSTOMER_COLUMNS), rejected_at=RUN_TS)
        assert result.silver.first()["country"] == "US"
        assert result.quarantine.first()["country"] == "CA"


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------
def test_inventory_rules(spark, tmp_path):
    path = _csv(
        tmp_path,
        "inventory.csv",
        [
            ",".join(sc.INVENTORY_COLUMNS),
            "PROD_1001,Laptop Pro,Electronics,-3,50",  # negative stock
            "PROD_1002,,Books,10,25",  # blank name
            "PROD_1003,Yoga Mat,Sports & Outdoors,0,10",  # zero stock is valid
            "PROD_1004,Mug,Home & Kitchen,abc,10",  # unparseable stock
        ],
    )
    result = sc.clean_inventory(sc.read_raw_csv(spark, path, sc.INVENTORY_COLUMNS), rejected_at=RUN_TS)
    assert [r["product_id"] for r in result.silver.collect()] == ["PROD_1003"]
    assert dict(result.silver.dtypes)["current_stock"] == "int"
    assert _reasons(result) == {"negative_stock": 1, "blank_product_name": 1, "invalid_current_stock": 1}


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------
def test_orders_rules_dedup_and_decimal(spark, tmp_path):
    path = _csv(
        tmp_path,
        "orders.csv",
        [
            ",".join(sc.ORDER_COLUMNS),
            "ORD_100001,,2026-05-26,PROD_1074,3,161.44",  # null customer
            "ORD_100002,CUST_10001,2026-05-27,PROD_1075,-2,10.00",  # negative qty
            "ORD_100003,CUST_10001,2026-05-28,PROD_1076,1,0.00",  # zero price
            "ORD_100004,CUST_10002,2026-05-29,PROD_1077,2,19.99",
            "ORD_100004,CUST_10002,2026-05-29,PROD_1077,2,19.99",  # duplicate line
            "ORD_100004,CUST_10002,2026-05-29,PROD_1078,1,5.99",  # same order, new product: valid
        ],
    )
    result = sc.clean_orders(sc.read_raw_csv(spark, path, sc.ORDER_COLUMNS), rejected_at=RUN_TS)

    assert _reasons(result) == {
        "null_customer_id": 1,
        "negative_quantity": 1,
        "zero_unit_price": 1,
        "duplicate_order_line": 1,
    }
    schema = dict((f.name, f.dataType) for f in result.silver.schema.fields)
    assert schema["unit_price"] == DecimalType(10, 2)
    assert schema["quantity"] == IntegerType()
    assert schema["order_date"] == DateType()
    prices = sorted(r["unit_price"] for r in result.silver.collect())
    assert prices == [Decimal("5.99"), Decimal("19.99")]  # exact, no float error


def test_orders_not_rejected_for_preceding_signup_date(spark, tmp_path):
    # clean_orders never looks at customers, so an order before signup stays valid.
    path = _csv(
        tmp_path, "orders.csv", [",".join(sc.ORDER_COLUMNS), "ORD_100009,CUST_10003,2020-01-01,PROD_1001,1,9.99"]
    )
    result = sc.clean_orders(sc.read_raw_csv(spark, path, sc.ORDER_COLUMNS), rejected_at=RUN_TS)
    assert result.silver_count == 1 and result.quarantine_count == 0


# ---------------------------------------------------------------------------
# Integration on the approved Phase 1 data
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not (RAW_DIR / "orders.csv").is_file(), reason="Phase 1 raw data not generated")
def test_phase1_reconciliation_counts_and_idempotent_rerun(spark, tmp_path):
    silver_dir = tmp_path / "silver"
    for _ in range(2):  # second run must overwrite, not append
        results = sc.run_silver(spark, RAW_DIR, silver_dir, rejected_at=RUN_TS)

    expected = {
        "customers": (1010, 985, 25),
        "inventory": (300, 294, 6),
        "orders": (10000, 9700, 300),
    }
    for name, (raw, silver, quarantine) in expected.items():
        r = results[name]
        assert (r.raw_count, r.silver_count, r.quarantine_count) == (raw, silver, quarantine), name
        assert spark.read.parquet(str(silver_dir / name)).count() == silver
        assert spark.read.parquet(str(silver_dir / "quarantine" / name)).count() == quarantine

    assert results["customers"].reason_counts() == {
        "duplicate_customer_id": 10,
        "blank_country": 10,
        "future_signup_date": 5,
    }
    assert results["inventory"].reason_counts() == {"negative_stock": 3, "blank_product_name": 3}
    assert results["orders"].reason_counts() == {
        "duplicate_order_line": 100,
        "null_customer_id": 100,
        "negative_quantity": 50,
        "zero_unit_price": 50,
    }
    orders = results["orders"].silver
    assert orders.select("order_id", "product_id").distinct().count() == 9700
    assert orders.filter("customer_id IS NULL OR quantity < 1 OR unit_price <= 0").count() == 0
