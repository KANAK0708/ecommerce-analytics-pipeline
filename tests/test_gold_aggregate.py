"""Tests for src/etl/gold_aggregate.py using small hand-checkable Silver data."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path

import pytest
from chispa import assert_df_equality
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.etl import gold_aggregate as ga
from src.etl import silver_clean as sc

RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("order_date", DateType()),
        StructField("product_id", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DecimalType(10, 2)),
    ]
)
D1, D2 = dt.date(2026, 6, 1), dt.date(2026, 6, 2)


@pytest.fixture
def orders(spark):
    # Hand-checkable: ORD_1 is a 2-line order (must count once).
    rows = [
        ("ORD_1", "CUST_A", D1, "PROD_1", 2, Decimal("10.10")),  # 20.20
        ("ORD_1", "CUST_A", D1, "PROD_2", 1, Decimal("0.10")),  # 0.10  -> order 20.30
        ("ORD_2", "CUST_B", D1, "PROD_1", 3, Decimal("333.33")),  # 999.99
        ("ORD_3", "CUST_B", D2, "PROD_3", 1, Decimal("0.01")),  # 0.01
        ("ORD_4", "CUST_C", D2, "PROD_3", 10, Decimal("30.00")),  # 300.00
    ]
    return spark.createDataFrame(rows, ORDERS_SCHEMA)


def test_line_revenue_is_exact_decimal(spark):
    df = spark.createDataFrame([(3, Decimal("0.10")), (7, Decimal("499.99"))], "quantity int, unit_price decimal(10,2)")
    out = df.select(ga.line_revenue().alias("rev"))
    assert out.schema["rev"].dataType == DecimalType(12, 2)
    assert [r["rev"] for r in out.collect()] == [Decimal("0.30"), Decimal("3499.93")]


def test_daily_sales_decimal_values(orders):
    out = {r["order_date"]: r for r in ga.build_daily_sales(orders, athena_types=False).collect()}
    assert out[D1]["total_revenue"] == Decimal("1020.29")
    assert out[D1]["total_orders"] == 2  # ORD_1 counted once despite 2 lines
    assert out[D1]["aov"] == Decimal("510.15")  # 510.145 rounds half-up
    assert out[D2]["total_revenue"] == Decimal("300.01")
    assert out[D2]["total_orders"] == 2
    assert out[D2]["aov"] == Decimal("150.01")  # 150.005 rounds half-up


def test_daily_sales_matches_athena_schema(spark, orders):
    expected_schema = StructType(
        [
            StructField("order_date", DateType()),
            StructField("total_revenue", DoubleType()),
            StructField("total_orders", LongType()),
            StructField("aov", DoubleType()),
        ]
    )
    expected = spark.createDataFrame(
        [(D1, 1020.29, 2, 510.15), (D2, 300.01, 2, 150.01)], expected_schema
    )
    assert_df_equality(ga.build_daily_sales(orders), expected, ignore_nullable=True)


def test_inventory_health_reorder_flag(spark):
    inv = spark.createDataFrame(
        [
            ("PROD_1", "A", "Books", 5, 10),  # below -> 1
            ("PROD_2", "B", "Books", 10, 10),  # at threshold -> 1
            ("PROD_3", "C", "Books", 11, 10),  # above -> 0
            ("PROD_4", "D", "Books", 0, 25),  # stockout -> 1
        ],
        "product_id string, product_name string, category string, current_stock int, reorder_threshold int",
    )
    out = ga.build_inventory_health(inv)
    assert out.columns == ga.INVENTORY_HEALTH_COLUMNS
    assert dict(out.dtypes)["reorder_flag"] == "int"
    assert [r["reorder_flag"] for r in out.collect()] == [1, 1, 0, 1]


def test_customer_metrics_tiers_and_zero_order_customer(spark, orders):
    customers = spark.createDataFrame(
        [("CUST_A",), ("CUST_B",), ("CUST_C",), ("CUST_Z",)], "customer_id string"
    )
    out = {r["customer_id"]: r for r in ga.build_customer_metrics(customers, orders, athena_types=False).collect()}

    assert out["CUST_A"]["total_spent"] == Decimal("20.30") and out["CUST_A"]["clv_tier"] == "Low"
    assert out["CUST_A"]["total_orders"] == 1
    # 999.99 + 0.01 = 1000.00 exactly -> High (boundary is >=)
    assert out["CUST_B"]["total_spent"] == Decimal("1000.00") and out["CUST_B"]["clv_tier"] == "High"
    assert out["CUST_B"]["total_orders"] == 2
    # exactly 300.00 -> Medium (boundary is >=)
    assert out["CUST_C"]["total_spent"] == Decimal("300.00") and out["CUST_C"]["clv_tier"] == "Medium"
    # zero-order customer kept
    assert out["CUST_Z"]["total_spent"] == Decimal("0.00")
    assert out["CUST_Z"]["total_orders"] == 0 and out["CUST_Z"]["clv_tier"] == "Low"


def test_customer_metrics_athena_types(spark, orders):
    customers = spark.createDataFrame([("CUST_A",)], "customer_id string")
    out = ga.build_customer_metrics(customers, orders)
    assert out.dtypes == [
        ("customer_id", "string"),
        ("total_spent", "double"),
        ("total_orders", "bigint"),
        ("clv_tier", "string"),
    ]


def test_missing_columns_raise(spark):
    bad = spark.createDataFrame([("ORD_1",)], "order_id string")
    with pytest.raises(ValueError, match="missing required columns"):
        ga.build_daily_sales(bad)


def test_run_gold_requires_silver(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        ga.run_gold(spark, tmp_path / "silver", tmp_path / "gold")


@pytest.mark.skipif(not (RAW_DIR / "orders.csv").is_file(), reason="Phase 1 raw data not generated")
def test_phase1_gold_reconciliation(spark, tmp_path):
    silver_dir, gold_dir = tmp_path / "silver", tmp_path / "gold"
    sc.run_silver(spark, RAW_DIR, silver_dir)
    for _ in range(2):  # rerun must overwrite, not double-count
        counts = ga.run_gold(spark, silver_dir, gold_dir)

    assert counts["gold_inventory_health"] == 294
    assert counts["gold_customer_metrics"] == 985

    orders = spark.read.parquet(str(silver_dir / "orders"))
    exact = ga.build_daily_sales(orders, athena_types=False)
    totals = exact.agg({"total_revenue": "sum", "total_orders": "sum"}).first()
    assert totals[0] == Decimal("5016285.01")  # matches independent pandas Decimal check
    assert totals[1] == 7044  # each order has exactly one order_date
    assert orders.select("order_id").distinct().count() == 7044

    daily = spark.read.parquet(str(gold_dir / "gold_daily_sales"))
    assert dict(daily.dtypes) == {"order_date": "date", "total_revenue": "double", "total_orders": "bigint", "aov": "double"}
