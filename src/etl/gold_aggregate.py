"""
Gold layer: business KPIs aggregated from validated Silver data.

What this module does
---------------------
Gold tables are small, query-ready summaries that Athena and Power BI read directly.
Every number here is derived ONLY from Silver (already cleaned and deduplicated).

Approved business rules (PRD section 7, docs/decisions.md)
---------------------------------------------------------
* line revenue   = quantity x unit_price (no tax, discount, shipping or returns)
* total_revenue  = sum of line revenue
* total_orders   = count of DISTINCT order_id (orders are line-level, so a 3-line
                   order must count once, not three times)
* aov            = total_revenue / total_orders, rounded half-up to 2 decimals
* reorder_flag   = 1 if current_stock <= reorder_threshold else 0
* clv_tier       = High if total_spent >= 1000, Medium if 300 <= total_spent < 1000,
                   Low otherwise (customers with zero orders: total_spent 0, Low)

Financial precision (approved 2026-11-28)
----------------------------------------
Money is computed with Decimal arithmetic so cents never drift:
  unit_price DECIMAL(10,2) -> line revenue DECIMAL(12,2) -> aggregates DECIMAL(14,2)
Only at the very end are money columns cast to DOUBLE, because the existing Athena
DDL (aws/athena_ddl.sql) declares them DOUBLE. Output schemas match that DDL.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path
from typing import Dict, Sequence

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DoubleType, IntegerType, LongType

from src.utils.paths import ensure_no_overlap, validate_write_destinations

GOLD_TABLES = ("gold_daily_sales", "gold_inventory_health", "gold_customer_metrics")


def gold_write_destinations(gold_dir: str | Path) -> list[Path]:
    """Every folder run_gold overwrites."""
    return [Path(gold_dir) / t for t in GOLD_TABLES]

logger = logging.getLogger(__name__)

LINE_REVENUE_TYPE = DecimalType(12, 2)
AGGREGATE_MONEY_TYPE = DecimalType(14, 2)

CLV_HIGH_THRESHOLD = Decimal("1000.00")
CLV_MEDIUM_THRESHOLD = Decimal("300.00")

# Column order follows aws/athena_ddl.sql exactly.
DAILY_SALES_COLUMNS = ["order_date", "total_revenue", "total_orders", "aov"]
INVENTORY_HEALTH_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "current_stock",
    "reorder_threshold",
    "reorder_flag",
]
CUSTOMER_METRICS_COLUMNS = ["customer_id", "total_spent", "total_orders", "clv_tier"]

_REQUIRED_SILVER = {
    "orders": ["order_id", "customer_id", "order_date", "product_id", "quantity", "unit_price"],
    "inventory": ["product_id", "product_name", "category", "current_stock", "reorder_threshold"],
    "customers": ["customer_id"],
}


def _require_columns(df: DataFrame, table: str) -> None:
    """Fail early with a clear message if a Silver input is missing columns."""
    missing = [c for c in _REQUIRED_SILVER[table] if c not in df.columns]
    if missing:
        raise ValueError(f"Silver {table} is missing required columns: {missing}")


def line_revenue(quantity: str = "quantity", unit_price: str = "unit_price") -> Column:
    """quantity x unit_price as DECIMAL(12,2).

    The unit price is cast to DECIMAL(10,2) first so the multiplication is exact
    decimal math even if an upstream column arrived as another numeric type.
    """
    return (F.col(quantity).cast(LongType()) * F.col(unit_price).cast(DecimalType(10, 2))).cast(
        LINE_REVENUE_TYPE
    )


def _sum_money(col: Column) -> Column:
    return F.sum(col).cast(AGGREGATE_MONEY_TYPE)


def _aov(revenue: Column, orders: Column) -> Column:
    """Revenue / orders, rounded half-up to cents; NULL-safe for zero orders."""
    return F.when(orders > 0, F.round(revenue / orders, 2).cast(AGGREGATE_MONEY_TYPE))


def to_athena_money(df: DataFrame, money_columns: Sequence[str]) -> DataFrame:
    """Cast Decimal money columns to DOUBLE to match the existing Athena DDL."""
    for name in money_columns:
        df = df.withColumn(name, F.col(name).cast(DoubleType()))
    return df


# ---------------------------------------------------------------------------
# gold_daily_sales
# ---------------------------------------------------------------------------
def build_daily_sales(orders: DataFrame, athena_types: bool = True) -> DataFrame:
    """One row per order_date: total_revenue, total_orders (distinct), aov.

    Args:
        orders: Silver orders.
        athena_types: True casts money to DOUBLE for writing; False keeps Decimal
            (useful for exact checks in tests and reconciliation).
    """
    _require_columns(orders, "orders")
    daily = (
        orders.withColumn("_line_revenue", line_revenue())
        .groupBy("order_date")
        .agg(
            _sum_money(F.col("_line_revenue")).alias("total_revenue"),
            F.countDistinct("order_id").cast(LongType()).alias("total_orders"),
        )
        .withColumn("aov", _aov(F.col("total_revenue"), F.col("total_orders")))
        .select(*DAILY_SALES_COLUMNS)
        .orderBy("order_date")
    )
    return to_athena_money(daily, ["total_revenue", "aov"]) if athena_types else daily


# ---------------------------------------------------------------------------
# gold_inventory_health
# ---------------------------------------------------------------------------
def build_inventory_health(inventory: DataFrame) -> DataFrame:
    """One row per product with reorder_flag = 1 when stock is at/below threshold."""
    _require_columns(inventory, "inventory")
    return (
        inventory.withColumn(
            "reorder_flag",
            F.when(F.col("current_stock") <= F.col("reorder_threshold"), 1).otherwise(0).cast(IntegerType()),
        )
        .withColumn("current_stock", F.col("current_stock").cast(IntegerType()))
        .withColumn("reorder_threshold", F.col("reorder_threshold").cast(IntegerType()))
        .select(*INVENTORY_HEALTH_COLUMNS)
        .orderBy("product_id")
    )


# ---------------------------------------------------------------------------
# gold_customer_metrics
# ---------------------------------------------------------------------------
def clv_tier(total_spent: Column) -> Column:
    """Approved CLV tiers (docs/decisions.md section 3)."""
    return (
        F.when(total_spent >= F.lit(CLV_HIGH_THRESHOLD), "High")
        .when(total_spent >= F.lit(CLV_MEDIUM_THRESHOLD), "Medium")
        .otherwise("Low")
    )


def build_customer_metrics(customers: DataFrame, orders: DataFrame, athena_types: bool = True) -> DataFrame:
    """One row per Silver customer: total_spent, total_orders (distinct), clv_tier.

    Base = Silver customers LEFT JOIN their order totals, so customers with zero
    valid orders are kept (total_spent 0.00, total_orders 0, tier Low).
    """
    _require_columns(customers, "customers")
    _require_columns(orders, "orders")

    per_customer = (
        orders.withColumn("_line_revenue", line_revenue())
        .groupBy("customer_id")
        .agg(
            _sum_money(F.col("_line_revenue")).alias("total_spent"),
            F.countDistinct("order_id").cast(LongType()).alias("total_orders"),
        )
    )
    metrics = (
        customers.select("customer_id")
        .join(per_customer, on="customer_id", how="left")
        .withColumn(
            "total_spent",
            F.coalesce(F.col("total_spent"), F.lit(Decimal("0.00")).cast(AGGREGATE_MONEY_TYPE)),
        )
        .withColumn("total_orders", F.coalesce(F.col("total_orders"), F.lit(0).cast(LongType())))
        .withColumn("clv_tier", clv_tier(F.col("total_spent")))
        .select(*CUSTOMER_METRICS_COLUMNS)
        .orderBy("customer_id")
    )
    return to_athena_money(metrics, ["total_spent"]) if athena_types else metrics


# ---------------------------------------------------------------------------
# Orchestration for the Gold stage
# ---------------------------------------------------------------------------
def build_gold(customers: DataFrame, inventory: DataFrame, orders: DataFrame) -> Dict[str, DataFrame]:
    """Build all three Gold tables (Athena-typed) from Silver DataFrames."""
    return {
        "gold_daily_sales": build_daily_sales(orders),
        "gold_inventory_health": build_inventory_health(inventory),
        "gold_customer_metrics": build_customer_metrics(customers, orders),
    }


def run_gold(spark: SparkSession, silver_dir: str | Path, gold_dir: str | Path) -> Dict[str, int]:
    """Read Silver Parquet, write the three Gold Parquet tables, return row counts.

    Each table is written with mode("overwrite") so reruns replace, never append.
    """
    silver_dir, gold_dir = Path(silver_dir), Path(gold_dir)
    ensure_no_overlap(silver_dir, gold_dir, "Silver directory", "Gold directory")
    validate_write_destinations(silver_dir, gold_dir, gold_write_destinations(gold_dir), "Silver directory")
    for name in ("customers", "inventory", "orders"):
        if not (silver_dir / name).exists():
            raise FileNotFoundError(f"Silver input not found: {silver_dir / name}. Run the Silver stage first.")

    tables = build_gold(
        customers=spark.read.parquet(str(silver_dir / "customers")),
        inventory=spark.read.parquet(str(silver_dir / "inventory")),
        orders=spark.read.parquet(str(silver_dir / "orders")),
    )
    counts: Dict[str, int] = {}
    for name, df in tables.items():
        df.write.mode("overwrite").parquet(str(gold_dir / name))
        counts[name] = spark.read.parquet(str(gold_dir / name)).count()
        logger.info("%s: %d rows", name, counts[name])
    return counts
