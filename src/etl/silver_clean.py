"""
Silver layer: clean, type and validate the raw (Bronze) CSV files.

What this module does
---------------------
Bronze is the raw CSV exactly as the generator wrote it. Silver is the same data
after cleaning, which Gold can trust:

1. Read each CSV with every column as a STRING, so a bad value can never crash the
   read or be silently turned into NULL before we look at it.
2. Check the header matches the approved contract (coordination/API_AND_DATA_CONTRACTS.md).
3. Cast each column to its contract type (dates, ints, Decimal(10,2) prices).
4. Apply the human-approved rejection rules (approved 2026-11-28). Every rejected
   row is kept in a *quarantine* DataFrame with its original raw columns plus
   ``rejection_reason`` and ``rejected_at``; nothing is dropped without a trace.
5. Deduplicate the remaining valid rows deterministically and quarantine the
   duplicate replays.
6. Reconcile: raw rows == Silver rows + quarantine rows, or the job fails.

Approved rejection rules (one reason per row; the first matching rule wins)
--------------------------------------------------------------------------
customers: blank_country, future_signup_date, then duplicate_customer_id
inventory: negative_stock, blank_product_name
orders:    null_customer_id, negative_quantity, zero_unit_price, then duplicate_order_line

Schema-conformance checks (``invalid_<field>``) reject a row only if a value cannot be
cast to its contract type or a non-nullable key is blank. They never fire on the
approved Phase 1 data; they exist so a malformed file fails loudly instead of
producing NULLs in Silver.

Orders are NOT rejected because of their relationship with customer signup dates
(contract section 3.3 says order_date may precede signup_date).

Expected reconciliation for the approved Phase 1 data (seed=42, ref 2026-11-19):
  customers 1,010 raw = 985 Silver + 25 quarantine
  inventory   300 raw = 294 Silver +  6 quarantine
  orders   10,000 raw = 9,700 Silver + 300 quarantine
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, TimestampType

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Contract constants
# ---------------------------------------------------------------------------
# reference_date anchors "future" signup dates. It must match the generator run
# (contract section 1.2), NOT today's wall clock, or counts would drift over time.
DEFAULT_REFERENCE_DATE: str = "2026-11-19"

CUSTOMER_COLUMNS: List[str] = ["customer_id", "signup_date", "country"]
INVENTORY_COLUMNS: List[str] = [
    "product_id",
    "product_name",
    "category",
    "current_stock",
    "reorder_threshold",
]
ORDER_COLUMNS: List[str] = [
    "order_id",
    "customer_id",
    "order_date",
    "product_id",
    "quantity",
    "unit_price",
]

UNIT_PRICE_TYPE = DecimalType(10, 2)  # approved financial precision for unit prices

REJECTION_REASON_COL = "rejection_reason"
REJECTED_AT_COL = "rejected_at"
_ROW_ID = "_source_row_id"  # internal: preserves file order for deterministic dedup

ISO_DATE_REGEX = r"^\d{4}-\d{2}-\d{2}$"
INT_REGEX = r"^-?\d+$"


@dataclass
class CleanResult:
    """Output of cleaning one table: typed Silver rows + quarantined raw rows."""

    table: str
    silver: DataFrame
    quarantine: DataFrame
    raw_count: int
    silver_count: int
    quarantine_count: int

    def reason_counts(self) -> Dict[str, int]:
        """Return {rejection_reason: row_count} for the quarantine (for reporting/tests)."""
        rows = self.quarantine.groupBy(REJECTION_REASON_COL).count().collect()
        return {r[REJECTION_REASON_COL]: r["count"] for r in rows}


# ---------------------------------------------------------------------------
# Reading Bronze
# ---------------------------------------------------------------------------
def read_raw_csv(spark: SparkSession, path: str | Path, expected_columns: Sequence[str]) -> DataFrame:
    """Read a raw CSV with every column as STRING and validate its header.

    Why strings: if Spark inferred types, a value like "abc" in an INT column would
    become NULL silently. Reading strings lets *us* decide what is invalid.

    Raises:
        FileNotFoundError: the file does not exist.
        ValueError: the header does not match the approved contract exactly.
    """
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Raw input file not found: {path}")

    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .csv(str(path))
    )
    if df.columns != list(expected_columns):
        raise ValueError(
            f"Header mismatch in {path.name}: expected {list(expected_columns)}, got {df.columns}"
        )
    # monotonically_increasing_id() grows with file position (partition id is the
    # high bits), so ordering by it reproduces the original CSV row order.
    return df.withColumn(_ROW_ID, F.monotonically_increasing_id())


# ---------------------------------------------------------------------------
# Small column helpers
# ---------------------------------------------------------------------------
def _is_blank(col_name: str) -> Column:
    """True when a raw string value is NULL, empty, or whitespace only."""
    c = F.col(col_name)
    return c.isNull() | (F.trim(c) == "")


def _parse_date(col_name: str) -> Column:
    """Strict YYYY-MM-DD parse; returns NULL (never raises) for invalid values.

    ``try_to_timestamp`` is used because Spark 4 runs in ANSI mode, where plain
    ``to_date`` throws on a bad value. The regex rejects forms like ``2026-1-5``.
    """
    c = F.trim(F.col(col_name))
    parsed = F.to_date(F.expr(f"try_to_timestamp(trim(`{col_name}`), 'yyyy-MM-dd')"))
    return F.when(c.rlike(ISO_DATE_REGEX), parsed)


def _parse_int(col_name: str) -> Column:
    """Integer parse that yields NULL instead of failing on bad input."""
    c = F.trim(F.col(col_name))
    return F.when(c.rlike(INT_REGEX), F.expr(f"try_cast(trim(`{col_name}`) AS INT)"))


def _parse_decimal(col_name: str, dtype: DecimalType) -> Column:
    """Parse a string straight to Decimal (never via float, so 161.44 stays exact)."""
    return F.expr(f"try_cast(trim(`{col_name}`) AS DECIMAL({dtype.precision},{dtype.scale}))")


def _first_reason(rules: Sequence[tuple[Column, str]]) -> Column:
    """Build a CASE WHEN chain: the first rule whose condition is true names the reason."""
    expr: Optional[Column] = None
    for condition, reason in rules:
        expr = F.when(condition, F.lit(reason)) if expr is None else expr.when(condition, F.lit(reason))
    return expr if expr is not None else F.lit(None).cast("string")


def _split(
    typed: DataFrame,
    raw_columns: Sequence[str],
    typed_columns: Dict[str, Column],
    dedup_keys: Sequence[str],
    duplicate_reason: str,
    rejected_at: datetime,
) -> tuple[DataFrame, DataFrame]:
    """Separate rows into (silver, quarantine) after rule checks and deduplication.

    Steps:
      1. Rows with a non-NULL rejection_reason are rejected.
      2. Among the remaining valid rows, rank rows sharing the same key by original
         file position; rank 1 is kept, the rest become ``duplicate_reason``.
         There is no timestamp/version column in the contract, so file order is the
         only meaningful, deterministic ordering available.
    """
    w = Window.partitionBy(*dedup_keys).orderBy(F.col(_ROW_ID).asc())
    valid = typed.filter(F.col(REJECTION_REASON_COL).isNull()).withColumn("_rank", F.row_number().over(w))
    invalid = typed.filter(F.col(REJECTION_REASON_COL).isNotNull())

    duplicates = valid.filter(F.col("_rank") > 1).withColumn(REJECTION_REASON_COL, F.lit(duplicate_reason))
    kept = valid.filter(F.col("_rank") == 1)

    silver = kept.orderBy(_ROW_ID).select(*[expr.alias(name) for name, expr in typed_columns.items()])

    quarantine = (
        invalid.select(*raw_columns, _ROW_ID, REJECTION_REASON_COL)
        .unionByName(duplicates.select(*raw_columns, _ROW_ID, REJECTION_REASON_COL))
        .orderBy(_ROW_ID)
        .withColumn(REJECTED_AT_COL, F.lit(rejected_at).cast(TimestampType()))
        .drop(_ROW_ID)
    )
    return silver, quarantine


def _finish(table: str, raw: DataFrame, silver: DataFrame, quarantine: DataFrame) -> CleanResult:
    """Cache outputs, count them, and enforce raw == silver + quarantine."""
    silver = silver.cache()
    quarantine = quarantine.cache()
    raw_count = raw.count()
    silver_count = silver.count()
    quarantine_count = quarantine.count()
    reconcile(table, raw_count, silver_count, quarantine_count)
    logger.info(
        "%s: raw=%d silver=%d quarantine=%d", table, raw_count, silver_count, quarantine_count
    )
    return CleanResult(table, silver, quarantine, raw_count, silver_count, quarantine_count)


def reconcile(table: str, raw_count: int, silver_count: int, quarantine_count: int) -> None:
    """Fail loudly if any row was lost or double-counted during cleaning."""
    if raw_count != silver_count + quarantine_count:
        raise ValueError(
            f"Reconciliation failed for {table}: raw={raw_count} != "
            f"silver={silver_count} + quarantine={quarantine_count}"
        )


def _validate_reference_date(reference_date: str) -> date:
    try:
        return datetime.strptime(reference_date, "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise ValueError(f"reference_date must be YYYY-MM-DD, got {reference_date!r}") from exc


def _default_rejected_at(rejected_at: Optional[datetime]) -> datetime:
    """One timestamp per run, in UTC, so every quarantined row of a run shares it."""
    if rejected_at is None:
        return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    if rejected_at.tzinfo is not None:
        rejected_at = rejected_at.astimezone(timezone.utc).replace(tzinfo=None)
    return rejected_at


# ---------------------------------------------------------------------------
# Table cleaners
# ---------------------------------------------------------------------------
def clean_customers(
    raw: DataFrame,
    reference_date: str = DEFAULT_REFERENCE_DATE,
    rejected_at: Optional[datetime] = None,
) -> CleanResult:
    """Clean customers: quarantine blank country, future signup and duplicate replays.

    Silver schema: customer_id STRING, signup_date DATE, country STRING.
    """
    ref = _validate_reference_date(reference_date)
    rejected_at = _default_rejected_at(rejected_at)

    typed = raw.withColumn("_signup_date", _parse_date("signup_date"))
    typed = typed.withColumn(
        REJECTION_REASON_COL,
        _first_reason(
            [
                (_is_blank("customer_id"), "invalid_customer_id"),
                (F.col("_signup_date").isNull(), "invalid_signup_date"),
                (_is_blank("country"), "blank_country"),
                (F.col("_signup_date") > F.lit(ref), "future_signup_date"),
            ]
        ),
    )
    silver, quarantine = _split(
        typed,
        CUSTOMER_COLUMNS,
        {
            "customer_id": F.col("customer_id"),
            "signup_date": F.col("_signup_date"),
            "country": F.col("country"),
        },
        dedup_keys=["customer_id"],
        duplicate_reason="duplicate_customer_id",
        rejected_at=rejected_at,
    )
    return _finish("customers", raw, silver, quarantine)


def clean_inventory(raw: DataFrame, rejected_at: Optional[datetime] = None) -> CleanResult:
    """Clean inventory: quarantine negative stock and blank product names.

    Silver schema: product_id STRING, product_name STRING, category STRING,
    current_stock INT, reorder_threshold INT.
    """
    rejected_at = _default_rejected_at(rejected_at)
    typed = raw.withColumn("_current_stock", _parse_int("current_stock")).withColumn(
        "_reorder_threshold", _parse_int("reorder_threshold")
    )
    typed = typed.withColumn(
        REJECTION_REASON_COL,
        _first_reason(
            [
                (_is_blank("product_id"), "invalid_product_id"),
                (F.col("_current_stock").isNull(), "invalid_current_stock"),
                (F.col("_reorder_threshold").isNull(), "invalid_reorder_threshold"),
                (F.col("_current_stock") < 0, "negative_stock"),
                (_is_blank("product_name"), "blank_product_name"),
            ]
        ),
    )
    silver, quarantine = _split(
        typed,
        INVENTORY_COLUMNS,
        {
            "product_id": F.col("product_id"),
            "product_name": F.col("product_name"),
            "category": F.col("category"),
            "current_stock": F.col("_current_stock"),
            "reorder_threshold": F.col("_reorder_threshold"),
        },
        dedup_keys=["product_id"],
        # product_id is unique in the contract; this only fires on a contract breach.
        duplicate_reason="duplicate_product_id",
        rejected_at=rejected_at,
    )
    return _finish("inventory", raw, silver, quarantine)


def clean_orders(raw: DataFrame, rejected_at: Optional[datetime] = None) -> CleanResult:
    """Clean order lines: quarantine null customer, negative qty, zero price, duplicates.

    Silver schema: order_id STRING, customer_id STRING, order_date DATE,
    product_id STRING, quantity INT, unit_price DECIMAL(10,2).
    """
    rejected_at = _default_rejected_at(rejected_at)
    typed = (
        raw.withColumn("_order_date", _parse_date("order_date"))
        .withColumn("_quantity", _parse_int("quantity"))
        .withColumn("_unit_price", _parse_decimal("unit_price", UNIT_PRICE_TYPE))
    )
    typed = typed.withColumn(
        REJECTION_REASON_COL,
        _first_reason(
            [
                (_is_blank("order_id"), "invalid_order_id"),
                (_is_blank("product_id"), "invalid_product_id"),
                (F.col("_order_date").isNull(), "invalid_order_date"),
                (F.col("_quantity").isNull(), "invalid_quantity"),
                (F.col("_unit_price").isNull(), "invalid_unit_price"),
                (_is_blank("customer_id"), "null_customer_id"),
                (F.col("_quantity") < 0, "negative_quantity"),
                (F.col("_unit_price") == 0, "zero_unit_price"),
            ]
        ),
    )
    silver, quarantine = _split(
        typed,
        ORDER_COLUMNS,
        {
            "order_id": F.col("order_id"),
            "customer_id": F.col("customer_id"),
            "order_date": F.col("_order_date"),
            "product_id": F.col("product_id"),
            "quantity": F.col("_quantity"),
            "unit_price": F.col("_unit_price"),
        },
        dedup_keys=["order_id", "product_id"],
        duplicate_reason="duplicate_order_line",
        rejected_at=rejected_at,
    )
    return _finish("orders", raw, silver, quarantine)


# ---------------------------------------------------------------------------
# Orchestration for the Silver stage
# ---------------------------------------------------------------------------
def write_parquet(df: DataFrame, path: str | Path) -> None:
    """Overwrite a Parquet folder. Overwrite (not append) keeps reruns idempotent."""
    df.write.mode("overwrite").parquet(str(path))


def run_silver(
    spark: SparkSession,
    raw_dir: str | Path,
    silver_dir: str | Path,
    reference_date: str = DEFAULT_REFERENCE_DATE,
    rejected_at: Optional[datetime] = None,
) -> Dict[str, CleanResult]:
    """Clean all three raw files and write Silver + quarantine Parquet.

    Output layout:
      <silver_dir>/customers|inventory|orders/
      <silver_dir>/quarantine/customers|inventory|orders/
    """
    raw_dir, silver_dir = Path(raw_dir), Path(silver_dir)
    rejected_at = _default_rejected_at(rejected_at)

    results = {
        "customers": clean_customers(
            read_raw_csv(spark, raw_dir / "customers.csv", CUSTOMER_COLUMNS),
            reference_date=reference_date,
            rejected_at=rejected_at,
        ),
        "inventory": clean_inventory(
            read_raw_csv(spark, raw_dir / "inventory.csv", INVENTORY_COLUMNS), rejected_at=rejected_at
        ),
        "orders": clean_orders(
            read_raw_csv(spark, raw_dir / "orders.csv", ORDER_COLUMNS), rejected_at=rejected_at
        ),
    }
    for name, result in results.items():
        write_parquet(result.silver, silver_dir / name)
        write_parquet(result.quarantine, silver_dir / "quarantine" / name)
    return results
