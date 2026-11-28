"""
Local end-to-end pipeline runner: Bronze CSV -> Silver Parquet -> Gold Parquet.

Why this exists
---------------
Silver and Gold are separate modules so each can be tested on its own. This runner
chains them in the order the TRD prescribes (section 7) and prints a reconciliation
summary, so one command reproduces the whole local pipeline. It is local only: it
reads and writes folders on disk and never touches AWS.

Run it from the project root:
    .\\.venv\\Scripts\\python.exe -m src.pipeline
    .\\.venv\\Scripts\\python.exe -m src.pipeline --raw-dir data/raw --output-dir data

Outputs (each overwritten on every run, so reruns never double-count):
    data/silver/{customers,inventory,orders}/
    data/silver/quarantine/{customers,inventory,orders}/
    data/gold/{gold_daily_sales,gold_inventory_health,gold_customer_metrics}/
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from src.etl.gold_aggregate import run_gold
from src.etl.silver_clean import DEFAULT_REFERENCE_DATE, run_silver
from src.utils.paths import ensure_no_overlap
from src.utils.spark_session import get_spark_session, stop_spark_session

logger = logging.getLogger("pipeline")

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class PipelineSummary:
    """Row counts from one run, used for the printed report and for tests."""

    silver: Dict[str, Dict[str, int]] = field(default_factory=dict)
    quarantine_reasons: Dict[str, Dict[str, int]] = field(default_factory=dict)
    gold: Dict[str, int] = field(default_factory=dict)

    def lines(self) -> List[str]:
        out = ["Silver reconciliation (raw = silver + quarantine):"]
        for table, c in self.silver.items():
            out.append(f"  {table:<10} {c['raw']:>6} = {c['silver']:>6} + {c['quarantine']:>4}")
            for reason, n in sorted(self.quarantine_reasons.get(table, {}).items()):
                out.append(f"      {reason:<24} {n:>4}")
        out.append("Gold row counts:")
        for table, n in self.gold.items():
            out.append(f"  {table:<24} {n:>6}")
        return out


def run_pipeline(
    raw_dir: Path,
    output_dir: Path,
    reference_date: str = DEFAULT_REFERENCE_DATE,
    master: Optional[str] = None,
) -> PipelineSummary:
    """Run Silver then Gold. Raises on any validation or reconciliation failure."""
    raw_dir, output_dir = Path(raw_dir), Path(output_dir)
    if not raw_dir.is_dir():
        raise FileNotFoundError(f"Raw input directory not found: {raw_dir}")
    silver_dir, gold_dir = output_dir / "silver", output_dir / "gold"
    # Validate before any write: the folders Spark overwrites must not be, contain,
    # or sit inside the raw folder (canonicalized, so ../ and symlinks can't sneak past).
    ensure_no_overlap(raw_dir, silver_dir, "raw directory", "Silver directory")
    ensure_no_overlap(raw_dir, gold_dir, "raw directory", "Gold directory")
    run_ts = datetime.now(timezone.utc).replace(microsecond=0)

    spark = get_spark_session("EcommerceLocalPipeline", master=master, shuffle_partitions=8)
    summary = PipelineSummary()

    logger.info("Stage 1/2: Silver (raw=%s -> %s)", raw_dir, silver_dir)
    results = run_silver(spark, raw_dir, silver_dir, reference_date=reference_date, rejected_at=run_ts)
    for name, r in results.items():
        summary.silver[name] = {"raw": r.raw_count, "silver": r.silver_count, "quarantine": r.quarantine_count}
        summary.quarantine_reasons[name] = r.reason_counts()
        r.silver.unpersist()
        r.quarantine.unpersist()

    logger.info("Stage 2/2: Gold (%s -> %s)", silver_dir, gold_dir)
    summary.gold = run_gold(spark, silver_dir, gold_dir)
    return summary


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local Bronze -> Silver -> Gold pipeline.")
    parser.add_argument("--raw-dir", default=str(PROJECT_ROOT / "data" / "raw"), help="Folder with the 3 raw CSVs")
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "data"), help="Parent folder for silver/ and gold/")
    parser.add_argument(
        "--reference-date",
        default=DEFAULT_REFERENCE_DATE,
        help="YYYY-MM-DD anchor for future-signup checks; must match the generator run",
    )
    parser.add_argument("--master", default=None, help="Spark master, default local[*]")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None, stop_spark: bool = True) -> int:
    """CLI entry point. Returns 0 on success, 1 on a handled failure.

    ``stop_spark=False`` lets tests reuse their shared SparkSession.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    try:
        summary = run_pipeline(Path(args.raw_dir), Path(args.output_dir), args.reference_date, args.master)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Pipeline failed: %s", exc)
        return 1
    finally:
        if stop_spark:
            stop_spark_session()
    print("\n".join(summary.lines()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
