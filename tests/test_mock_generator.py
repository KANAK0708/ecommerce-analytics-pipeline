"""Phase 1 acceptance tests for coordination/API_AND_DATA_CONTRACTS.md."""

import csv
import importlib.util
import re
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest


GENERATOR = Path(__file__).resolve().parents[1] / "data" / "mock_generator.py"
REFERENCE_DATE = date(2026, 11, 19)
HEADERS = {
    "customers": ("customer_id", "signup_date", "country"),
    "inventory": (
        "product_id", "product_name", "category", "current_stock", "reorder_threshold",
    ),
    "orders": ("order_id", "customer_id", "order_date", "product_id", "quantity", "unit_price"),
}
ROW_COUNTS = {"customers": 1010, "inventory": 300, "orders": 10_000}
CUSTOMER_IDS = {f"CUST_{number}" for number in range(10001, 11001)}
PRODUCT_IDS = {f"PROD_{number}" for number in range(1001, 1301)}
COUNTRIES = {"US", "CA", "GB", "DE", "FR", "IN", "JP", "AU"}
CATEGORIES = {
    "Electronics", "Home & Kitchen", "Apparel", "Health & Personal Care", "Books",
    "Sports & Outdoors",
}


def invoke_generator(directory, seed, reference_date):
    script_dir = directory / "data"
    script_dir.mkdir()
    script = script_dir / GENERATOR.name
    # The legacy CLI writes beside its script, so isolate it from repository data.
    shutil.copyfile(GENERATOR, script)
    output = directory / "generated"
    result = subprocess.run(
        [sys.executable, "-B", str(script), "--seed", str(seed),
         "--reference-date", reference_date, "--output-dir", str(output)],
        cwd=directory,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return output, result


def run_generator(directory, seed=42, reference_date=REFERENCE_DATE):
    output, result = invoke_generator(directory, seed, reference_date.isoformat())
    assert result.returncode == 0, result.stderr or result.stdout
    expected_files = {f"{name}.csv" for name in HEADERS}
    actual_files = set()
    if output.is_dir():
        actual_files = {path.name for path in output.iterdir() if path.is_file()}
    assert expected_files <= actual_files, (
        f"Missing from --output-dir: {sorted(expected_files - actual_files)}\n{result.stdout}"
    )
    return output


def read_rows(output, dataset):
    with (output / f"{dataset}.csv").open(encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        assert tuple(next(reader, ())) == HEADERS[dataset], f"{dataset}: incorrect CSV header"
        rows = []
        for number, values in enumerate(reader, 1):
            assert len(values) == len(HEADERS[dataset]), f"{dataset} data row {number}: wrong width"
            rows.append(dict(zip(HEADERS[dataset], values)))
    return rows


@pytest.fixture(scope="module")
def nominal_output(tmp_path_factory):
    return run_generator(tmp_path_factory.mktemp("seed42"))


@pytest.fixture(scope="module")
def nominal_rows(nominal_output):
    return {name: read_rows(nominal_output, name) for name in HEADERS}


def parse_date(value):
    assert re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value), value
    return date.fromisoformat(value)


def assert_date_ranges(customers, orders, reference_date):
    for row in customers:
        offset = (parse_date(row["signup_date"]) - reference_date).days
        assert -730 <= offset <= 30, row
    for row in orders:
        offset = (parse_date(row["order_date"]) - reference_date).days
        assert -180 <= offset <= 0, row


@pytest.mark.parametrize("dataset", HEADERS)
def test_csv_header_record_count_and_empty_fields(nominal_output, dataset):
    rows = read_rows(nominal_output, dataset)
    assert len(rows) == ROW_COUNTS[dataset]
    intentional_blanks = {
        ("customers", "country"): 10,
        ("inventory", "product_name"): 3,
        ("orders", "customer_id"): 100,
    }
    for field in HEADERS[dataset]:
        assert sum(row[field] == "" for row in rows) == intentional_blanks.get((dataset, field), 0), field
        assert all(row[field] == "" or row[field].strip() for row in rows), field


def test_field_types_and_allowed_values(nominal_rows):
    for row in nominal_rows["customers"]:
        assert row["country"] == "" or row["country"] in COUNTRIES, row
    for row in nominal_rows["inventory"]:
        assert row["category"] in CATEGORIES, row
        assert re.fullmatch(r"-?[0-9]+", row["current_stock"]), row
        assert int(row["current_stock"]) >= -10, row
        assert re.fullmatch(r"[0-9]+", row["reorder_threshold"]), row
        assert int(row["reorder_threshold"]) in {10, 20, 25, 50, 75, 100}, row
    for row in nominal_rows["orders"]:
        assert re.fullmatch(r"ORD_[0-9]{6}", row["order_id"]), row
        assert re.fullmatch(r"-?[0-9]+", row["quantity"]), row
        assert int(row["quantity"]) in {-5, -4, -3, -2, -1, 1, 2, 3, 4, 5, 10}, row
        assert re.fullmatch(r"[0-9]+(?:\.[0-9]{1,2})?", row["unit_price"]), row
        price = Decimal(row["unit_price"])
        assert price == 0 or Decimal("5.99") <= price <= Decimal("499.99"), row


def test_date_formats_and_ranges(nominal_rows):
    assert_date_ranges(nominal_rows["customers"], nominal_rows["orders"], REFERENCE_DATE)


def assert_keys_and_foreign_keys(rows):
    customer_ids = {row["customer_id"] for row in rows["customers"]}
    product_keys = [row["product_id"] for row in rows["inventory"]]
    assert customer_ids == CUSTOMER_IDS
    assert len(product_keys) == len(set(product_keys)) == 300
    assert set(product_keys) == PRODUCT_IDS
    for row in rows["orders"]:
        assert row["customer_id"] == "" or row["customer_id"] in customer_ids, row
        assert row["product_id"] in PRODUCT_IDS, row


def test_primary_and_foreign_keys(nominal_rows):
    assert_keys_and_foreign_keys(nominal_rows)


def order_line_key(row):
    return row["order_id"], row["product_id"]


def assert_exact_replays(key, source, replays, eligible):
    source_by_key = {key(row): row for row in source}
    assert len(source_by_key) == len(source), "Source rows must have unique keys"
    replay_keys = [key(row) for row in replays]
    assert len(set(replay_keys)) == len(replays), "Each source row may be replayed only once"
    eligible_keys = {key(row) for row in eligible}
    for row in replays:
        assert key(row) in eligible_keys, row
        assert row == source_by_key[key(row)], f"Replay differs from its source: {row}"


def test_duplicate_customers_are_exact_replays(nominal_rows):
    rows = nominal_rows["customers"]
    assert len(rows[1000:]) == 10
    assert_exact_replays(lambda row: row["customer_id"], rows[:1000], rows[1000:], rows[15:1000])


def test_duplicate_order_lines_are_exact_replays(nominal_rows):
    rows = nominal_rows["orders"]
    source = rows[:200] + rows[300:]
    replays = rows[200:300]
    assert len(source) == 9900
    assert len(replays) == 100
    assert_exact_replays(order_line_key, source, replays, rows[300:])


def test_distinct_order_groups(nominal_rows):
    orders = defaultdict(list)
    for row in nominal_rows["orders"][:200] + nominal_rows["orders"][300:]:
        orders[row["order_id"]].append(row)
    assert len(orders) == 7200, "Approved contract specifies exactly 7,200 distinct orders"
    counts = Counter(len(lines) for lines in orders.values())
    assert counts[1] == 5040, f"Expected 5,040 single-line orders (70%), got {counts[1]}"
    assert counts[2] == 1620, f"Expected 1,620 two-line orders (22.5%), got {counts[2]}"
    assert counts[3] == 540, f"Expected 540 three-line orders (7.5%), got {counts[3]}"
    multi_count = counts[2] + counts[3]
    assert multi_count == 2160, f"Expected 2,160 multi-item orders (30%), got {multi_count}"
    assert counts[2] / multi_count == 0.75, "Two-line orders must be 75% of multi-item orders"
    assert counts[3] / multi_count == 0.25, "Three-line orders must be 25% of multi-item orders"
    for lines in orders.values():
        assert 1 <= len(lines) <= 3, lines
        assert len({row["product_id"] for row in lines}) == len(lines), lines
        assert len({row["customer_id"] for row in lines if row["customer_id"]}) <= 1, lines


# Each tuple gives the exact data-row positions and the inclusive variable-seed bounds.
ANOMALIES = [
    ("customers", "blank country", lambda row: row["country"] == "", 1, 10, (8, 12)),
    ("customers", "future signup", lambda row: parse_date(row["signup_date"]) > REFERENCE_DATE, 11, 15, (3, 7)),
    ("inventory", "negative stock", lambda row: int(row["current_stock"]) < 0, 1, 3, (2, 4)),
    ("inventory", "blank name", lambda row: row["product_name"] == "", 4, 6, (2, 4)),
    ("orders", "blank customer", lambda row: row["customer_id"] == "", 1, 100, (90, 110)),
    ("orders", "negative quantity", lambda row: int(row["quantity"]) < 0, 101, 150, (40, 60)),
    ("orders", "zero price", lambda row: Decimal(row["unit_price"]) == 0, 151, 200, (40, 60)),
]


@pytest.mark.parametrize(
    "dataset,label,is_anomaly,first,last",
    [pytest.param(*item[:-1], id=item[1]) for item in ANOMALIES],
)
def test_seed_42_anomaly_positions_and_counts(nominal_rows, dataset, label, is_anomaly, first, last):
    rows = [number for number, row in enumerate(nominal_rows[dataset], 1) if is_anomaly(row)]
    assert len(rows) == last - first + 1, label
    assert rows == list(range(first, last + 1)), label


@pytest.mark.parametrize("seed", [7, 101, 2026])
def test_variable_seed_anomaly_tolerances(tmp_path, seed):
    output = run_generator(tmp_path, seed=seed)
    rows = {name: read_rows(output, name) for name in HEADERS}
    assert {name: len(records) for name, records in rows.items()} == ROW_COUNTS
    assert_keys_and_foreign_keys(rows)
    assert_date_ranges(rows["customers"], rows["orders"], REFERENCE_DATE)
    for dataset, label, is_anomaly, _, _, (lower, upper) in ANOMALIES:
        count = sum(is_anomaly(row) for row in rows[dataset])
        assert lower <= count <= upper, f"seed {seed}, {label}: {count} outside [{lower}, {upper}]"
    customer_records = Counter(tuple(row.values()) for row in rows["customers"])
    order_keys = Counter(order_line_key(row) for row in rows["orders"])
    assert 8 <= customer_records.total() - len(customer_records) <= 12
    assert 90 <= order_keys.total() - len(order_keys) <= 110
    assert len(customer_records) == len(CUSTOMER_IDS), "Conflicting customer records share an ID"
    assert len({tuple(row.values()) for row in rows["orders"]}) == len(order_keys), (
        "Conflicting order lines share a composite key"
    )
    assert 9890 <= sum(row["customer_id"] in CUSTOMER_IDS for row in rows["orders"]) <= 9910


@pytest.fixture(scope="module")
def catalog_function():
    spec = importlib.util.spec_from_file_location("phase1_mock_generator", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    catalog = getattr(module, "get_canonical_product_catalog", None)
    assert callable(catalog), "Missing public get_canonical_product_catalog(seed=42) interface"
    return catalog


def test_canonical_catalog_contract(catalog_function):
    catalog = catalog_function(seed=42)
    assert isinstance(catalog, dict)
    assert len(catalog) == 300
    assert set(catalog) == PRODUCT_IDS
    for product, price in catalog.items():
        assert isinstance(price, float) and 5.99 <= price <= 499.99, (product, price)
        assert price == round(price, 2), (product, price)
    expected = catalog.copy()
    catalog_function(seed=7)
    assert catalog_function(seed=42) == expected


def test_canonical_catalog_rejects_size_override(catalog_function):
    with pytest.raises(TypeError):
        catalog_function(seed=42, num_products=301)
    with pytest.raises(TypeError):
        catalog_function(42, 301)


def test_positive_order_prices_match_canonical_catalog(nominal_rows, catalog_function):
    catalog = catalog_function(seed=42)
    for row in nominal_rows["orders"]:
        price = Decimal(row["unit_price"])
        if price > 0:
            assert price == Decimal(str(catalog[row["product_id"]])), row


def csv_bytes(output):
    return {name: (output / f"{name}.csv").read_bytes() for name in HEADERS}


def test_seed_reproducibility(nominal_output, tmp_path_factory):
    repeat = run_generator(tmp_path_factory.mktemp("seed42-repeat"))
    different = run_generator(tmp_path_factory.mktemp("seed7"), seed=7)
    assert csv_bytes(repeat) == csv_bytes(nominal_output)
    assert csv_bytes(different) != csv_bytes(nominal_output), "The seed must affect generated data"


def test_reference_date_reproducibility(tmp_path_factory):
    # The historical anchor is far enough away to expose wall-clock dependencies.
    anchor = date(2023, 3, 1)
    first = run_generator(tmp_path_factory.mktemp("reference-date"), reference_date=anchor)
    repeat = run_generator(tmp_path_factory.mktemp("reference-date-repeat"), reference_date=anchor)
    customers = read_rows(first, "customers")
    orders = read_rows(first, "orders")
    assert len(customers) == ROW_COUNTS["customers"] and len(orders) == ROW_COUNTS["orders"]
    assert_date_ranges(customers, orders, anchor)
    assert sum(parse_date(row["signup_date"]) > anchor for row in customers) == 5
    assert csv_bytes(first) == csv_bytes(repeat)


@pytest.mark.parametrize(
    "anchor",
    [
        "20261119", "2026-W47-4", "0001-01-01", "9999-12-31",
        "2026/11/19", "2026-1-1", "2026-13-01", "2026-02-30",
    ],
)
def test_reference_date_rejected_with_validation_error(tmp_path, anchor):
    output, result = invoke_generator(tmp_path, seed=42, reference_date=anchor)
    assert result.returncode != 0, f"Accepted invalid reference date {anchor}"
    assert "--reference-date" in result.stderr, result.stderr
    assert "Traceback" not in result.stderr, result.stderr
    assert not list(output.glob("*.csv")), "Invalid input must not produce CSVs"


@pytest.mark.parametrize(
    "anchor",
    [date.min + timedelta(days=730), date.max - timedelta(days=30)],
    ids=["earliest-supported-date", "latest-supported-date"],
)
def test_reference_date_accepts_full_offset_windows(tmp_path, anchor):
    output = run_generator(tmp_path, reference_date=anchor)
    customers = read_rows(output, "customers")
    orders = read_rows(output, "orders")
    assert_date_ranges(customers, orders, anchor)
    assert len(customers) == ROW_COUNTS["customers"]
    assert len(orders) == ROW_COUNTS["orders"]
    assert sum(parse_date(row["signup_date"]) > anchor for row in customers) == 5
