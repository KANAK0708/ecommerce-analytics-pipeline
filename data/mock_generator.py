"""
Mock Data Generator for the Automated E-Commerce Sales & Inventory Analytics Pipeline.

TASK-1.2. Implements coordination/API_AND_DATA_CONTRACTS.md (Phase 1, Bronze raw layer).

It writes three CSV files with deliberate, *exactly placed* data-quality problems so the
later Silver-layer cleaning code has known answers to test against:

1. customers.csv  (customer_id, signup_date, country)
2. inventory.csv  (product_id, product_name, category, current_stock, reorder_threshold)
3. orders.csv     (order_id, customer_id, order_date, product_id, quantity, unit_price)

Key ideas
---------
* Reproducibility: every random choice comes from one ``random.Random(seed)`` object
  that is passed around. Nothing reads the wall clock; every date is an offset from
  ``--reference-date``. Same seed + same reference date = byte-identical files.
* Anomalies are not sprinkled by probability. They sit in fixed, non-overlapping data
  rows (row 1 = first line after the header), exactly as the contract's anomaly matrix
  lists them, so tests can assert exact counts and positions.
* Prices live in ``get_canonical_product_catalog()``, not in inventory.csv, because the
  approved TRD inventory schema has no unit_price column.

Run from the repository root:
    python data/mock_generator.py
    python data/mock_generator.py --seed 7 --reference-date 2026-11-19 --clean
"""

import argparse
import csv
import random
import re
import sys
from datetime import date, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants taken from the contract
# ---------------------------------------------------------------------------

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "raw"  # = data/raw from any cwd
DEFAULT_REFERENCE_DATE = "2026-11-19"

# The only files --clean is ever allowed to delete. Listed by exact name, no wildcards.
TARGET_FILES = ("customers.csv", "inventory.csv", "orders.csv")

CUSTOMER_HEADER = ["customer_id", "signup_date", "country"]
INVENTORY_HEADER = ["product_id", "product_name", "category", "current_stock", "reorder_threshold"]
ORDER_HEADER = ["order_id", "customer_id", "order_date", "product_id", "quantity", "unit_price"]

COUNTRIES = ["US", "CA", "GB", "DE", "FR", "IN", "JP", "AU"]
CATEGORIES = [
    "Electronics", "Home & Kitchen", "Apparel",
    "Health & Personal Care", "Books", "Sports & Outdoors",
]
REORDER_THRESHOLDS = [10, 20, 25, 50, 75, 100]
QUANTITIES = [1, 2, 3, 4, 5, 10]
QUANTITY_WEIGHTS = [50, 25, 12, 6, 5, 2]

MIN_PRICE, MAX_PRICE = 5.99, 499.99
CATALOG_SIZE = 300  # contract 2.1: exactly 300 entries, PROD_1001..PROD_1300
FIRST_CUSTOMER_NUMBER = 10001   # CUST_10001 ...
FIRST_PRODUCT_NUMBER = 1001     # PROD_1001 ...
FIRST_ORDER_NUMBER = 100001     # ORD_100001 ...

HISTORY_DAYS_SIGNUP = 730   # normal signups: [ref - 730d, ref]
FUTURE_DAYS_SIGNUP = 30     # anomalous signups: [ref + 1d, ref + 30d]
HISTORY_DAYS_ORDER = 180    # orders: [ref - 180d, ref]

# --reference-date must be written exactly like 2026-11-19. date.fromisoformat() alone
# also accepts 20261119 and week dates such as 2026-W47-4 (Python 3.11+), so we check
# the shape first. [0-9] is used instead of \d because \d also matches non-ASCII digits.
REFERENCE_DATE_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")

# Anomaly counts (contract section 4.1). They are fixed, so they also define the
# minimum dataset sizes accepted on the command line.
CUST_BLANK_COUNTRY = 10      # data rows 1-10
CUST_FUTURE_SIGNUP = 5       # data rows 11-15
CUST_DUPLICATES = 10         # appended replays of rows 16..N
INV_NEGATIVE_STOCK = 3       # data rows 1-3
INV_BLANK_NAME = 3           # data rows 4-6
ORD_NULL_CUSTOMER = 100      # data rows 1-100
ORD_NEGATIVE_QTY = 50        # data rows 101-150
ORD_ZERO_PRICE = 50          # data rows 151-200
ORD_DUPLICATES = 100         # data rows 201-300, replays of rows 301..N

# Multi-item order shape (see _plan_order_sizes for the maths).
MULTI_ITEM_SHARE = 0.30      # ~30% of distinct orders have 2-3 lines
THREE_LINE_SHARE = 0.25      # of those multi-item orders: 75% two-line, 25% three-line

# Word lists for product names. We use fixed lists instead of Faker so that the output is
# byte-identical on every machine: Faker's word lists change between library versions.
ADJECTIVES = [
    "Classic", "Smart", "Ultra", "Compact", "Premium", "Eco", "Pro", "Essential",
    "Deluxe", "Active", "Urban", "Vintage", "Wireless", "Portable", "Everyday", "Nordic",
]
NOUNS_BY_CATEGORY = {
    "Electronics": ["Headphones", "Speaker", "Charger", "Keyboard", "Monitor", "Tablet", "Camera", "Router"],
    "Home & Kitchen": ["Blender", "Kettle", "Cookware Set", "Knife Block", "Toaster", "Lamp", "Rug", "Coffee Maker"],
    "Apparel": ["Jacket", "Hoodie", "Sneakers", "T-Shirt", "Jeans", "Scarf", "Backpack", "Cap"],
    "Health & Personal Care": ["Toothbrush", "Hair Dryer", "Shaver", "Massager", "Scale", "Vitamins", "Lotion", "Diffuser"],
    "Books": ["Cookbook", "Novel", "Atlas", "Workbook", "Biography", "Journal", "Guidebook", "Anthology"],
    "Sports & Outdoors": ["Yoga Mat", "Tent", "Dumbbells", "Water Bottle", "Bike Helmet", "Sleeping Bag", "Jump Rope", "Trekking Poles"],
}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def customer_ids(num_customers):
    """CUST_10001 .. CUST_(10000 + n), in order."""
    return [f"CUST_{FIRST_CUSTOMER_NUMBER + i}" for i in range(num_customers)]


def product_ids(num_products):
    """PROD_1001 .. PROD_(1000 + n), in order."""
    return [f"PROD_{FIRST_PRODUCT_NUMBER + i}" for i in range(num_products)]


def random_date(rng, start, end):
    """Uniform random date in the inclusive range [start, end]."""
    return start + timedelta(days=rng.randint(0, (end - start).days))


def format_price(price):
    """Prices are written with exactly two decimals, e.g. 12.50 and 0.00."""
    return f"{price:.2f}"


def parse_reference_date(text):
    """
    Turn the --reference-date text into a date, or raise ValueError with a clear message.

    Two checks:
      1. Format: only YYYY-MM-DD (e.g. 2026-11-19). 20261119, week dates, and malformed strings are refused.
      2. Range: the generator needs dates 730 and 180 days before the reference date and
         30 days after it. Python dates run from 0001-01-01 to 9999-12-31, so a
         reference date outside [0003-01-01, 9999-12-01] is rejected before any files are written.
    """
    if isinstance(text, date):
        reference_date = text
    elif isinstance(text, str) and REFERENCE_DATE_PATTERN.fullmatch(text):
        try:
            reference_date = date.fromisoformat(text)
        except ValueError:
            raise ValueError(f"is not a real calendar date, got {text!r}") from None
    else:
        raise ValueError(f"must be in YYYY-MM-DD format (for example 2026-11-19), got {text!r}")

    earliest = date.min + timedelta(days=HISTORY_DAYS_SIGNUP)
    latest = date.max - timedelta(days=FUTURE_DAYS_SIGNUP)
    if not (earliest <= reference_date <= latest):
        raise ValueError(
            f"must be between {earliest.isoformat()} and {latest.isoformat()} so that "
            f"{HISTORY_DAYS_SIGNUP} days before and {FUTURE_DAYS_SIGNUP} days after are valid dates, got {text!r}"
        )
    return reference_date


# ---------------------------------------------------------------------------
# Canonical product catalog (contract section 2)
# ---------------------------------------------------------------------------

def get_canonical_product_catalog(seed: int = 42) -> dict[str, float]:
    """
    Return the deterministic price list: {product_id: unit_price}.

    Why it exists: inventory.csv must not carry prices (TRD schema), but orders.csv needs
    one consistent price per product, and tests need to check that. This function is that
    single source of truth.

    It uses its own Random(seed), separate from the rest of the generator, so calling it
    never disturbs other random draws and always returns the same prices for a seed.
    It always returns exactly 300 entries PROD_1001..PROD_1300 (CATALOG_SIZE); there is no
    size argument, so the catalog cannot be made bigger or smaller. Each price is a float
    in [5.99, 499.99] rounded to 2 decimals.
    """
    rng = random.Random(seed)
    catalog = {}
    for pid in product_ids(CATALOG_SIZE):
        price = round(rng.uniform(MIN_PRICE, MAX_PRICE), 2)
        catalog[pid] = min(max(price, MIN_PRICE), MAX_PRICE)  # guard against rounding drift
    if len(catalog) != CATALOG_SIZE:  # safety net if someone edits the code above
        raise RuntimeError(f"catalog must have exactly {CATALOG_SIZE} entries, got {len(catalog)}")
    return catalog


# ---------------------------------------------------------------------------
# Customers (contract section 3.1)
# ---------------------------------------------------------------------------

def generate_customers(rng, num_customers, reference_date):
    """
    Build customer rows (lists in CUSTOMER_HEADER order).

    Layout of data rows:
      1-10        blank country, valid past signup date
      11-15       future signup date, valid country
      16-N        clean customers
      N+1..N+10   exact replays of 10 distinct rows picked from 16-N
    """
    ids = customer_ids(num_customers)
    rng.shuffle(ids)  # so the anomalies are not always the lowest IDs
    history_start = reference_date - timedelta(days=HISTORY_DAYS_SIGNUP)

    rows = []
    for position, cid in enumerate(ids, start=1):
        signup = random_date(rng, history_start, reference_date)
        country = rng.choice(COUNTRIES)
        if position <= CUST_BLANK_COUNTRY:
            country = ""
        elif position <= CUST_BLANK_COUNTRY + CUST_FUTURE_SIGNUP:
            signup = random_date(rng, reference_date + timedelta(days=1),
                                 reference_date + timedelta(days=FUTURE_DAYS_SIGNUP))
        rows.append([cid, signup.isoformat(), country])

    clean_rows = rows[CUST_BLANK_COUNTRY + CUST_FUTURE_SIGNUP:]
    replays = rng.sample(clean_rows, CUST_DUPLICATES)  # sample = no row picked twice
    rows.extend(list(row) for row in replays)
    return rows


# ---------------------------------------------------------------------------
# Inventory (contract section 3.2)
# ---------------------------------------------------------------------------

def _product_name(rng, category, used_names):
    """Make a readable, unique name such as 'Smart Speaker'."""
    base = f"{rng.choice(ADJECTIVES)} {rng.choice(NOUNS_BY_CATEGORY[category])}"
    name, series = base, 2
    while name in used_names:          # add a series number if the name is taken
        name, series = f"{base} {series}", series + 1
    used_names.add(name)
    return name


def generate_inventory(rng, num_products):
    """
    Build inventory rows (lists in INVENTORY_HEADER order). No unit_price column.

    Layout of data rows:
      1-3     negative stock (-1 to -10), valid name
      4-6     blank product name, stock >= 0
      7-N     clean products
    Clean stock is a mix of stockouts (0), low stock (at or below the reorder threshold)
    and healthy stock, so the low-stock alert logic has something to find later.
    """
    rows, used_names = [], set()
    for position, pid in enumerate(product_ids(num_products), start=1):
        category = rng.choice(CATEGORIES)
        name = _product_name(rng, category, used_names)
        threshold = rng.choice(REORDER_THRESHOLDS)
        band = rng.choices(["stockout", "low", "healthy"], weights=[10, 25, 65])[0]
        if band == "stockout":
            stock = 0
        elif band == "low":
            stock = rng.randint(1, threshold)
        else:
            stock = rng.randint(threshold + 1, 500)

        if position <= INV_NEGATIVE_STOCK:
            stock = -rng.randint(1, 10)
        elif position <= INV_NEGATIVE_STOCK + INV_BLANK_NAME:
            name = ""
        rows.append([pid, name, category, stock, threshold])
    return rows


# ---------------------------------------------------------------------------
# Orders (contract section 3.3)
# ---------------------------------------------------------------------------

def _plan_order_sizes(unique_lines):
    """
    Decide how many 1-, 2- and 3-line orders to make so that they add up to exactly
    `unique_lines` lines, ~30% of orders are multi-item and the order count lands in the
    contract's 7,000-7,500 range (for 9,900 unique lines).

    Maths: with 30% multi-item orders, of which 75% have 2 lines and 25% have 3 lines,
    an average order has 0.70*1 + 0.30*(0.75*2 + 0.25*3) = 1.375 lines.
    So orders = 9,900 / 1.375 = 7,200   -> 5,040 single, 1,620 two-line, 540 three-line.
    (A 50/50 split gives 1.45 lines per order and only ~6,828 orders: too few.)
    """
    avg_lines = (1 - MULTI_ITEM_SHARE) + MULTI_ITEM_SHARE * (
        (1 - THREE_LINE_SHARE) * 2 + THREE_LINE_SHARE * 3)
    total_orders = round(unique_lines / avg_lines)
    multi = round(total_orders * MULTI_ITEM_SHARE)
    three = round(multi * THREE_LINE_SHARE)
    two = multi - three
    single = unique_lines - 2 * two - 3 * three
    if single < 0:
        raise ValueError("Not enough order lines to build the multi-item orders.")
    return [1] * single + [2] * two + [3] * three


def _take_groups_filling(groups, target):
    """
    Remove and return groups from the front of `groups` whose sizes add up to exactly
    `target` lines. Used so that orders never straddle an anomaly-block boundary.
    If the next group would overshoot, a later group of exactly the remaining size is
    used instead (single-line orders always exist, so this always succeeds).
    """
    taken, filled, i = [], 0, 0
    while filled < target:
        remaining = target - filled
        if len(groups[i]) > remaining:
            i = next(j for j in range(i, len(groups)) if len(groups[j]) == remaining)
        group = groups.pop(i)
        taken.append(group)
        filled += len(group)
        i = 0
    return taken


def generate_orders(rng, num_lines, cust_ids, catalog, reference_date):
    """
    Build order line rows (lists in ORDER_HEADER order).

    Steps:
      1. Plan order sizes (1-3 lines each) for the 9,900 unique lines.
      2. Give each order an order_id, a customer, a date and distinct products.
      3. Lay lines out so orders sit together, and so whole orders fill rows 1-100 and
         rows 101-200 (nobody's order is split across a block edge).
      4. Inject anomalies by row position:
           1-100    customer_id = ""   (the whole order has no customer)
           101-150  quantity -1 to -5
           151-200  unit_price = 0.00
           201-300  exact replays of 100 distinct lines from rows 301-N
           301-N    clean lines
    """
    unique_lines = num_lines - ORD_DUPLICATES
    pids = list(catalog)
    history_start = reference_date - timedelta(days=HISTORY_DAYS_ORDER)

    sizes = _plan_order_sizes(unique_lines)
    rng.shuffle(sizes)

    groups = []
    for number, size in enumerate(sizes):
        order_id = f"ORD_{FIRST_ORDER_NUMBER + number}"
        customer = rng.choice(cust_ids)
        order_date = random_date(rng, history_start, reference_date).isoformat()
        products = rng.sample(pids, size)  # sample = different products within an order
        group = []
        for pid in products:
            quantity = rng.choices(QUANTITIES, weights=QUANTITY_WEIGHTS)[0]
            group.append([order_id, customer, order_date, pid, quantity, catalog[pid]])
        groups.append(group)

    # Whole orders for the null-customer block, then for the qty/price block, then the rest.
    anomaly_block_lines = ORD_NEGATIVE_QTY + ORD_ZERO_PRICE
    null_customer_groups = _take_groups_filling(groups, ORD_NULL_CUSTOMER)
    line_anomaly_groups = _take_groups_filling(groups, anomaly_block_lines)

    head = [line for g in null_customer_groups + line_anomaly_groups for line in g]
    clean = [line for g in groups for line in g]

    for position, line in enumerate(head, start=1):
        if position <= ORD_NULL_CUSTOMER:
            line[1] = ""
        elif position <= ORD_NULL_CUSTOMER + ORD_NEGATIVE_QTY:
            line[4] = -rng.randint(1, 5)
        else:
            line[5] = 0.0

    replays = [list(line) for line in rng.sample(clean, ORD_DUPLICATES)]
    rows = head + replays + clean
    for line in rows:
        line[5] = format_price(line[5])
    return rows


# ---------------------------------------------------------------------------
# File handling
# ---------------------------------------------------------------------------

def clean_output(output_dir):
    """
    Delete ONLY customers.csv, inventory.csv and orders.csv in output_dir.
    Each file is named explicitly (no wildcards, no folder deletion). Anything else in the
    folder is left alone. Returns the list of files that were deleted.
    """
    deleted = []
    for name in TARGET_FILES:
        path = output_dir / name
        if path.is_file():
            path.unlink()
            deleted.append(path)
    return deleted


def save_csv(rows, header, path):
    """Write header + rows as UTF-8 CSV with LF line endings (same bytes on every OS)."""
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)
    print(f"Generated {len(rows):,} rows -> {path}")


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Generate Phase 1 mock e-commerce CSV files.")
    parser.add_argument("--customers", type=int, default=1000, help="unique customers (default 1000)")
    parser.add_argument("--products", type=int, default=300, help="catalog products (default 300)")
    parser.add_argument("--orders", type=int, default=10000,
                        help="total order line rows incl. duplicates (default 10000)")
    parser.add_argument("--seed", type=int, default=42, help="random seed (default 42)")
    parser.add_argument("--reference-date", default=DEFAULT_REFERENCE_DATE,
                        help="anchor date YYYY-MM-DD (default 2026-11-19)")
    parser.add_argument("--clean", action="store_true",
                        help="delete only customers.csv, inventory.csv, orders.csv first")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
                        help="where to write the CSVs (default data/raw)")
    args = parser.parse_args(argv)

    # Input validation: the fixed anomaly blocks need a minimum size, and IDs have a
    # fixed number of digits in the contract (CUST_5 digits, PROD_4 digits, ORD_6 digits).
    min_customers = CUST_BLANK_COUNTRY + CUST_FUTURE_SIGNUP + CUST_DUPLICATES
    if not min_customers <= args.customers <= 89999:
        parser.error(f"--customers must be between {min_customers} and 89999")
    # Products are drawn from the fixed 300-entry catalog, so more than 300 is refused.
    min_products = INV_NEGATIVE_STOCK + INV_BLANK_NAME + 1
    if not min_products <= args.products <= CATALOG_SIZE:
        parser.error(f"--products must be between {min_products} and {CATALOG_SIZE} "
                     f"(the canonical catalog has exactly {CATALOG_SIZE} products)")
    min_orders = ORD_NULL_CUSTOMER + ORD_NEGATIVE_QTY + ORD_ZERO_PRICE + 2 * ORD_DUPLICATES
    if not min_orders <= args.orders <= 500000:
        parser.error(f"--orders must be between {min_orders} and 500000")
    try:
        args.reference_date = parse_reference_date(args.reference_date)
    except ValueError as error:
        parser.error(f"--reference-date {error}")  # exits here: nothing deleted or written
    return args


def main(argv=None):
    args = parse_args(argv)
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.clean:
        deleted = clean_output(output_dir)
        print(f"--clean: deleted {len(deleted)} file(s): {', '.join(p.name for p in deleted) or 'none'}")

    # One shared random generator for the datasets; the catalog has its own (see above).
    rng = random.Random(args.seed)
    # The full catalog always has 300 entries; a smaller --products run uses its first N.
    full_catalog = get_canonical_product_catalog(seed=args.seed)
    catalog = {pid: full_catalog[pid] for pid in product_ids(args.products)}

    customers = generate_customers(rng, args.customers, args.reference_date)
    inventory = generate_inventory(rng, args.products)
    orders = generate_orders(rng, args.orders, customer_ids(args.customers), catalog,
                             args.reference_date)

    save_csv(customers, CUSTOMER_HEADER, output_dir / "customers.csv")
    save_csv(inventory, INVENTORY_HEADER, output_dir / "inventory.csv")
    save_csv(orders, ORDER_HEADER, output_dir / "orders.csv")
    print("Mock data generation complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
