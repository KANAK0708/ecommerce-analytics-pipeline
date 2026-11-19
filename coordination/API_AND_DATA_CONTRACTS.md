# API & Data Contracts: Phase 1 Synthetic Data Generation

**Pipeline**: Automated E-Commerce Sales & Inventory Analytics Pipeline  
**Phase**: Phase 1 (Local Setup & Synthetic Data Engine)  
**Status**: Formal Specification (Approved by Project Owner)  
**Task Reference**: `TASK-1.1`  
**Last Updated**: 2026-11-19  

---

## 1. Global Interface & Reproducibility Specifications

### 1.1 Command-Line Interface (CLI) Arguments
The synthetic data generator [data/mock_generator.py](file:///d:/E-commerce/data/mock_generator.py) must support the following CLI interface:

| Argument | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `--customers` | `int` | `1000` | Number of **unique** customer entities to generate. |
| `--products` | `int` | `300` | Number of catalog products to generate. |
| `--orders` | `int` | `10000` | Total order line-item rows to generate (including multi-item lines and duplicates). |
| `--seed` | `int` | `42` | Random generator seed for bit-exact reproducibility. |
| `--reference-date` | `str` | `"2026-11-19"` | ISO-8601 anchor date (`YYYY-MM-DD`) for relative date generation. |
| `--clean` | `flag` | `False` | **Strict File Deletion Safety**: Deletes *only* `customers.csv`, `inventory.csv`, and `orders.csv` from the output directory. Never deletes other files; never uses wildcards. If omitted, target files are safely overwritten in place. |
| `--output-dir` | `str` | `"data/raw"` | Destination directory for generated raw CSV files. |

### 1.2 Deterministic Date & Seed Reproducibility
* **Seed Contract (`seed: int = 42`)**: Generating with the same seed must produce bit-exact identical records across runs and operating systems.
* **Reference Date Contract (`reference_date: str`)**: All date fields are calculated relative to `reference_date`, decoupling tests from the machine's wall-clock calendar:
  * Normal Customer Signups: Sampled within `[reference_date - 730 days, reference_date]`.
  * Future Customer Signup Anomalies: Sampled within `[reference_date + 1 day, reference_date + 30 days]`.
  * Order Dates: Sampled within `[reference_date - 180 days, reference_date]`.

### 1.3 Row Indexing & CSV Output Format
* **Row Numbering**: In all anomaly tables below, **row numbers refer strictly to data rows (1-indexed), excluding the CSV header**. (Data Row 1 = physical CSV line 2).
* **CSV Format**: UTF-8 encoded, comma-delimited (`,`), standard CRLF/LF line endings, header on line 1, double-quote escaped strings if required.

---

## 2. Canonical Product Catalog Interface & Pricing Contract

To strictly preserve the approved TRD layout (where `inventory.csv` does not contain `unit_price`), product pricing is decoupled into a canonical catalog interface that is independently testable without altering `inventory.csv`.

### 2.1 Interface Definition
```python
def get_canonical_product_catalog(seed: int = 42) -> dict[str, float]:
    """
    Returns a deterministic lookup dictionary mapping product_id to base unit price.
    
    Contract Rules:
    1. Size: Exactly 300 entries (PROD_1001 through PROD_1300).
    2. Price Range: Float values strictly bounded between $5.99 and $499.99.
    3. Precision: Rounded to exactly 2 decimal places.
    4. Determinism: Bit-exact identical dictionary across repeated calls with identical seed.
    """
```

### 2.2 Independent Testability Contract
`Codex` will independently test the pricing interface in `tests/test_mock_generator.py`:
1. Assert catalog size is exactly 300 entries.
2. Assert keys match regex `^PROD_1[0-2][0-9]{2}|PROD_1300$`.
3. Assert all prices fall within `5.99 <= price <= 499.99`.
4. Assert all prices have at most 2 decimal places.
5. Assert price consistency: For every row in `orders.csv` where `unit_price > 0.0`, `orders.unit_price == catalog[orders.product_id]`.

---

## 3. Dataset Contracts (Bronze Raw Layer)

### 3.1 Customers Dataset (`data/raw/customers.csv`)

* **Volume**: Exactly **1,010 raw data rows** representing **1,000 unique customer identities**.
* **Primary Key**: `customer_id` (unique across 1,000 distinct entities; exactly 10 IDs repeated as duplicate replay rows).
* **File Header**: `customer_id,signup_date,country`

#### Field Specifications
| Field Name | Type | Constraints | Nullable | Allowed Values & Business Rules |
| :--- | :--- | :--- | :--- | :--- |
| `customer_id` | `STRING` | PK (`^CUST_[0-9]{5}$`) | No | `CUST_10001` to `CUST_11000`. Mandatory on all 1,010 data rows. Exactly 1,000 unique IDs; exactly 10 IDs appear twice. |
| `signup_date` | `STRING` | ISO-8601 `YYYY-MM-DD` | No | Valid historical range `[ref_date - 730d, ref_date]` for 1,005 rows. Exactly 5 rows contain future dates. |
| `country` | `STRING` | ISO-2 code | Yes | Valid codes: `US`, `CA`, `GB`, `DE`, `FR`, `IN`, `JP`, `AU` for 1,000 rows. Exactly 10 rows contain intentional blank string `""`. |

#### Mutually Exclusive Row Positions (Excluding Header)
* **Data Rows 1 to 10** (10 rows): Injected blank country (`country = ""`, valid historical signup date).
* **Data Rows 11 to 15** (5 rows): Injected future signup date (`signup_date` in `[ref_date + 1d, ref_date + 30d]`, valid country).
* **Data Rows 16 to 1,000** (985 rows): Clean, valid unique customer records.
* **Data Rows 1,001 to 1,010** (10 rows): Exact row replays of 10 records chosen from Data Rows 16–1,000.

---

### 3.2 Inventory Dataset (`data/raw/inventory.csv`)

* **Volume**: Exactly **300 data rows** representing **300 catalog products**.
* **Primary Key**: `product_id` (strictly unique, non-null across all 300 data rows).
* **Preservation**: Strict TRD compliance; does **not** include `unit_price`.
* **File Header**: `product_id,product_name,category,current_stock,reorder_threshold`

#### Field Specifications
| Field Name | Type | Constraints | Nullable | Allowed Values & Business Rules |
| :--- | :--- | :--- | :--- | :--- |
| `product_id` | `STRING` | PK (`^PROD_[0-9]{4}$`) | No | `PROD_1001` through `PROD_1300`. Strictly unique and non-null across all 300 data rows. |
| `product_name` | `STRING` | Text | Yes | Realistic product name (e.g. `Laptop Pro`). Exactly 297 valid non-empty names; exactly 3 rows contain `""`. |
| `category` | `STRING` | Enum | No | Strictly one of: `Electronics`, `Home & Kitchen`, `Apparel`, `Health & Personal Care`, `Books`, `Sports & Outdoors`. |
| `current_stock` | `INT` | Signed Integer | No | Valid stock $\ge 0$ for 297 products (healthy, at-threshold, or 0/stockout). Exactly 3 rows contain negative stock. |
| `reorder_threshold` | `INT` | Discrete Integer | No | Strictly one of: $\{10, 20, 25, 50, 75, 100\}$. Strictly positive and non-null. |

#### Mutually Exclusive Row Positions (Excluding Header)
* **Data Rows 1 to 3** (3 rows): Injected negative stock (`current_stock` between $-1$ and $-10$, valid product name).
* **Data Rows 4 to 6** (3 rows): Injected blank product name (`product_name = ""`, valid stock $\ge 0$).
* **Data Rows 7 to 300** (294 rows): Clean, valid unique inventory records.

---

### 3.3 Orders Dataset (`data/raw/orders.csv`)

* **Volume**: Exactly **10,000 order line-item data rows**.
* **Natural / Composite Key**: `(order_id, product_id)`.
* **Line Item Composition**:
  * **9,900 Unique Source Lines**: Unique composite pairs of `(order_id, product_id)`.
  * **100 Duplicate Line Replays**: Exact row replays of 100 unique lines chosen from Data Rows 301–10,000.
  * **Legitimate Multi-Item Orders**: ~30% of distinct orders contain 2 to 3 lines sharing the same `order_id` and `customer_id` with **different** `product_id` values.
* **File Header**: `order_id,customer_id,order_date,product_id,quantity,unit_price`

#### Field Specifications
| Field Name | Type | Constraints | Nullable | Allowed Values & Business Rules |
| :--- | :--- | :--- | :--- | :--- |
| `order_id` | `STRING` | `^ORD_[0-9]{6}$` | No | Grouping key for orders (~7,000–7,500 distinct order IDs). |
| `customer_id` | `STRING` | FK $\rightarrow$ `customers` | Yes | Valid foreign key for 9,900 rows. Exactly 100 rows contain intentional `""`. |
| `order_date` | `STRING` | ISO-8601 `YYYY-MM-DD` | No | Bounded within `[ref_date - 180 days, ref_date]`. Strictly non-null. |
| `product_id` | `STRING` | FK $\rightarrow$ `inventory` | No | 100% referential integrity (all 10,000 rows match valid `product_id`s in `inventory.csv`). |
| `quantity` | `INT` | Signed Integer | No | Valid rule: strictly positive integer $\ge 1$ (discrete distribution: 1, 2, 3, 4, 5, 10) for 9,950 rows. Exactly 50 rows contain negative quantities. |
| `unit_price` | `FLOAT` | 2 decimals | No | Valid rule: strictly positive matching canonical catalog price ($5.99 to $499.99) for 9,950 rows. Exactly 50 rows contain `unit_price = 0.0`. |

#### Mutually Exclusive Row Positions (Excluding Header)
* **Data Rows 1 to 100** (100 rows): Injected null customer ID (`customer_id = ""`, valid positive quantity, valid catalog unit price).
* **Data Rows 101 to 150** (50 rows): Injected negative quantity (`quantity` between $-1$ and $-5$, valid customer ID, valid catalog unit price).
* **Data Rows 151 to 200** (50 rows): Injected zero unit price (`unit_price = 0.0`, valid customer ID, valid positive quantity).
* **Data Rows 201 to 300** (100 rows): Injected duplicate lines (exact row replays of 100 valid lines from Data Rows 301–10,000).
* **Data Rows 301 to 10,000** (9,700 rows): Clean, valid order line items with unique `(order_id, product_id)`.

---

## 4. Standardized Anomaly & Test Assertion Matrix

### 4.1 Assertion Reference Matrix
| Dataset | Metric / Anomaly | Nominal Count (`seed=42`) | Deterministic Test Assertion (`seed=42`) | Variable-Seed Tolerance |
| :--- | :--- | :--- | :--- | :--- |
| **Customers** | Total Raw Data Rows | 1,010 | `assert len(df) == 1010` | Exact: 1,010 |
| **Customers** | Unique `customer_id`s | 1,000 | `assert df['customer_id'].nunique() == 1000` | Exact: 1,000 |
| **Customers** | Duplicate Customer Rows | 10 | `assert df.duplicated().sum() == 10` | $[8, 12]$ |
| **Customers** | Blank Country (`country == ""`) | 10 | `assert (df['country'] == "").sum() == 10` | $[8, 12]$ |
| **Customers** | Future Signup Date (`> ref_date`) | 5 | `assert (df['signup_date'] > ref_date).sum() == 5` | $[3, 7]$ |
| **Inventory** | Total Products / Unique SKUs | 300 | `assert len(df) == 300 and df['product_id'].nunique() == 300` | Exact: 300 |
| **Inventory** | Negative Stock (`current_stock < 0`) | 3 | `assert (df['current_stock'] < 0).sum() == 3` | $[2, 4]$ |
| **Inventory** | Blank Product Name (`product_name == ""`) | 3 | `assert (df['product_name'] == "").sum() == 3` | $[2, 4]$ |
| **Orders** | Total Order Line Rows | 10,000 | `assert len(df) == 10000` | Exact: 10,000 |
| **Orders** | Duplicate Line Replays `(order_id, product_id)` | 100 | `assert df.duplicated(subset=['order_id', 'product_id']).sum() == 100` | $[90, 110]$ |
| **Orders** | Null Customer ID (`customer_id == ""`) | 100 | `assert (df['customer_id'] == "").sum() == 100` | $[90, 110]$ |
| **Orders** | Negative Quantity (`quantity <= -1`) | 50 | `assert (df['quantity'] <= -1).sum() == 50` | $[40, 60]$ |
| **Orders** | Zero Unit Price (`unit_price == 0.0`) | 50 | `assert (df['unit_price'] == 0.0).sum() == 50` | $[40, 60]$ |
| **Orders** | Foreign Key Validity: `customer_id` | 9,900 | `assert df['customer_id'].isin(valid_cust_ids).sum() == 9900` | $[9890, 9910]$ |
| **Orders** | Foreign Key Validity: `product_id` | 10,000 | `assert df['product_id'].isin(valid_prod_ids).sum() == 10000` | Exact: 100% |

### 4.2 Quality & Testing Expectations
1. **Deterministic Test Policy**: In test executions using `seed=42` and `reference_date="2026-11-19"`, all tests must assert **exact count equality** (`== nominal_count`).
2. **Variable-Seed Test Policy**: In parameterized or variable-seed test runs, tests assert that counts fall strictly within the documented tolerance intervals.
3. **Intentional Blank Fields vs. Missing Data**:
   * Columns with **no anomalies** must have exactly 0 blank or null fields.
   * Columns with **intentional anomalies** (`country`, `product_name`, `customer_id`) must have *exactly* the nominal count of empty strings `""`, and zero unparsed `NaN`/`None` values.
4. **Independent Pricing Verification**:
   * Tests will verify that every order record with `unit_price > 0.0` exactly matches the value returned by `get_canonical_product_catalog(seed=42)` for that product ID.
