"""
Data quality validation for messy_cars.csv before the Spark job runs.
Called via PythonOperator in the DAG — raising any exception fails the task
and stops the pipeline before the Dataproc cluster is even created.
"""

import csv
import io
from google.cloud import storage


# ── Thresholds ────────────────────────────────────────────────────────────────

MIN_ROW_COUNT       = 100          # fail if file has fewer rows than this
MAX_NULL_PCT        = 0.50         # fail if any critical column is >50% null
EXPECTED_COLUMNS    = {            # these must all exist in the file header
    "Car Make", "model_NAME", "YEAR ", "price($)",
    "Mileage(km) ", "colour/Color", " fuel_Type", "No. of Owners"
}
CRITICAL_COLUMNS    = [            # nulls in these columns are counted
    "Car Make", "model_NAME", "YEAR ", "price($)", "Mileage(km) "
]


# ── Helpers ───────────────────────────────────────────────────────────────────

NULL_VALUES = {"", "n/a", "na", "null", "none", "#n/a", "-", "unknown"}

def is_null(value):
    return value is None or str(value).strip().lower() in NULL_VALUES


def read_csv_from_gcs(bucket_name, object_path):
    """Download a GCS file and return it as a list of dicts (one per row)."""
    client  = storage.Client()
    bucket  = client.bucket(bucket_name)
    blob    = bucket.blob(object_path)
    content = blob.download_as_text(encoding="utf-8")
    reader  = csv.DictReader(io.StringIO(content))
    return list(reader), reader.fieldnames


# ── Checks ────────────────────────────────────────────────────────────────────

def check_file_not_empty(rows):
    if len(rows) < MIN_ROW_COUNT:
        raise ValueError(
            f"File has only {len(rows)} rows — expected at least {MIN_ROW_COUNT}. "
            "The file may be empty or truncated."
        )
    print(f"[OK] Row count: {len(rows)}")


def check_expected_columns(fieldnames):
    if fieldnames is None:
        raise ValueError("Could not read column headers — file may be malformed.")
    actual   = set(fieldnames)
    missing  = EXPECTED_COLUMNS - actual
    if missing:
        raise ValueError(
            f"Missing expected columns: {missing}. "
            f"Found: {actual}"
        )
    print(f"[OK] All expected columns present")


def check_null_rates(rows):
    total = len(rows)
    for col in CRITICAL_COLUMNS:
        null_count = sum(1 for row in rows if is_null(row.get(col)))
        null_pct   = null_count / total
        if null_pct > MAX_NULL_PCT:
            raise ValueError(
                f"Column '{col}' is {null_pct:.0%} null ({null_count}/{total} rows). "
                f"Threshold is {MAX_NULL_PCT:.0%}. Data may be corrupt or missing."
            )
        print(f"[OK] '{col}' null rate: {null_pct:.1%}")


def check_no_duplicate_headers(fieldnames):
    """Catch files where the header row was accidentally repeated as a data row."""
    if fieldnames is None:
        return
    duplicates = [col for col in fieldnames if fieldnames.count(col) > 1]
    if duplicates:
        raise ValueError(f"Duplicate column names found: {duplicates}")
    print(f"[OK] No duplicate column headers")


def check_year_has_some_valid_values(rows):
    """At least 50% of YEAR values should contain a 4-digit number."""
    import re
    total = len(rows)
    valid = sum(
        1 for row in rows
        if re.search(r"\d{4}", str(row.get("YEAR ", "")))
    )
    if valid / total < 0.50:
        raise ValueError(
            f"Only {valid}/{total} rows have a parseable 4-digit year. "
            "File may be from the wrong source."
        )
    print(f"[OK] Year parseable in {valid}/{total} rows")


# ── Entry point (called by PythonOperator) ────────────────────────────────────

def validate(**context):
    """
    Main validation function. Airflow calls this via PythonOperator.
    Any unhandled exception fails the task and halts the DAG.
    """
    bucket_name = "etl-migration-1-data"
    object_path = "raw/messy_cars.csv"

    print(f"Starting validation: gs://{bucket_name}/{object_path}")

    rows, fieldnames = read_csv_from_gcs(bucket_name, object_path)

    # Run all checks — they raise ValueError on failure
    check_file_not_empty(rows)
    check_expected_columns(fieldnames)
    check_no_duplicate_headers(fieldnames)
    check_null_rates(rows)
    check_year_has_some_valid_values(rows)

    print(f"All checks passed. {len(rows)} rows ready for processing.")
