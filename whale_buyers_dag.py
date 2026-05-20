"""
Whale Buyers ETL Pipeline
=========================
Airflow DAG that:
  1. EXTRACT  – reads raw_sales_data.csv
  2. VALIDATE – checks schema & data quality
  3. TRANSFORM – filters customers with >$500 spend in the past 30 days
                 and aggregates totals per customer
  4. LOAD      – writes whale_buyers.csv to the output directory

Schedule: runs daily at midnight UTC
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Paths (override via Airflow Variables or env vars in production) ──────────
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH   = os.path.join(BASE_DIR, "data",   "raw_sales_data.csv")
OUT_PATH   = os.path.join(BASE_DIR, "output", "whale_buyers.csv")
STAGING    = os.path.join(BASE_DIR, "data",   "staging_clean.csv")

WHALE_THRESHOLD = 500.00   # dollars
LOOKBACK_DAYS   = 30

log = logging.getLogger(__name__)


# ── Task functions ─────────────────────────────────────────────────────────────

def extract(**context) -> None:
    """Read raw CSV and push record count to XCom for observability."""
    log.info("EXTRACT – reading %s", RAW_PATH)
    df = pd.read_csv(RAW_PATH)
    log.info("Loaded %d rows, columns: %s", len(df), df.columns.tolist())
    context["ti"].xcom_push(key="raw_row_count", value=len(df))


def validate(**context) -> None:
    """
    Basic data-quality checks:
      - Required columns present
      - No null order_id / customer_id
      - total_amount is numeric and non-negative
      - purchase_date is parseable
    Raises ValueError and stops the DAG on failure.
    """
    log.info("VALIDATE – checking data quality")
    df = pd.read_csv(RAW_PATH)

    required = {"order_id", "customer_id", "purchase_date", "num_items", "total_amount"}
    missing  = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    null_keys = df[["order_id", "customer_id"]].isnull().sum()
    if null_keys.any():
        raise ValueError(f"Null key fields detected:\n{null_keys[null_keys > 0]}")

    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
    bad_amounts = df["total_amount"].isnull().sum()
    if bad_amounts:
        log.warning("%d rows have non-numeric total_amount – will be dropped", bad_amounts)

    df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce")
    bad_dates = df["purchase_date"].isnull().sum()
    if bad_dates:
        log.warning("%d rows have unparseable purchase_date – will be dropped", bad_dates)

    # Drop invalid rows and persist clean staging file
    df = df.dropna(subset=["total_amount", "purchase_date"])
    df = df[df["total_amount"] >= 0]
    df.to_csv(STAGING, index=False)

    log.info("Validation passed – %d clean rows written to staging", len(df))
    context["ti"].xcom_push(key="clean_row_count", value=len(df))


def transform(**context) -> None:
    """
    Transformation logic:
      1. Filter to orders placed within the past LOOKBACK_DAYS days
         (relative to logical execution date so backfills work correctly).
      2. Filter orders where total_amount > WHALE_THRESHOLD.
      3. Aggregate per customer:
           - total_spend        : sum of all qualifying order amounts
           - num_orders         : count of qualifying orders
           - avg_order_value    : mean order value
           - total_items_bought : sum of num_items
           - first_order_date   : earliest order date in window
           - last_order_date    : most recent order date in window
      4. Sort descending by total_spend (biggest whales first).
    """
    execution_date = context["logical_date"]   # timezone-aware Pendulum datetime
    cutoff = execution_date - timedelta(days=LOOKBACK_DAYS)

    log.info("TRANSFORM – window: %s → %s, threshold: $%.2f",
             cutoff.date(), execution_date.date(), WHALE_THRESHOLD)

    df = pd.read_csv(STAGING, parse_dates=["purchase_date"])

    # ── Step 1: date window ────────────────────────────────────────────────────
    # Make cutoff tz-aware to match the parsed column if needed
    if df["purchase_date"].dt.tz is not None:
        cutoff = cutoff
    else:
        cutoff = cutoff.replace(tzinfo=None)
        execution_date = execution_date.replace(tzinfo=None)

    in_window = df[
        (df["purchase_date"] >= cutoff) &
        (df["purchase_date"] <= execution_date)
    ]
    log.info("Rows in %d-day window: %d", LOOKBACK_DAYS, len(in_window))

    # ── Step 2: whale filter ───────────────────────────────────────────────────
    whales_raw = in_window[in_window["total_amount"] > WHALE_THRESHOLD]
    log.info("Whale orders (>$%.2f): %d", WHALE_THRESHOLD, len(whales_raw))

    if whales_raw.empty:
        log.warning("No whale orders found – writing empty output file")
        pd.DataFrame(columns=[
            "customer_id", "total_spend", "num_orders",
            "avg_order_value", "total_items_bought",
            "first_order_date", "last_order_date",
        ]).to_csv(OUT_PATH, index=False)
        return

    # ── Step 3: aggregate per customer ────────────────────────────────────────
    whale_buyers = (
        whales_raw.groupby("customer_id")
        .agg(
            total_spend        = ("total_amount",   "sum"),
            num_orders         = ("order_id",        "count"),
            avg_order_value    = ("total_amount",   "mean"),
            total_items_bought = ("num_items",       "sum"),
            first_order_date   = ("purchase_date",  "min"),
            last_order_date    = ("purchase_date",  "max"),
        )
        .reset_index()
    )

    # Round monetary columns
    whale_buyers["total_spend"]     = whale_buyers["total_spend"].round(2)
    whale_buyers["avg_order_value"] = whale_buyers["avg_order_value"].round(2)

    # Format dates as YYYY-MM-DD strings
    whale_buyers["first_order_date"] = whale_buyers["first_order_date"].dt.strftime("%Y-%m-%d")
    whale_buyers["last_order_date"]  = whale_buyers["last_order_date"].dt.strftime("%Y-%m-%d")

    # ── Step 4: sort ───────────────────────────────────────────────────────────
    whale_buyers = whale_buyers.sort_values("total_spend", ascending=False)

    log.info("Unique whale customers: %d", len(whale_buyers))
    context["ti"].xcom_push(key="whale_count", value=len(whale_buyers))

    # Persist to staging so the load step is a clean hand-off
    whale_buyers.to_csv(STAGING + ".whales.csv", index=False)


def load(**context) -> None:
    """Write the final whale_buyers.csv to the output directory."""
    staging_whales = STAGING + ".whales.csv"
    df = pd.read_csv(staging_whales)

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_csv(OUT_PATH, index=False)

    whale_count = context["ti"].xcom_pull(key="whale_count", task_ids="transform")
    log.info("LOAD – wrote %d whale customers to %s", whale_count or len(df), OUT_PATH)


def summarize(**context) -> None:
    """Print a pipeline summary pulled from XCom metadata."""
    ti = context["ti"]
    raw   = ti.xcom_pull(key="raw_row_count",   task_ids="extract")
    clean = ti.xcom_pull(key="clean_row_count",  task_ids="validate")
    whale = ti.xcom_pull(key="whale_count",      task_ids="transform")

    log.info(
        "\n========== WHALE BUYERS PIPELINE SUMMARY ==========\n"
        "  Raw rows ingested   : %s\n"
        "  Clean rows (staged) : %s\n"
        "  Whale customers     : %s\n"
        "  Output file         : %s\n"
        "====================================================",
        raw, clean, whale, OUT_PATH,
    )


# ── DAG definition ─────────────────────────────────────────────────────────────

default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="whale_buyers_etl",
    description="Daily ETL: identify customers who spent >$500 in the past 30 days",
    schedule="@daily",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "sales", "whale-buyers"],
) as dag:

    t_extract  = PythonOperator(task_id="extract",   python_callable=extract)
    t_validate = PythonOperator(task_id="validate",  python_callable=validate)
    t_transform= PythonOperator(task_id="transform", python_callable=transform)
    t_load     = PythonOperator(task_id="load",      python_callable=load)
    t_summary  = PythonOperator(task_id="summarize", python_callable=summarize)

    t_extract >> t_validate >> t_transform >> t_load >> t_summary
