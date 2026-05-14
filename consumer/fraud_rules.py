"""
fraud_rules.py — Rule-based fraud scoring applied per micro-batch RDD
Each rule returns a risk score (0–100). Final score = weighted sum.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


# ── RULE WEIGHTS ─────────────────────────────────────────────────────────────
WEIGHTS = {
    "high_amount":        30,
    "foreign_country":    25,
    "risky_merchant":     20,
    "is_online":          10,
    "velocity_spike":     15,   # applied via stateful aggregation
}

FRAUD_THRESHOLD  = 60.0   # score >= 60 → flagged as fraud
RISKY_MERCHANTS  = {"luxury", "online_gaming"}
SAFE_COUNTRIES   = {"US", "UK", "DE", "FR", "CA", "AU"}


# ── UDF HELPERS ───────────────────────────────────────────────────────────────

def compute_score(amount, merchant_cat, country, is_online):
    """Pure Python scoring function — registered as a Spark UDF."""
    score = 0.0

    if amount is not None and amount > 700:
        score += WEIGHTS["high_amount"] * min(amount / 700, 2.0)

    if country and country not in SAFE_COUNTRIES:
        score += WEIGHTS["foreign_country"]

    if merchant_cat and merchant_cat in RISKY_MERCHANTS:
        score += WEIGHTS["risky_merchant"]

    if is_online:
        score += WEIGHTS["is_online"]

    return min(round(score, 2), 100.0)


def register_udfs(spark):
    """Register all UDFs with the SparkSession."""
    spark.udf.register(
        "compute_score",
        compute_score,
        DoubleType(),
    )
    return F.udf(compute_score, DoubleType())
