"""
How an order's payment_gateway_names maps to a payment bucket, in Python (classify_payment_mode) and as SQL
fragments used by every summary query, so the two can never drift apart.

TWO RULES, chosen per brand (brand_uses_unmapped_bucket):

  LEGACY rule (every brand by default - the behaviour the pipeline had before the "unmapped" bucket existed):
    partially_paid - contains "Gokwik PPCOD" (checked first)
    cod            - gateway name is NULL/blank, OR contains a COD gateway name   (blank means COD)
    prepaid        - any other non-blank gateway name
    unmapped       - never used (always 0)

  UNMAPPED rule (only the brands listed in UNMAPPED_PAYMENT_BRANDS, default: editeveryday):
    unmapped       - no gateway name at all (NULL or blank)  <- NOT counted as COD
    partially_paid - contains "Gokwik PPCOD" (checked before COD)
    cod            - contains a COD gateway name (and no PPCOD)
    prepaid        - any other non-blank gateway name

Why per brand: a brand whose COD orders reach Shopify with no gateway name at all (BBB) needs blank == COD, or
its COD orders vanish from the COD columns (total_orders stays right, cod_orders reads 0). EDITEVERYDAY wants
those orders reported separately as "unmapped".

Callers pass unmapped_bucket=brand_uses_unmapped_bucket(brand_name). The DEFAULT of every function here is the
legacy rule, so a call site that forgets the flag falls back to the long-standing behaviour.

Matching is case-insensitive in SQL (the MySQL columns use a case-insensitive collation).
"""

import os
import re
from typing import Mapping, Optional

PPCOD_NAME = "Gokwik PPCOD"
COD_NAMES = ("Cash on Delivery (COD)", "cash_on_delivery")

# Comma-separated brand names / tags that use the UNMAPPED rule. Compared after lower-casing and dropping every
# non-alphanumeric character ("Edit Everyday" == "editeveryday"). Set to an empty string to turn it off for all.
UNMAPPED_BRANDS_ENV = "UNMAPPED_PAYMENT_BRANDS"
DEFAULT_UNMAPPED_BRANDS = "editeveryday"


# ---------------------------------------------------------------------------
# Per-brand switch
# ---------------------------------------------------------------------------
def _token(value) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def unmapped_bucket_brand_tokens(env: Optional[Mapping[str, str]] = None) -> frozenset:
    env = os.environ if env is None else env
    raw = env.get(UNMAPPED_BRANDS_ENV)
    if raw is None:
        raw = DEFAULT_UNMAPPED_BRANDS
    return frozenset(t for t in (_token(part) for part in raw.split(",")) if t)


def brand_uses_unmapped_bucket(
    brand_name: Optional[str],
    brand_key: Optional[str] = None,
    env: Optional[Mapping[str, str]] = None,
) -> bool:
    """True only for brands configured for the UNMAPPED rule. Matches the brand name or tag; when only the
    name is known it also looks up that brand's BRAND_TAG_<i> in the environment."""
    env = os.environ if env is None else env
    tokens = unmapped_bucket_brand_tokens(env)
    if not tokens:
        return False
    name_token, key_token = _token(brand_name), _token(brand_key)
    if name_token in tokens or key_token in tokens:
        return True
    if name_token:
        for key, value in env.items():
            if key.startswith("BRAND_NAME_") and _token(value) == name_token:
                if _token(env.get("BRAND_TAG_" + key[len("BRAND_NAME_"):])) in tokens:
                    return True
    return False


# ---------------------------------------------------------------------------
# Python classification
# ---------------------------------------------------------------------------
def classify_payment_mode(payment_gateway_names, unmapped_bucket: bool = False) -> str:
    if unmapped_bucket:
        pg = (payment_gateway_names or "").strip().lower()
        if not pg:
            return "unmapped"
        if PPCOD_NAME.lower() in pg:
            return "partially_paid"
        if any(name.lower() in pg for name in COD_NAMES):
            return "cod"
        return "prepaid"

    # legacy rule, exactly as it was before the unmapped bucket: blank means COD
    pg = payment_gateway_names or ""
    if PPCOD_NAME in pg:
        return "partially_paid"
    if not pg or any(name in pg for name in COD_NAMES):
        return "cod"
    return "prepaid"


# ---------------------------------------------------------------------------
# SQL fragments
# ---------------------------------------------------------------------------
def payment_predicates(col: str = "payment_gateway_names", unmapped_bucket: bool = False) -> dict:
    """SQL boolean expressions for each bucket over the given column."""
    if not unmapped_bucket:
        # legacy rule: the SQL the pipeline used before the unmapped bucket existed
        cod_name_match = " OR ".join(f"{col} LIKE '%{name}%'" for name in COD_NAMES)
        return {
            "unmapped": "(1 = 0)",
            "ppcod": f"({col} LIKE '%{PPCOD_NAME}%')",
            "cod": (
                f"(({col} IS NULL OR {col} = '' OR {cod_name_match}) "
                f"AND ({col} NOT LIKE '%{PPCOD_NAME}%' OR {col} IS NULL))"
            ),
            "prepaid": (
                f"({col} IS NOT NULL AND {col} != '' "
                f"AND NOT ({cod_name_match} OR {col} LIKE '%{PPCOD_NAME}%'))"
            ),
        }

    has_value = f"{col} IS NOT NULL AND TRIM({col}) <> ''"
    not_ppcod = f"{col} NOT LIKE '%{PPCOD_NAME}%'"
    is_cod_name = " OR ".join(f"{col} LIKE '%{name}%'" for name in COD_NAMES)
    not_cod_name = " AND ".join(f"{col} NOT LIKE '%{name}%'" for name in COD_NAMES)
    return {
        "unmapped": f"({col} IS NULL OR TRIM({col}) = '')",
        "ppcod": f"({has_value} AND {col} LIKE '%{PPCOD_NAME}%')",
        "cod": f"({has_value} AND {not_ppcod} AND ({is_cod_name}))",
        "prepaid": f"({has_value} AND {not_ppcod} AND {not_cod_name})",
    }


def count_distinct_orders(condition: str, order_col: str = "order_id") -> str:
    return f"COUNT(DISTINCT CASE WHEN {condition} THEN {order_col} END)"


def count_rows(condition: str) -> str:
    return f"COUNT(CASE WHEN {condition} THEN 1 END)"


def sum_amount(condition: str, amount: str = "COALESCE(total_price, 0)") -> str:
    return f"SUM(CASE WHEN {condition} THEN {amount} ELSE 0 END)"


def breakdown_select_sql(col: str = "payment_gateway_names", unmapped_bucket: bool = False) -> str:
    """Per-group order counts and sales for all four buckets, as select-list
    text: prepaid_orders, cod_orders, ppcod_orders, unmapped_orders,
    cod_sales, prepaid_sales, partially_prepaid_sales, unmapped_sales."""
    p = payment_predicates(col, unmapped_bucket)
    return ",\n".join(
        [
            f"{count_distinct_orders(p['prepaid'])} AS prepaid_orders",
            f"{count_distinct_orders(p['cod'])} AS cod_orders",
            f"{count_distinct_orders(p['ppcod'])} AS ppcod_orders",
            f"{count_distinct_orders(p['unmapped'])} AS unmapped_orders",
            f"{sum_amount(p['cod'])} AS cod_sales",
            f"{sum_amount(p['prepaid'])} AS prepaid_sales",
            f"{sum_amount(p['ppcod'])} AS partially_prepaid_sales",
            f"{sum_amount(p['unmapped'])} AS unmapped_sales",
        ]
    )
