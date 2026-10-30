"""
Single source of truth for how an order's payment_gateway_names maps to a
payment bucket, in Python (classify_payment_mode) and as SQL fragments used by
every summary query, so the two can never drift apart.

Buckets (mutually exclusive, together exhaustive):
  unmapped      - no gateway name at all (NULL or blank)
  partially_paid- contains "Gokwik PPCOD" (checked before COD)
  cod           - contains a COD gateway name (and no PPCOD)
  prepaid       - any other non-blank gateway name

Matching is case-insensitive in both Python and SQL (the MySQL columns use a
case-insensitive collation).
"""

PPCOD_NAME = "Gokwik PPCOD"
COD_NAMES = ("Cash on Delivery (COD)", "cash_on_delivery")


def classify_payment_mode(payment_gateway_names) -> str:
    pg = (payment_gateway_names or "").strip().lower()
    if not pg:
        return "unmapped"
    if PPCOD_NAME.lower() in pg:
        return "partially_paid"
    if any(name.lower() in pg for name in COD_NAMES):
        return "cod"
    return "prepaid"


def payment_predicates(col: str = "payment_gateway_names") -> dict:
    """SQL boolean expressions for each bucket over the given column."""
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


def breakdown_select_sql(col: str = "payment_gateway_names") -> str:
    """Per-group order counts and sales for all four buckets, as select-list
    text: prepaid_orders, cod_orders, ppcod_orders, unmapped_orders,
    cod_sales, prepaid_sales, partially_prepaid_sales, unmapped_sales."""
    p = payment_predicates(col)
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
