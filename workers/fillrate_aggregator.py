import traceback
from typing import Dict, List, Tuple

from workers import fillrate_common as common


def process_order_batch(
    core,
    brand_index: int,
    brand_name: str,
    brand_key: str,
    process_type: str,
    orders_list: List[Dict],
    cursor,
    connection,
):
    if not common.is_fillrate_enabled() or not orders_list or not brand_key:
        return

    fast_movers = common.get_fast_movers(
        core, brand_index, brand_name, brand_key, cursor, connection
    )
    if not fast_movers:
        common.fillrate_log(core, "info", f"[{brand_name}] No fast movers found. Skipping fill-rate aggregation.")
        return

    common.ensure_tables(cursor, connection)

    candidate_order_ids = sorted(
        {str(order.get("id")) for order in orders_list if order.get("id")}
    )
    state_by_key = common.load_order_state_rows(cursor, candidate_order_ids)

    agg: Dict[Tuple[str, int, int], Dict[str, int]] = {}
    state_rows = []
    matched_product_ids = set()
    matched_line_items = 0

    for order in orders_list:
        metric_date = core._parse_iso_to_ist_date(order.get("created_at"))
        order_id = str(order.get("id") or "")
        new_status = common.normalize_fulfillment_status(order.get("fulfillment_status"))

        if not metric_date or not order_id:
            continue

        for item in order.get("line_items") or []:
            if not item:
                continue
            try:
                product_id = int(item.get("product_id") or 0)
                quantity = int(item.get("quantity") or 0)
            except Exception:
                continue

            if product_id <= 0 or quantity <= 0 or product_id not in fast_movers:
                continue

            matched_product_ids.add(product_id)
            matched_line_items += 1
            variant_id = common.normalize_variant_id(item.get("variant_id"))
            state_key = (order_id, metric_date, product_id, variant_id)
            existing_state = state_by_key.get(state_key)

            ordered_counted = int(existing_state["ordered_counted"]) if existing_state else 0
            fulfilled_counted = int(existing_state["fulfilled_counted"]) if existing_state else 0

            ordered_delta = 0
            fulfilled_delta = 0

            if not ordered_counted:
                ordered_delta = quantity
                ordered_counted = 1

            if new_status == "fulfilled" and not fulfilled_counted:
                fulfilled_delta = quantity
                fulfilled_counted = 1

            if ordered_delta or fulfilled_delta:
                daily_key = (metric_date, product_id, variant_id)
                if daily_key not in agg:
                    agg[daily_key] = {"ordered_delta": 0, "fulfilled_delta": 0}
                agg[daily_key]["ordered_delta"] += ordered_delta
                agg[daily_key]["fulfilled_delta"] += fulfilled_delta

            state_payload = {
                "quantity": quantity,
                "ordered_counted": ordered_counted,
                "fulfilled_counted": fulfilled_counted,
                "current_fulfillment_status": new_status,
            }
            state_by_key[state_key] = state_payload
            state_rows.append(
                (
                    order_id,
                    metric_date,
                    product_id,
                    variant_id,
                    quantity,
                    ordered_counted,
                    fulfilled_counted,
                    new_status,
                )
            )

    if matched_product_ids:
        common.fillrate_log(
            core,
            "info",
            f"[{brand_name}] Matched fast-mover product IDs in {process_type} batch: {sorted(matched_product_ids)} (line items={matched_line_items})",
        )

    rows = [
        (metric_date, product_id, variant_id, values["ordered_delta"], values["fulfilled_delta"])
        for (metric_date, product_id, variant_id), values in agg.items()
        if values["ordered_delta"] or values["fulfilled_delta"]
    ]

    try:
        if rows:
            core.executemany_chunked(
                cursor,
                connection,
                """
                INSERT INTO product_fill_rate_daily (
                    metric_date, product_id, variant_id, ordered_units, fulfilled_units
                )
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    ordered_units = ordered_units + VALUES(ordered_units),
                    fulfilled_units = fulfilled_units + VALUES(fulfilled_units),
                    updated_at = CURRENT_TIMESTAMP
                """,
                rows,
            )
            common.fillrate_log(
                core,
                "info",
                f"✅ [{brand_name}] Upserted {len(rows)} daily fill-rate rows from {process_type} batch",
            )
        else:
            common.fillrate_log(core, "info", f"[{brand_name}] No fill-rate deltas to upsert for {process_type} batch")

        common.upsert_order_state_rows(core, cursor, connection, state_rows, brand_name)
    except Exception as e:
        core.logger.error(f"❌ [{brand_name}] Failed to process fill-rate daily facts: {e}")
        traceback.print_exc()
