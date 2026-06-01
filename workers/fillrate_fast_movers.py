import os
import traceback

from workers import fillrate_common as common


def refresh_for_brand(core, brand_index: int, brand_name: str, brand_key: str):
    if not common.is_fillrate_enabled() or not common.is_fast_mover_worker_enabled():
        return
    if not brand_key:
        core.logger.warning(f"Skipping fill-rate fast mover refresh for {brand_name}: missing brand key")
        return

    with core.get_db_cursor(brand_index) as (cursor, connection):
        product_ids = common.fetch_fast_movers_from_db(
            core, brand_index, brand_name, cursor, connection
        )

    ok = common.replace_fast_movers_cache(core, brand_key, product_ids)
    if ok:
        common.fillrate_log(
            core,
            "info",
            f"✅ [{brand_name}] Refreshed fill-rate fast mover cache with {len(product_ids)} products",
        )


def run_job(core):
    if not common.is_fillrate_enabled() or not common.is_fast_mover_worker_enabled():
        return

    for brand_index in core.active_brand_indices:
        brand_name = os.environ.get(f"BRAND_NAME_{brand_index}", f"Brand_{brand_index}")
        brand_key = os.environ.get(f"BRAND_TAG_{brand_index}", "")
        try:
            refresh_for_brand(core, brand_index, brand_name, brand_key)
        except Exception as e:
            core.logger.error(f"❌ [{brand_name}] Fill-rate fast mover job failed: {e}")
            traceback.print_exc()
