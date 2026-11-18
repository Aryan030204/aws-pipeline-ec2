"""
Memory benchmark for the order path: LEGACY (whole fetch window in one list) vs NEW (bounded sub-windows).

SYNTHETIC. No database, no Shopify, no network, and the repo's .env is never read (dotenv is stubbed in the child
process). What is real: pipeline.shopify_orders.transform_orders_to_df_optimized, split_order_window,
compute_insert_chunk_rows. What is simulated: the Shopify pages (fresh dicts, 250 per page, built like
response.json() output) and the database write, which emulates what pandas' to_sql(method="multi") does per chunk
(`insert(table).values(list_of_row_dicts)` compiled with the MySQL dialect, plus a flat string of the bound values as a
stand-in for the connector's client-side formatting) and then discards the result.

Each (driver, size) runs in a FRESH subprocess so peak RSS is not polluted by earlier runs:

    LEGACY: every page of the whole range is appended to ONE list (`order_list.extend(page)`), then transform, then the
            insert in 1000-row chunks, with the whole list still referenced - the code before the fix.
    NEW:    the range is split by split_order_window (24h by default); per sub-window only that window's pages are
            accumulated, transformed and inserted in compute_insert_chunk_rows() chunks, then released.

Usage (from the repo root):
    python scripts/memory_benchmark.py                          # orders: 1k 5k 10k 20k 30k, legacy vs new
    python scripts/memory_benchmark.py --sizes 1000 10000 --days 30 --tracemalloc
    python scripts/memory_benchmark.py --traffic                # product-traffic fetch sizing (dates x products)
Numbers are for THIS machine / OS / Python, not the production Linux 3.11 image; use them for the ratio between legacy
and new, and for how each curve scales with the number of orders.
"""

import argparse
import gc
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IST = ZoneInfo("Asia/Kolkata")
PAGE_SIZE = 250


# ------------------------------------------------------------------------------------------------
# Measurement helpers
# ------------------------------------------------------------------------------------------------
def current_and_peak_rss_mb():
    """(current RSS, peak RSS) of THIS process in MB."""
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD),
                        ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                        ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]

        kernel, psapi = ctypes.windll.kernel32, ctypes.windll.psapi
        kernel.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(), ctypes.byref(counters), counters.cb)
        return counters.WorkingSetSize / 1048576, counters.PeakWorkingSetSize / 1048576
    values = {}
    with open("/proc/self/status") as handle:
        for line in handle:
            key, _, rest = line.partition(":")
            if key in ("VmRSS", "VmHWM"):
                values[key] = int(rest.split()[0]) / 1024
    return values["VmRSS"], values["VmHWM"]


# ------------------------------------------------------------------------------------------------
# Synthetic Shopify orders (fresh dicts per page, unique strings per order, ~2-3 KB of JSON each)
# ------------------------------------------------------------------------------------------------
def make_order(index, created):
    n = f"{index:08d}"

    def address():
        return {"address1": f"Flat {index % 900}, Sunrise Apartments, Road {n}", "phone": f"+9198{index % 100000000:08d}",
                "city": f"City{index % 300}", "zip": f"{560000 + index % 999}", "province": f"State{index % 28}"}

    def item(k):
        pid = 7771234567890 + (index * 3 + k) % 4000
        return {"sku": f"SKU-{pid}-{k}", "title": f"Vitamin C Serum {pid % 97} ml - Brightening", "variant_title": f"{k + 1} Pack / {pid % 5}",
                "price": f"{799 + k * 100}.00", "quantity": 1 + (index + k) % 3, "total_discount": "80.00",
                "product_id": pid, "variant_id": pid * 10 + k,
                "properties": [{"name": "_bundle_id", "value": f"bdl-{pid}"}, {"name": "_bundle_name", "value": f"Glow Combo {pid % 50}"}]}

    stamp = created.isoformat()
    return {
        "id": 5512345678901 + index, "name": f"#{index}", "tags": "cod, rto-check, repeat", "created_at": stamp, "updated_at": stamp,
        "cancelled_at": None, "total_price": f"{1598 + index % 400}.00", "financial_status": "paid", "payment_gateway_names": ["Gokwik PPCOD"],
        "app_id": 580111, "currency": "INR", "discount_codes": [{"code": f"WELCOME{index % 50}", "amount": "160.00", "type": "percentage"}],
        "discount_applications": [{"title": f"WELCOME{index % 50}", "value": "10.0", "type": "discount_code", "app_id": 1234}],
        "landing_site": f"/products/vitamin-c-serum-{index % 700}?utm_source=facebook&utm_medium=paid&utm_campaign=bf_sale_{index % 40}&utm_content=video{index % 9}&utm_term=skin",
        "refunds": [], "line_items": [item(0), item(1)] if index % 3 else [item(0)],
        "customer": {"id": 6612345678 + index, "email": f"customer.{n}@example.com", "first_name": f"First{index % 500}",
                     "last_name": f"Last{index % 700}", "phone": f"+9197{index % 100000000:08d}", "tags": "vip, repeat"},
        "shipping_address": address(), "billing_address": address(),
        "note_attributes": [{"name": "utm_source", "value": "facebook"}, {"name": "appmaker_platform", "value": "android"},
                            {"name": "app_version", "value": "4.2.1"}, {"name": "customer_ip", "value": f"103.21.{index % 250}.{index % 200}"},
                            {"name": "pg_order_id", "value": f"PG-{n}"}],
    }


def pages(first_index, count, window_start, spacing_seconds):
    """Yield pages (lists of fresh order dicts) for orders first_index .. first_index+count-1, spaced evenly in time."""
    produced = 0
    while produced < count:
        take = min(PAGE_SIZE, count - produced)
        yield [make_order(first_index + produced + k, window_start + timedelta(seconds=(produced + k) * spacing_seconds))
               for k in range(take)]
        produced += take


# ------------------------------------------------------------------------------------------------
# The simulated database write (what pandas' to_sql(method="multi") does per chunk)
# ------------------------------------------------------------------------------------------------
class StubWriter:
    def __init__(self, df):
        from sqlalchemy import Column, MetaData, Table, Text
        from sqlalchemy.dialects import mysql

        self.keys = list(df.columns)
        self.table = Table("shopify_orders", MetaData(), *[Column(k, Text) for k in self.keys])
        self.dialect = mysql.dialect()
        self.rows_written = 0
        self.params_in_largest_statement = 0

    def write(self, df, chunk_rows):
        from sqlalchemy import insert

        for lo in range(0, len(df), chunk_rows):
            part = df.iloc[lo:lo + chunk_rows]
            data = [dict(zip(self.keys, row)) for row in part.itertuples(index=False, name=None)]  # pandas _execute_insert_multi
            compiled = insert(self.table).values(data).compile(dialect=self.dialect)
            flat = "".join(repr(value) for value in compiled.params.values())  # stand-in for the connector's formatting
            self.params_in_largest_statement = max(self.params_in_largest_statement, len(compiled.params))
            self.rows_written += len(part)
            del data, compiled, flat


# ------------------------------------------------------------------------------------------------
# Drivers
# ------------------------------------------------------------------------------------------------
def run_legacy(n_orders, days, transform, tracemalloc_on):
    window_start = datetime(2026, 11, 1, 0, 0, 0, tzinfo=IST)
    spacing = days * 86400 / n_orders
    order_list = []
    for page in pages(0, n_orders, window_start, spacing):
        order_list.extend(page)  # the old fetch_orders_async: the whole range accumulates in one list
    df = transform(order_list, {}, IST)
    writer = StubWriter(df)
    writer.write(df, 1000)  # the old max(500, min(BATCH_SIZE, 2000)) with BATCH_SIZE=1000
    return {"rows": writer.rows_written, "windows": 1, "chunk_rows": 1000, "params_per_statement": writer.params_in_largest_statement}


def run_new(n_orders, days, transform, tracemalloc_on, span_hours):
    from pipeline import shopify_orders as so

    window_start = datetime(2026, 11, 1, 0, 0, 0, tzinfo=IST)
    window_end = window_start + timedelta(days=days) - timedelta(seconds=1)
    tiles = so.split_order_window(window_start, window_end, span_hours)
    spacing = days * 86400 / n_orders
    total_rows, chunk_rows, largest, writer, first = 0, 0, 0, None, 0
    for tile in tiles:
        # the orders whose (evenly spaced) timestamps fall in this tile
        lo = int((tile.start - window_start).total_seconds() / spacing + 0.9999999)
        hi = min(n_orders, int((tile.end_exclusive - window_start).total_seconds() / spacing + 0.9999999))
        count = max(0, hi - lo)
        if not count:
            continue
        orders = []
        for page in pages(lo, count, window_start + timedelta(seconds=lo * spacing), spacing):
            orders.extend(page)
        df = transform(orders, {}, IST)
        writer = writer or StubWriter(df)
        chunk_rows = so.compute_insert_chunk_rows(len(df.columns), 1000)
        writer.write(df, chunk_rows)
        total_rows += len(df)
        largest = max(largest, writer.params_in_largest_statement)
        del orders, df  # released before the next sub-window is fetched
    return {"rows": total_rows, "windows": len(tiles), "chunk_rows": chunk_rows, "params_per_statement": largest}


def run_traffic(dates, rows_per_date):
    """The real update_product_traffic_split_incremental against a fake cursor that returns `dates x rows_per_date` rows."""
    from datetime import date
    from decimal import Decimal

    from pipeline import utm_traffic

    day0 = date(2026, 1, 1)
    target = [(day0 + timedelta(days=d)).isoformat() for d in range(dates)]
    sources = ["facebook", "google", "instagram", "direct", "email", "whatsapp", None]

    class Cursor:
        def __init__(self):
            self.statements = 0
            self.requested = ()

        def execute(self, sql, params=None):
            self.statements += 1
            if "FROM product_sessions_snapshot" in " ".join(sql.split()):
                self.requested = tuple(params or ())  # the dates THIS pass asked for

        def executemany(self, sql, rows):
            self.statements += 1

        def fetchall(self):
            # only the requested dates' rows, like the real GROUP BY ... WHERE date IN (...) query
            wanted = [(date.fromisoformat(d) - day0).days for d in self.requested]
            return [{"date": day0 + timedelta(days=d), "product_id": str(7771234567890 + p // 2),
                     "utm_source": sources[(p + d) % len(sources)], "sessions": Decimal(10 + p % 40), "atc_sessions": Decimal(p % 7)}
                    for d in wanted for p in range(rows_per_date)]

    class Connection:
        def commit(self):
            pass

    utm_traffic.update_product_traffic_split_incremental(Cursor(), Connection(), "BENCH", target)
    return {"rows": dates * rows_per_date, "windows": 1, "chunk_rows": 0, "params_per_statement": 0}


# ------------------------------------------------------------------------------------------------
# Child / parent
# ------------------------------------------------------------------------------------------------
def child(args):
    import dotenv

    dotenv.load_dotenv = lambda *a, **k: False  # never read the repo's .env (production credentials)
    sys.path.insert(0, ROOT)
    import logging

    import aws_background  # noqa: F401  (import order: the pipeline modules import it circularly)

    logging.disable(logging.CRITICAL)
    from pipeline import shopify_orders as so

    gc.collect()
    baseline, _ = current_and_peak_rss_mb()
    if args.tracemalloc:
        import tracemalloc

        tracemalloc.start()
    started = time.perf_counter()
    if args.child == "traffic":
        info = run_traffic(args.traffic_dates, args.traffic_rows)
    elif args.child == "legacy":
        info = run_legacy(args.orders, args.days, so.transform_orders_to_df_optimized, args.tracemalloc)
    else:
        info = run_new(args.orders, args.days, so.transform_orders_to_df_optimized, args.tracemalloc, args.span_hours)
    elapsed = time.perf_counter() - started
    after, peak = current_and_peak_rss_mb()
    result = dict(info, baseline_mb=round(baseline, 1), peak_mb=round(peak, 1), after_mb=round(after, 1), seconds=round(elapsed, 1))
    if args.tracemalloc:
        import tracemalloc

        result["py_peak_mb"] = round(tracemalloc.get_traced_memory()[1] / 1048576, 1)
    print("RESULT " + json.dumps(result))


def spawn(extra):
    cmd = [sys.executable, os.path.abspath(__file__)] + extra
    out = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT, timeout=3600)
    for line in out.stdout.splitlines():
        if line.startswith("RESULT "):
            return json.loads(line[7:])
    raise RuntimeError(f"benchmark child failed ({extra}):\n{out.stdout[-800:]}\n{out.stderr[-1500:]}")


def parent(args):
    import platform

    sample = make_order(1, datetime(2026, 11, 1, tzinfo=IST))
    print(f"python {platform.python_version()} on {platform.system()} {platform.release()}; synthetic order ~{len(json.dumps(sample))/1024:.1f} KB JSON; "
          f"window {args.days} days, new-path sub-window {args.span_hours:g}h, orders spread evenly\n")
    if args.traffic:
        print("product-traffic fetch (real update_product_traffic_split_incremental, fake cursor), rows/date =", args.traffic_rows)
        print("| dates | rows fetched | peak RSS MB | RSS over baseline MB | seconds |" + (" python-heap peak MB |" if args.tracemalloc else ""))
        for dates in args.traffic_dates_list:
            extra = ["--child", "traffic", "--traffic-dates", str(dates), "--traffic-rows", str(args.traffic_rows)] + (["--tracemalloc"] if args.tracemalloc else [])
            r = spawn(extra)
            line = f"| {dates} | {r['rows']:,} | {r['peak_mb']:.0f} | {r['peak_mb'] - r['baseline_mb']:.0f} | {r['seconds']} |"
            print(line + (f" {r['py_peak_mb']:.0f} |" if args.tracemalloc else ""))
        return
    header = "| orders | driver | windows | rows | chunk rows | params/stmt | baseline MB | peak RSS MB | peak over baseline MB | seconds |"
    print(header + (" python-heap peak MB |" if args.tracemalloc else ""))
    print("|" + "---|" * (header.count("|") - 1 + (1 if args.tracemalloc else 0)))
    for n in args.sizes:
        for driver in ("legacy", "new"):
            extra = ["--child", driver, "--orders", str(n), "--days", str(args.days), "--span-hours", str(args.span_hours)] + (["--tracemalloc"] if args.tracemalloc else [])
            r = spawn(extra)
            line = (f"| {n:,} | {driver} | {r['windows']} | {r['rows']:,} | {r['chunk_rows']} | {r['params_per_statement']:,} | {r['baseline_mb']:.0f} | "
                    f"{r['peak_mb']:.0f} | {r['peak_mb'] - r['baseline_mb']:.0f} | {r['seconds']} |")
            print(line + (f" {r['py_peak_mb']:.0f} |" if args.tracemalloc else ""), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--sizes", type=int, nargs="+", default=[1000, 5000, 10000, 20000, 30000])
    p.add_argument("--days", type=int, default=30, help="length of the fetch window in days (orders spread evenly)")
    p.add_argument("--span-hours", type=float, default=24.0, help="new-path sub-window length")
    p.add_argument("--tracemalloc", action="store_true", help="also report the Python-heap peak (slower)")
    p.add_argument("--traffic", action="store_true", help="size the product-traffic fetch instead of the order path")
    p.add_argument("--traffic-rows", type=int, default=2000, help="rows per date for --traffic")
    p.add_argument("--traffic-dates-list", type=int, nargs="+", default=[7, 30, 90, 180])
    p.add_argument("--child", choices=["legacy", "new", "traffic"], help=argparse.SUPPRESS)
    p.add_argument("--orders", type=int, help=argparse.SUPPRESS)
    p.add_argument("--traffic-dates", type=int, help=argparse.SUPPRESS)
    args = p.parse_args()
    child(args) if args.child else parent(args)


if __name__ == "__main__":
    main()
