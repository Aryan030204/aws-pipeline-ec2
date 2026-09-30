<!-- title: Shopify Pipeline -->

# Shopify Pipeline

A multi-brand Shopify data pipeline. It pulls orders, sessions, checkout events, and page-speed data for every configured brand, keeps each brand's own MySQL database up to date, and sends live alerts for sales and site performance.

![Architecture](docs/images/architecture.png)

## What it does

For every brand it manages, the pipeline:

- Fetches new and updated Shopify orders and loads them into that brand's own MySQL database.
- Pulls session and traffic data via ShopifyQL (Shopify's analytics query language).
- Syncs checkout-initiated and buy-now events from MongoDB.
- Syncs page-speed ("web vitals") data from MongoDB.
- Rebuilds dashboard summary tables — but only for the dates that actually changed, not the whole table — including a full payment-mode breakdown (COD, prepaid, partially-prepaid) for both order counts and sales amounts.
- Tracks any checkout/session event type per product variant, not just checkout-initiated and buy-now — new event types are discovered automatically and get their own column, via a separate daily worker.
- Rolls up "bundle" product sales (currently TMC and PTS) — bundle products are discovered automatically from the Shopify product catalog (active products only), not a hardcoded list.
- Sends live alerts to QStash: sales/session numbers and page-speed averages, per brand.
- Triggers the downstream cache pipeline directly after each run finishes, so dashboards pick up fresh data.

Each brand is fully isolated — its own database, its own Shopify store, its own failure boundary. If one brand's run fails, the others still complete.

## How it runs

- **Automatically**, every 10 minutes, via an internal scheduler (APScheduler).
- **On demand**, via `POST /trigger` (used for manual runs or by an external pipeline manager).
- Packaged as a Docker container, deployed on an EC2 instance.

## Project structure

The pipeline used to be a single ~10,000-line file. It's now split into focused modules under `pipeline/`, all tied together through `aws_background.py`, which remains the entrypoint.

![Project structure](docs/images/project-structure.png)

| Module | Responsibility |
|---|---|
| `aws_background.py` | Entrypoint, brand configuration, and the facade that re-exports everything below (so nothing else in the repo had to change) |
| `pipeline/state.py` | Shared config and long-lived objects (logger, Flask app, scheduler, brand registries) |
| `pipeline/db.py` | Database connections, retries, and cleanup |
| `pipeline/schema_migrations.py` | Table creation / schema upkeep |
| `pipeline/shopify_orders.py` | Order fetching, transforming, and loading |
| `pipeline/summary_incremental.py` | Rebuilding dashboard summary tables |
| `pipeline/mongo_events.py` | Checkout-initiated / buy-now event sync |
| `pipeline/web_vitals.py` | Page-speed data sync |
| `pipeline/shopifyql_sessions.py` | Session and city-level traffic analytics |
| `pipeline/utm_traffic.py` | UTM and referrer attribution |
| `pipeline/notifications.py` | QStash alerts and webhook triggers |
| `pipeline/orchestration.py` | Runs everything above, per brand, per pipeline tick |
| `bundle_rollups.py` | Bundle product discovery (via the Shopify Admin API, active products only) and sales rollups (TMC, PTS) |
| `workers/events_summary_worker.py` | A separate, smaller worker that rolls up daily event totals — runs as its own Docker container |
| `workers/variant_wise_events_worker.py` | A separate worker that rolls up any Mongo event type, per product variant, into each brand's own database — runs as its own Docker container |
| `workers/checkout_login_status_worker.py` | A separate worker that rolls up daily checkout counts by logged-in/non-logged-in status, per brand and per variant — runs as its own Docker container |
| `backfill scripts/` | Standalone scripts for re-running specific pieces of the pipeline over a historical date range |

## Configuration

Brands are configured through numbered environment variables — one full set per brand index:

```
BRAND_NAME_0=PTS
BRAND_TAG_0=pts
DB_HOST_0=...
DB_USER_0=...
DB_PASSWORD_0=...
SHOP_NAME_0=...
ACCESS_TOKEN_0=...
```

In production, brand configuration is instead pulled from an internal brand-config API. Local/dev runs use `TEST_MODE=true`, which loads brand config from `.env.local` instead and disables outbound QStash calls.

Other important flags:

| Variable | Purpose |
|---|---|
| `SELF_RUN` | `true` = auto-run on schedule (every 10 minutes, or per `BRANDS_CRON_MAP` below); `false` = manual trigger only |
| `BRANDS_CRON_MAP` | Optional per-brand cron override, JSON `{"<brand_index>": "<minute-list>"}`, e.g. `{"3":"15,45","4":"15,45","1":"0,30","2":"0,30"}` runs brands 3/4 at :15/:45 past the hour and 1/2 at :00/:30. Any active brand **not** a key here (including every brand, if this is unset/empty) keeps running on the legacy every-10-min schedule - opt-in per brand. Read once at startup; changing it needs a restart. |
| `BACKFILL_MODE` | `true` = process a specific historical date window instead of "since last run" |
| `BACKFILL_BRAND_INDICES` | Restrict a backfill run to specific brands |
| `ALERTS_WEBHOOK_URL` | Where QStash-forwarded sales/web-vitals alerts get delivered |
| `PIPELINE_METRICS_SHEET_SWITCH` | `true` (default) = log each run's per-brand metrics to Google Sheets (see below); `false` disables it entirely |
| `PIPELINE_METRICS_SHEET_ID` | The destination spreadsheet's ID (the long id in its URL) |
| `GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE` | Path to a service-account JSON key file, mounted into the container. Use this **or** the next var, not both |
| `GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON_B64` | The same service-account JSON key, base64-encoded, as an env var instead of a mounted file |

No secrets or real credentials are ever meant to live in source control — see `.env.local`/`.env` locally, and the deployment's own secret store in production.

### Pipeline run metrics (Google Sheets)

After each run, `pipeline/metrics_logging.py` appends one row per brand (new/updated order counts, affected-date count, sessions/ATC sessions synced, success/error, that brand's own duration) plus one `ALL BRANDS` summary row (aggregated counts, overall run duration), to a monthly worksheet (`Runs-2026-09`, etc., created automatically) in the spreadsheet at `PIPELINE_METRICS_SHEET_ID`. It's entirely best-effort — a Sheets outage, bad credentials, or quota error is logged and never fails or delays the pipeline run.

One-time setup:
1. In Google Cloud Console, create a service account and download its JSON key.
2. Enable the Google Sheets API for that project.
3. Share your destination spreadsheet with the service account's email (found in the JSON key, `client_email`) as **Editor**.
4. Set `PIPELINE_METRICS_SHEET_ID` (from the spreadsheet's URL) and either `GOOGLE_SHEETS_SERVICE_ACCOUNT_FILE` or `GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON_B64`.

## Running it locally

```bash
pip install -r requirements.txt
python aws_background.py
```

With `TEST_MODE=true`, this loads brand config from `.env.local`, starts the scheduler, and serves the API on port 5000.

Check it's alive:

```bash
curl http://localhost:5000/health
```

Trigger a run manually:

```bash
curl -X POST http://localhost:5000/trigger \
  -H "Content-Type: application/json" \
  -H "x-pipeline-key: <PIPELINE_AUTH_HEADER>" \
  -d '{"runId": "manual-test-1"}'
```

## API endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/health` | GET | Basic liveness check, returns `{"status": "healthy", ...}` |
| `/trigger` | POST | Runs the pipeline immediately. Requires an `x-pipeline-key` header and a JSON body with a `runId`. Optional `brandIds` (a list, or a single `brandId`) restricts the run to just those brands, independent of `BRANDS_CRON_MAP` grouping - omitted, it runs every active brand as before |

## Deployment

Runs as four Docker services (see `docker-compose.yml`):

- **`shopify-pipeline`** — the main pipeline described above.
- **`events-summary-worker`** — a separate container that rolls up daily event totals across all brands into one summary table, on its own daily schedule. The same daily run also stores the full raw `alerts.sessions` events, one row per event, in each brand's own `mongo_events_summary` table (every document key becomes a column, nested `data` is flattened, new keys add columns automatically, `createdAt`/`updatedAt` kept as UTC and IST). Older history can be loaded with `backfill scripts/backfill_mongo_events_summary.py`.
- **`variant-wise-events-worker`** — a separate container that rolls up any Mongo event type, per product variant, into each brand's own database, on its own daily schedule.
- **`checkout-login-status-worker`** — a separate container that rolls up daily checkout counts by logged-in/non-logged-in status, per brand, on its own daily schedule (00:05 IST).
- **`pnl-worker`** — a separate container that builds the daily brand-level P&L (`overall_pnl`) in each brand DB at 00:10 IST, from the summary tables plus the brand's cost config in the arch-auth Mongo `total_config` collection. Right before that it syncs paid-ads spend for exactly the dates it is rebuilding: for every brand with a credential document in Mongo (`meta_ads_credentials` / `google_ads_credentials`, pasted on the dashboard's P&L page) it pulls that channel's daily spend and upserts it into `pnl_meta_ad_spend_rollup` / `pnl_google_ad_spend_rollup` (`migrations/004_pnl_ad_spend_rollups.sql`, per brand DB). Brands without a document for a channel are skipped for it. Needs `PASSWORD_AES_KEY` (same as the dashboard, to decrypt tokens) and, for Google only, `GOOGLE_ADS_CLIENT_ID`, `GOOGLE_ADS_CLIENT_SECRET` and `GOOGLE_ADS_DEVELOPER_TOKEN`; `META_API_VERSION` / `GOOGLE_ADS_API_VERSION` are optional. A failed pull is logged, written to the document's `last_error`, and that line falls back to the configured cost.

To deploy an update on the server: pull the latest code on the branch that's live, rebuild, and restart:

```bash
cd ~/pipelines/aws-pipeline-ec2
git pull origin master
docker compose build shopify-pipeline events-summary-worker variant-wise-events-worker checkout-login-status-worker
docker compose up -d shopify-pipeline events-summary-worker variant-wise-events-worker checkout-login-status-worker
```

`checkout-login-status-worker`'s own schedule is once a day (00:05 IST), so after deploying a change to it, optionally trigger one immediate run instead of waiting for the next scheduled tick:

```bash
docker compose exec -T checkout-login-status-worker \
  python -c "from workers.checkout_login_status_worker import run_worker; run_worker()"
```

### Cache pipeline trigger

The cache pipeline runs as its own container (`cache-pipeline`) on the same EC2 host, joined to the same Docker network (`pipeline-net`) as `shopify-pipeline`. After each full run finishes, the pipeline fires a plain `GET http://cache-pipeline:8080/trigger-pipeline` — no QStash involved, just a direct container-to-container call over the shared network.

This request runs in a background thread, so the pipeline doesn't wait for the cache rebuild to finish before moving on. Whenever the response does come back, only a short summary (status + brand count) is logged — not the full per-brand/per-date result body.

### Per-brand cron groups

With `BRANDS_CRON_MAP` set, each distinct cron value gets its own scheduler job (independent `max_instances=1`, so a slow run of one group never blocks another group's tick), plus the always-registered legacy fallback job (`pipeline_job`, every 10 minutes) for anything not in the map. A manual `/trigger` call — with or without `brandIds` — always runs as its own one-off job, so it can overlap with a brand's own cron group if the timing lines up; per-brand DB writes are upserts (`ON DUPLICATE KEY UPDATE`), so the worst case is duplicated work, not corrupted data.

## Triggering a worker manually

Every standalone worker (`pnl`, `events-summary`, `variant-wise-events`, `checkout-login-status`, `intent-events`) can be run on demand, once, without waiting for its daily schedule. Both knobs below use the same registry in `workers/registry.py`, so adding a worker there makes it available to both.

### From a shell

```bash
python -m workers.run list                                      # what's available
python -m workers.run pnl                                       # default window
python -m workers.run pnl --start 2026-09-01 --end 2026-09-15   # backfill a range
python -m workers.run checkout-login-status --as-of 2026-09-10T00:05:00+05:30
python -m workers.run pnl --env-file env                        # load a specific dotenv file
```

It loads `.env` (or a file named `env`) unless `--env-file` says otherwise, forces the worker's `*_SELF_RUN` flag off, runs it in the foreground, and exits 0/1. Inside Docker:

```bash
docker compose exec -T shopify-pipeline python -m workers.run pnl
```

### Over HTTP (main pipeline container, port 5000)

```bash
curl http://localhost:5000/workers                                       # list + last run per worker
curl -X POST http://localhost:5000/workers/pnl/trigger \
  -H "x-pipeline-key: $PIPELINE_AUTH_HEADER" \
  -H "Content-Type: application/json" \
  -d '{"start": "2026-09-01", "end": "2026-09-15"}'                      # body optional
curl http://localhost:5000/workers/pnl/status                            # running / succeeded / failed
```

The trigger spawns `python -m workers.run <name>` as a child process of the pipeline container, so the worker's output shows up in `docker logs shopify-pipeline`. One run per worker at a time; a second trigger while one is running returns 409.

## Testing

```bash
python -m unittest discover -p "test_*.py"
```

Covers brand-activation filtering, bundle rollup logic, UTM source mapping, and the events summary worker.

## Backfill scripts

`backfill scripts/` holds standalone tools for re-running one specific piece of the pipeline over a chosen date range — useful for fixing a gap in historical data without re-running everything.

| Category | Examples |
|---|---|
| Orders & addresses | `backfill_shopify.py`, `backfill_shopify_order_addresses.py`, `backfill_shopify_order_tags.py` |
| Returns / RTO | `backfill_returns_fact_order_created_date.py`, `backfill_rto_summary.py` |
| Checkout / buy-now events | `backfill_checkout_initiated_ci_events.py`, `backfill_buy_now_events.py`, and their variant-level counterparts |
| Variant-wise events | `backfill_variant_wise_events_summary.py` |
| Sessions & web vitals | `backfill_hourly_sessions.py`, `backfill_web_vitals_summary.py`, `backfill_daily_web_vitals_summary.py` |
| UTM & traffic | `utm_sessions_backfill.py`, `referrer_sessions_backfill.py`, `backfill_product_traffic_split.py` |
| Bundles | `bundle_rollup_backfill.py` |
| Payment mode breakdown | `backfill_payment_mode_breakdown_columns.py` — cod/prepaid/ppcod order counts and sales amounts only, on `hour_wise_sales` and `overall_summary` |

Each script is run directly with Python and reads the same brand configuration as the main pipeline.
