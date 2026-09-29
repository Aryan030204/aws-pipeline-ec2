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
| `pipeline/schema_migrations.py` | Table creation / schema upkeep run inline by the pipeline |
| `pipeline/sql_migrations.py` | Applies `migrations/*.sql` to each brand database once, at container start |
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
| `SELF_RUN` | `true` = auto-run every 10 minutes; `false` = manual trigger only |
| `BACKFILL_MODE` | `true` = process a specific historical date window instead of "since last run" |
| `BACKFILL_BRAND_INDICES` | Restrict a backfill run to specific brands |
| `ALERTS_WEBHOOK_URL` | Where QStash-forwarded sales/web-vitals alerts get delivered |

No secrets or real credentials are ever meant to live in source control — see `.env.local`/`.env` locally, and the deployment's own secret store in production.

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
| `/trigger` | POST | Runs the pipeline immediately. Requires an `x-pipeline-key` header and a JSON body with a `runId` |

## Deployment

Runs as a one-shot migration task plus six long-running Docker services (see `docker-compose.yml`):

- **`db-migrate`** — a one-shot container that applies `migrations/*.sql` to **every brand database**, once each, and then exits. Every other service declares `depends_on: db-migrate: condition: service_completed_successfully`, so nothing starts until migrations have finished cleanly and no worker ever reads a table a migration has not created yet. See [Database migrations](#database-migrations).
- **`shopify-pipeline`** — the main pipeline described above.
- **`events-summary-worker`** — a separate container that rolls up daily event totals across all brands into one summary table, on its own daily schedule.
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

### Cache invalidation for rewritten dates

The dashboard serves day-level metrics out of Redis (`metrics:<brand>:<date>`,
`hourly_metrics:<brand>:<date>`, `date_presets:<brand>`) and only falls back to
MySQL on a miss, so a write that lands outside the cache pipeline's rebuild
window - a backfill, or a manual worker trigger for an older range - would leave
the dashboard showing the pre-write numbers for those dates even though the
tables are correct.

`pipeline/cache_invalidation.py` closes that gap: whenever a run rewrites rows
for a set of dates it deletes exactly those brand+date keys, so the next
dashboard read misses and goes to MySQL, and the cache pipeline refills them on
its next run. Deleting is always safe - worst case is one uncached read. It is
wired into the two paths that write dated rows:

| Caller | Dates invalidated |
| --- | --- |
| `pipeline/orchestration.py`, right after `execute_summary_queries_incremental` | the run's affected + RTO-affected dates |
| `workers/pnl_worker.py`, after `overall_pnl` is upserted | that rebuild's target dates |

It needs `REDIS_URL` (the same instance the dashboard's analytics service
reads; already in `.env`) and the `redis` package. With either missing it logs
once and turns itself off, and any Redis error is logged and swallowed - a cache
that can't be reached never fails a run that has already written its rows.
`TEST_MODE=true` skips it entirely.

### Worker admission control

Every worker runs in its own container on one EC2 box, and several wake together
(`events-summary` and `variant-wise-events` both at 18:30 UTC, near the main
pipeline's `:30` slot and `intent-events`' 15-minute interval). Overlapping runs
spike memory, the host swaps onto its EBS root volume, and the random I/O that
follows drags the whole instance down.

`workers/admission.py` caps how many worker runs may be in flight at once,
across containers, using one Redis sorted set (`workers:running`). Each worker
declares itself with a single decorator on `run_worker`:

```python
@admission_controlled("pnl")
def run_worker(current_dt=None, brand_key=None):
```

`run_worker` is the one function every entry path reaches - the worker's own
scheduler, `python -m workers.run <name>`, and `POST /workers/<name>/trigger` -
so gating it covers all three, including the ad-hoc backfills most likely to
collide with a scheduled job.

Admission is a single Lua script, so two workers starting in the same second
cannot both see a free slot: it evicts expired leases, counts what is left, and
admits only if `running + 1 <= MAX_CONCURRENT_WORKERS`. Slots are **leases, not
a counter** - the failure being guarded against is the OOM killer, and a
decrement that never runs would leak the count upward until no worker could
start at all. A killed container's slot frees itself once its lease expires; a
heartbeat thread pushes the lease forward while a long run is still alive.

Like `pipeline/cache_invalidation.py` it fails open: missing `REDIS_URL`, a
missing `redis` package, or an unreachable Redis logs and lets the run proceed
unguarded, because a gate that can't be reached must not stop work that is
otherwise fine. `TEST_MODE=true` skips it entirely. The one case that does stop
a run is waiting past `WORKER_ADMISSION_WAIT_S`, which means something is
genuinely stuck - the worker raises `AdmissionTimeout` rather than piling onto a
host that is already struggling.

| Variable | Default | Purpose |
|---|---|---|
| `MAX_CONCURRENT_WORKERS` | `2` | How many worker runs may overlap |
| `WORKER_LEASE_TTL_S` | `300` | How long a slot is held without a heartbeat |
| `WORKER_ADMISSION_WAIT_S` | `1800` | How long to wait for a slot before giving up |
| `WORKER_ADMISSION_POLL_S` | `10` | How often to re-check while waiting |
| `WORKER_ADMISSION_KEY` | `workers:running` | The sorted set the containers coordinate through |

To see what is holding a slot right now:

```bash
docker compose exec -T shopify-pipeline \
  python -c "from workers.admission import current_running; print(current_running())"
```

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

## Database migrations

`migrations/*.sql` are versioned, per-brand schema changes. The `db-migrate` container applies them
to every brand database at startup and exits; the other containers wait for it to exit 0.

Each brand database records what it has already had applied in its own `schema_migrations` table, so
a restart with no new files just connects once per brand and exits. Concurrent starts are safe: the
runner takes a MySQL advisory lock per brand database, so if several containers come up at once, one
migrates and the rest wait rather than racing into duplicate-key errors.

| Command | What it does |
| --- | --- |
| `python -m pipeline.sql_migrations` | Apply every pending migration to every active brand |
| `python -m pipeline.sql_migrations --status` | Show, per brand, what is applied and what is pending |
| `python -m pipeline.sql_migrations --dry-run` | List what would run, without running it |
| `python -m pipeline.sql_migrations --brand 3` | Restrict the run to one brand index |

| Env var | Effect |
| --- | --- |
| `MIGRATIONS_ALLOW_FAILURE` | `true` = exit 0 and let the stack boot even when a brand fails to migrate (default `false`, which blocks every dependent container) |
| `MIGRATIONS_LOCK_TIMEOUT_S` | Seconds to wait for another container's migration lock (default 300) |
| `MIGRATIONS_STRICT_CHECKSUMS` | `true` = fail when an already-applied file has been edited (default is to warn) |

### Writing a new migration

Name it `NNN_short_description.sql` with the next free number. It must be **individually
re-runnable**, because a brand whose run dies halfway through re-applies the whole file next start:

- `CREATE TABLE IF NOT EXISTS`, `DROP ... IF EXISTS`.
- **No top-level `ALTER TABLE`.** MySQL has no `IF NOT EXISTS` for it, so it is the usual way a
  migration stops being re-runnable — `ADD COLUMN` fails with errno 1060 the second time, `ADD
  PRIMARY KEY` with 1068, `DROP INDEX` with 1091, and any of them with 1146 on a brand whose table
  does not exist yet. Put the change in a stored procedure that checks `information_schema` first;
  `003_overall_pnl.sql` is the shortest example, `002_converge_to_canonical.sql` the fullest.
- Assume brand databases differ. They are provisioned incrementally, so guard for a missing table
  *and* a missing column rather than assuming the schema matches your local one.
- Never edit a migration that has already been applied — the runner records a checksum and will warn
  that the file changed and refuse to re-run it. Add a new file instead.

`test_sql_migrations.py` enforces the rules above over everything in `migrations/`, so a file that
breaks them fails the test suite rather than a production boot.

A migration that genuinely cannot be automated (needs a maintenance window, or a human reading
pre-flight output) carries `-- migrate: manual` in its header; the runner lists it and skips it.
`001_rds_perf_pass.sql` is the one such file — it is superseded by `002_converge_to_canonical.sql`,
which does the same work through guarded procedures.

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
