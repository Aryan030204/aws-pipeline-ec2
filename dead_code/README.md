# dead_code/

Code that's no longer part of the live pipeline, kept for reference instead
of deleted outright.

## intent-events-worker

Ingested from the separate Intent MongoDB cluster (behavioral/click/session
events) into each brand's `behavioral_events`/`click_events`/`intent_sessions`
tables. Now runs as its own service, outside this repo. Removed from the
active set (not in `docker-compose.yml`, not imported by `aws_background.py`,
not registered in `workers/registry.py`) as of the `mongo_events_summary`
consolidation that moved the CI/buy-now event workers onto a single ingest
point - see that change's summary for context.

Files, with their original paths:
- `workers/intent_events_worker.py`
- `pipeline/intent_events.py`
- `Dockerfile.intent-events-worker`
- `archived_test_intent_events.py` (was `test_intent_events.py`)
- `archived_test_intent_events_worker.py` (was `test_intent_events_worker.py`)

The two test files are renamed so `python -m unittest discover -p "test_*.py"`
doesn't pick them up - they'd fail to import anyway, since their internal
`from pipeline.intent_events import ...` / `from workers import
intent_events_worker` references the original (now moved) paths, not updated
to point here. Restoring this worker means moving all five files back to
their original paths and re-adding the `docker-compose.yml` service,
`aws_background.py` facade import, and `workers/registry.py` entry.
