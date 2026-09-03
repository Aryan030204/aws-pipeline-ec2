"""
One-off script: builds docs/Pipeline-Documentation.pdf from the same
content as README.md / the docx. Not part of the pipeline itself -
re-run manually if the documentation content changes.
"""

import os

from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Image,
    Table,
    TableStyle,
    ListFlowable,
    ListItem,
)

HERE = os.path.dirname(os.path.abspath(__file__))
IMG_DIR = os.path.join(HERE, "images")
OUT_PATH = os.path.join(HERE, "Pipeline-Documentation.pdf")

NAVY = colors.HexColor("#1E293B")
GRAY = colors.HexColor("#475569")
BLUE = colors.HexColor("#2563EB")
HEADER_BG = colors.HexColor("#DBEAFE")

styles = getSampleStyleSheet()
styles.add(ParagraphStyle("DocTitle", parent=styles["Title"], textColor=NAVY, fontSize=26, spaceAfter=6))
styles.add(ParagraphStyle("Subtitle", parent=styles["Normal"], textColor=GRAY, fontSize=11, italic=True, spaceAfter=18))
styles.add(ParagraphStyle("H1", parent=styles["Heading1"], textColor=NAVY, fontSize=16, spaceBefore=18, spaceAfter=8))
styles.add(ParagraphStyle("Body", parent=styles["Normal"], fontSize=10.5, leading=15, spaceAfter=8))
styles.add(ParagraphStyle("CodeBlock", parent=styles["Normal"], fontName="Courier", fontSize=9.5, leading=13,
                          backColor=colors.HexColor("#F1F5F9"), borderPadding=6, spaceAfter=8))

story = []


def title(text, subtitle=None):
    story.append(Paragraph(text, styles["DocTitle"]))
    if subtitle:
        story.append(Paragraph(subtitle, styles["Subtitle"]))


def h1(text):
    story.append(Paragraph(text, styles["H1"]))


def body(text):
    story.append(Paragraph(text, styles["Body"]))


def code(text):
    story.append(Paragraph(text.replace("\n", "<br/>"), styles["CodeBlock"]))


def bullets(items):
    story.append(
        ListFlowable(
            [ListItem(Paragraph(i, styles["Body"])) for i in items],
            bulletType="bullet",
            leftIndent=18,
        )
    )
    story.append(Spacer(1, 6))


def image(filename, width_in=6.3):
    path = os.path.join(IMG_DIR, filename)
    from PIL import Image as PILImage
    w, h = PILImage.open(path).size
    ratio = h / w
    story.append(Image(path, width=width_in * inch, height=width_in * ratio * inch))
    story.append(Spacer(1, 10))


def table(headers, rows, col_widths=None):
    data = [headers] + [list(r) for r in rows]
    wrapped = [
        [Paragraph(f"<b>{c}</b>", styles["Body"]) for c in data[0]]
    ] + [[Paragraph(c, styles["Body"]) for c in r] for r in data[1:]]
    t = Table(wrapped, colWidths=col_widths, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), HEADER_BG),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CBD5E1")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 12))


def build():
    title(
        "Shopify Pipeline",
        "A multi-brand Shopify data pipeline &mdash; orders, sessions, checkout events, "
        "page speed, and dashboard summaries, kept up to date automatically.",
    )

    h1("What it does")
    body(
        "For every brand it manages, the pipeline pulls in new data, keeps that brand's "
        "own MySQL database up to date, and rebuilds dashboard tables &mdash; only for the "
        "parts that actually changed. Specifically, it:"
    )
    bullets(
        [
            "Fetches new and updated Shopify orders into that brand's own MySQL database.",
            "Pulls session and traffic data via ShopifyQL (Shopify's analytics query language).",
            "Syncs checkout-initiated and buy-now events from MongoDB.",
            "Syncs page-speed (“web vitals”) data from MongoDB.",
            "Rebuilds dashboard summary tables incrementally, only for changed dates &mdash; "
            "including a full payment-mode breakdown (COD, prepaid, partially-prepaid) for both "
            "order counts and sales amounts.",
            "Tracks any checkout/session event type per product variant, not just "
            "checkout-initiated and buy-now &mdash; new event types are discovered automatically "
            "and get their own column, via a separate daily worker.",
            "Rolls up “bundle” product sales (TMC and PTS today) &mdash; bundle products are "
            "discovered automatically from the Shopify product catalog (active products only), "
            "not a hardcoded list.",
            "Sends live alerts to QStash: sales/session numbers and page-speed averages, per brand.",
            "Triggers the downstream cache pipeline directly after each run finishes, so "
            "dashboards pick up fresh data.",
        ]
    )
    body(
        "Each brand is fully isolated &mdash; its own database, its own Shopify store, its own "
        "failure boundary. If one brand's run fails, the others still complete."
    )

    h1("Architecture")
    image("architecture.png")

    h1("How it runs")
    bullets(
        [
            "Automatically, every 10 minutes, via an internal scheduler (APScheduler).",
            "On demand, via POST /trigger (manual runs, or an external pipeline manager).",
            "Packaged as a Docker container, deployed on an EC2 instance.",
        ]
    )

    h1("Project structure")
    body(
        "The pipeline used to be a single ~10,000-line file. It's now split into focused "
        "modules under pipeline/, all tied together through aws_background.py, which "
        "remains the entrypoint."
    )
    image("project-structure.png")
    table(
        ["Module", "Responsibility"],
        [
            ("aws_background.py", "Entrypoint, brand configuration, and the facade that re-exports everything below"),
            ("pipeline/state.py", "Shared config and long-lived objects (logger, Flask app, scheduler, brand registries)"),
            ("pipeline/db.py", "Database connections, retries, and cleanup"),
            ("pipeline/schema_migrations.py", "Table creation / schema upkeep"),
            ("pipeline/shopify_orders.py", "Order fetching, transforming, and loading"),
            ("pipeline/summary_incremental.py", "Rebuilding dashboard summary tables"),
            ("pipeline/mongo_events.py", "Checkout-initiated / buy-now event sync"),
            ("pipeline/web_vitals.py", "Page-speed data sync"),
            ("pipeline/shopifyql_sessions.py", "Session and city-level traffic analytics"),
            ("pipeline/utm_traffic.py", "UTM and referrer attribution"),
            ("pipeline/notifications.py", "QStash alerts and webhook triggers"),
            ("pipeline/orchestration.py", "Runs everything above, per brand, per pipeline tick"),
            ("bundle_rollups.py", "Bundle product discovery (Shopify Admin API, active products only) and sales rollups (TMC, PTS)"),
            ("workers/events_summary_worker.py", "Separate worker rolling up daily event totals, own Docker container"),
            ("workers/variant_wise_events_worker.py", "Separate worker rolling up any Mongo event type per product variant, own Docker container"),
            ("backfill scripts/", "Standalone scripts to re-run specific pieces over a historical date range"),
        ],
        col_widths=[2.0 * inch, 4.3 * inch],
    )

    h1("Configuration")
    body(
        "Brands are configured through numbered environment variables &mdash; one full set "
        "per brand index, e.g. BRAND_NAME_0, BRAND_TAG_0, DB_HOST_0, SHOP_NAME_0, "
        "ACCESS_TOKEN_0. In production, brand configuration is instead pulled from an "
        "internal brand-config API. Local/dev runs use TEST_MODE=true, which loads brand "
        "config from .env.local instead and disables outbound QStash calls."
    )
    table(
        ["Variable", "Purpose"],
        [
            ("SELF_RUN", "true = auto-run every 10 minutes; false = manual trigger only"),
            ("BACKFILL_MODE", "true = process a specific historical date window instead of “since last run”"),
            ("BACKFILL_BRAND_INDICES", "Restrict a backfill run to specific brands"),
            ("ALERTS_WEBHOOK_URL", "Where QStash-forwarded sales/web-vitals alerts get delivered"),
        ],
        col_widths=[2.0 * inch, 4.3 * inch],
    )
    body(
        "No secrets or real credentials are ever meant to live in source control &mdash; "
        "see .env.local / .env locally, and the deployment's own secret store in production."
    )

    h1("Running it locally")
    code("pip install -r requirements.txt\npython aws_background.py")
    body(
        "With TEST_MODE=true, this loads brand config from .env.local, starts the "
        "scheduler, and serves the API on port 5000."
    )
    code("curl http://localhost:5000/health")
    code(
        "curl -X POST http://localhost:5000/trigger \\\n"
        "  -H \"x-pipeline-key: &lt;PIPELINE_AUTH_HEADER&gt;\" \\\n"
        "  -d '{\"runId\": \"manual-test-1\"}'"
    )

    h1("API endpoints")
    table(
        ["Endpoint", "Method", "Purpose"],
        [
            ("/health", "GET", "Basic liveness check"),
            ("/trigger", "POST", "Runs the pipeline immediately (requires x-pipeline-key + runId)"),
        ],
        col_widths=[1.3 * inch, 1.0 * inch, 4.0 * inch],
    )

    h1("Deployment")
    body(
        "Runs as three Docker services (see docker-compose.yml): shopify-pipeline (the main "
        "pipeline), events-summary-worker (rolls up daily event totals across all brands, on "
        "its own daily schedule), and variant-wise-events-worker (rolls up any Mongo event "
        "type per product variant, into each brand's own database, on its own daily schedule)."
    )
    code(
        "git pull origin master\n"
        "docker compose build shopify-pipeline\n"
        "docker compose up -d shopify-pipeline"
    )

    h1("Cache pipeline trigger")
    body(
        "The cache pipeline runs as its own container (cache-pipeline) on the same EC2 host, "
        "joined to the same Docker network (pipeline-net) as shopify-pipeline. After each full "
        "run finishes, the pipeline fires a plain GET to http://cache-pipeline:8080/trigger-pipeline "
        "&mdash; no QStash involved, just a direct container-to-container call over the shared "
        "network."
    )
    body(
        "This request runs in a background thread, so the pipeline doesn't wait for the cache "
        "rebuild to finish before moving on. Whenever the response does come back, only a short "
        "summary (status + brand count) is logged &mdash; not the full per-brand/per-date result "
        "body."
    )

    h1("Testing")
    code('python -m unittest discover -p "test_*.py"')
    body(
        "Covers brand-activation filtering, bundle rollup logic, UTM source mapping, "
        "and the events summary worker."
    )

    h1("Backfill scripts")
    body(
        "backfill scripts/ holds standalone tools for re-running one specific piece of "
        "the pipeline over a chosen date range &mdash; useful for fixing a gap in "
        "historical data without re-running everything. Each script is run directly with "
        "Python and reads the same brand configuration as the main pipeline."
    )
    table(
        ["Category", "Examples"],
        [
            ("Orders & addresses", "backfill_shopify.py, backfill_shopify_order_addresses.py, backfill_shopify_order_tags.py"),
            ("Returns / RTO", "backfill_returns_fact_order_created_date.py, backfill_rto_summary.py"),
            ("Checkout / buy-now events", "backfill_checkout_initiated_ci_events.py, backfill_buy_now_events.py (+ variant-level)"),
            ("Variant-wise events", "backfill_variant_wise_events_summary.py"),
            ("Sessions & web vitals", "backfill_hourly_sessions.py, backfill_web_vitals_summary.py, backfill_daily_web_vitals_summary.py"),
            ("UTM & traffic", "utm_sessions_backfill.py, referrer_sessions_backfill.py, backfill_product_traffic_split.py"),
            ("Bundles", "bundle_rollup_backfill.py"),
            ("Payment mode breakdown", "backfill_payment_mode_breakdown_columns.py &mdash; cod/prepaid/ppcod orders &amp; sales only, on hour_wise_sales and overall_summary"),
        ],
        col_widths=[1.7 * inch, 4.6 * inch],
    )

    doc = SimpleDocTemplate(
        OUT_PATH,
        pagesize=LETTER,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
        title="Shopify Pipeline Documentation",
    )
    doc.build(story)
    print("Wrote", OUT_PATH)


if __name__ == "__main__":
    build()
