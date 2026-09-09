"""
One-off script: builds docs/Pipeline-Optimizations.pdf, a summary of the
performance-relevant optimizations in this codebase (database-side and
code-organization). Not part of the pipeline itself - re-run manually if
the content changes.
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
    Table,
    TableStyle,
    ListFlowable,
    ListItem,
)

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_PATH = os.path.join(HERE, "Pipeline-Optimizations.pdf")

NAVY = colors.HexColor("#1E293B")
GRAY = colors.HexColor("#475569")
BLUE = colors.HexColor("#2563EB")
HEADER_BG = colors.HexColor("#DBEAFE")
AMBER_BG = colors.HexColor("#FEF3C7")
AMBER_BORDER = colors.HexColor("#D97706")

styles = getSampleStyleSheet()
styles.add(ParagraphStyle("DocTitle", parent=styles["Title"], textColor=NAVY, fontSize=26, spaceAfter=6))
styles.add(ParagraphStyle("Subtitle", parent=styles["Normal"], textColor=GRAY, fontSize=11, italic=True, spaceAfter=18))
styles.add(ParagraphStyle("H1", parent=styles["Heading1"], textColor=NAVY, fontSize=16, spaceBefore=18, spaceAfter=8))
styles.add(ParagraphStyle("H2", parent=styles["Heading2"], textColor=BLUE, fontSize=12.5, spaceBefore=12, spaceAfter=4))
styles.add(ParagraphStyle("Body", parent=styles["Normal"], fontSize=10.5, leading=15, spaceAfter=8))
styles.add(ParagraphStyle("BodySmall", parent=styles["Normal"], fontSize=9.5, leading=13, textColor=GRAY, spaceAfter=6))
styles.add(ParagraphStyle("CodeBlock", parent=styles["Normal"], fontName="Courier", fontSize=9.5, leading=13,
                          backColor=colors.HexColor("#F1F5F9"), borderPadding=6, spaceAfter=8))
styles.add(ParagraphStyle("Callout", parent=styles["Normal"], fontSize=10, leading=14,
                          backColor=AMBER_BG, borderPadding=8, borderColor=AMBER_BORDER, borderWidth=1, spaceAfter=8))

story = []


def title(text, subtitle=None):
    story.append(Paragraph(text, styles["DocTitle"]))
    if subtitle:
        story.append(Paragraph(subtitle, styles["Subtitle"]))


def h1(text):
    story.append(Paragraph(text, styles["H1"]))


def h2(text):
    story.append(Paragraph(text, styles["H2"]))


def body(text):
    story.append(Paragraph(text, styles["Body"]))


def body_small(text):
    story.append(Paragraph(text, styles["BodySmall"]))


def code(text):
    story.append(Paragraph(text.replace("\n", "<br/>"), styles["CodeBlock"]))


def callout(text):
    story.append(Paragraph(text, styles["Callout"]))


def bullets(items, style="Body"):
    story.append(
        ListFlowable(
            [ListItem(Paragraph(i, styles[style])) for i in items],
            bulletType="bullet",
            leftIndent=18,
        )
    )
    story.append(Spacer(1, 6))


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
        "Shopify Pipeline — Optimizations",
        "A record of the performance-relevant changes in this codebase: database-side "
        "schema/index/write-pattern work, and the one code-organization change with a "
        "real (if modest) performance effect.",
    )

    h1("1. Database-side optimizations")
    body(
        "These come from <b>migrations/001_rds_perf_pass.sql</b> and the query/connection "
        "patterns in <b>pipeline/db.py</b> and <b>pipeline/summary_incremental.py</b>. The "
        "migration's stated goal was to stop RDS <b>EBSByteBalance%</b> (I/O credit balance) "
        "from hitting zero on the per-brand MySQL databases."
    )

    h2("1.1 Primary keys added to the summary tables")
    body(
        "overall_summary, sales_summary, gross_summary, discount_summary, and order_summary "
        "each got a primary key on <b>date</b>; hour_wise_sales got a composite primary key on "
        "<b>(date, hour)</b> — applied to both the live tables and their _stage counterparts. "
        "Before this, none of these tables had an index on date at all, so every read "
        "(the pipeline's own merge logic, and the dashboard) was a full table scan."
    )

    h2("1.2 Dropped redundant/duplicate indexes on shopify_orders / shopify_orders_update")
    body(
        "shopify_orders went from 18 indexes down to 10: dropped varchar-created_date "
        "duplicates of proper indexes, an exact-duplicate composite index, and two "
        "typo'd/shadowed product-level composites. shopify_orders_update had three similarly "
        "redundant indexes dropped. Every INSERT/UPDATE maintains every index on the table, "
        "so this was identified as the single biggest driver of the EBSByteBalance% problem."
    )

    h2("1.3 Batched / chunked bulk writes")
    body(
        "pipeline/db.py's executemany_chunked runs bulk INSERT ... ON DUPLICATE KEY UPDATE "
        "in bounded chunks (EXECUTEMANY_CHUNK_SIZE, default 1000 rows), committing between "
        "chunks. One unbounded executemany() over a huge row list becomes one giant "
        "transaction holding row locks and growing the redo log for the entire batch — "
        "chunking gives MySQL room to flush between commits."
    )

    h2("1.4 Staging-table merge instead of per-date DELETE + INSERT")
    body(
        "merge_staging_to_main_by_date batches affected dates (5 at a time) into a single "
        "range DELETE plus a range INSERT per chunk. Its own docstring documents the "
        "before/after: the prior version did a DELETE and an INSERT per individual date in "
        "a loop — 2N round-trips and N index-scan deletes for N affected dates."
    )

    h2("1.5 Incremental-only summary rebuilds")
    body(
        "The entire update_*_incremental family in pipeline/summary_incremental.py "
        "recomputes rows only for the dates actually affected by new or changed data on "
        "each cycle — never a full-table rebuild on a normal run."
    )

    h2("1.6 Connection pooling: NullPool + pool_pre_ping")
    body(
        "The SQLAlchemy engines (one per brand, aws_background.py) use NullPool with "
        "pool_pre_ping=True — no idle pooled connections held open against RDS between "
        "uses, and pool_pre_ping avoids handing out a dead connection after a failover or "
        "idle timeout."
    )

    h2("1.7 Concurrency cap on per-brand processing")
    body(
        "MAX_CONCURRENT_BRANDS (default 3, pipeline/orchestration.py) deliberately limits "
        "how many brands' ETL passes — each a heavy set of multi-table JOINs and summary "
        "rebuilds — run at once, specifically to protect shared RDS I/O rather than firing "
        "every brand in parallel."
    )

    h2("1.8 Connection retry and SSL-fallback")
    body(
        "get_db_connection retries with exponential backoff plus jitter, and automatically "
        "falls back to a non-SSL connection on an SSL-specific failure. This is a "
        "reliability measure more than a throughput one, but it avoids reconnect storms "
        "under transient failure."
    )

    h1("2. Code-organization optimization")
    h2("2.1 Dead code removal (pre-modularization step)")
    body(
        "Before aws_background.py was split into the pipeline/ package, a first pass "
        "(commit 19c4fca) removed a fully-shadowed duplicate block of the checkout-initiated/"
        "buy-now Mongo sync functions — an entire first definition that was completely "
        "overwritten by later redefinitions of the same names and never executed — plus "
        "roughly ten other confirmed-unused functions and the now-unused smtplib / "
        "email.mime / multiprocessing imports."
    )
    body_small(
        "Note: the module split itself (aws_background.py → pipeline/*.py) was explicitly "
        "NOT a performance change. Every step moved code verbatim behind a facade "
        "re-export, with object-identity checks confirming shared singletons (DB engine "
        "pool, HTTP session, scheduler, Flask app) stayed the same objects across modules "
        "rather than being duplicated. It's a maintainability refactor with a "
        "zero-behavior-change guarantee, not an optimization pass."
    )

    h1("3. Open finding — verify before relying on it")
    callout(
        "<b>migrations/001_rds_perf_pass.sql</b> also claims the pipeline was \"patched to "
        "use created_dt / created_hr (generated, indexed columns) instead of VARCHAR "
        "created_date + HOUR(created_time)\", and drops the old varchar-based indexes on "
        "that assumption. This does <b>not</b> match the current pipeline/ package: "
        "pipeline/summary_incremental.py still queries via "
        "STR_TO_DATE(created_date, '%Y-%m-%d') and HOUR(created_time) everywhere, including "
        "the payment-mode-breakdown queries added most recently. The created_dt/created_hr "
        "pattern only exists in backfill scripts/aws_data_update.py, a separate standalone "
        "script — not the live pipeline. If Phase 2/2b's index drops from that migration "
        "were ever actually run against a brand's production database, the pipeline's real "
        "queries may now be missing index support they used to have. Worth an EXPLAIN check "
        "on the live hour_wise_sales / order_summary queries against a production brand "
        "database."
    )

    doc = SimpleDocTemplate(
        OUT_PATH,
        pagesize=LETTER,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
        title="Shopify Pipeline Optimizations",
    )
    doc.build(story)
    print("Wrote", OUT_PATH)


if __name__ == "__main__":
    build()
