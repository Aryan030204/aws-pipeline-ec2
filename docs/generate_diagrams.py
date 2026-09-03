"""
One-off script: generates the two documentation diagrams as PNGs under
docs/images/. Not part of the pipeline itself - a documentation-authoring
tool. Re-run manually if the diagrams need to change.
"""

from PIL import Image, ImageDraw, ImageFont

FONT_DIR = "C:/Windows/Fonts"
FONT_BOLD = f"{FONT_DIR}/arialbd.ttf"
FONT_REGULAR = f"{FONT_DIR}/arial.ttf"

# Palette (flat, readable in both light and dark PDF/DOCX viewers)
NAVY = (30, 41, 59)
BLUE = (37, 99, 235)
BLUE_LIGHT = (219, 234, 254)
GREEN = (5, 150, 105)
GREEN_LIGHT = (209, 250, 229)
AMBER = (217, 119, 6)
AMBER_LIGHT = (254, 243, 199)
GRAY = (100, 116, 139)
GRAY_LIGHT = (241, 245, 249)
WHITE = (255, 255, 255)


def font(size, bold=False):
    return ImageFont.truetype(FONT_BOLD if bold else FONT_REGULAR, size)


def text_wh(draw, text, f):
    bbox = draw.textbbox((0, 0), text, font=f)
    return bbox[2] - bbox[0], bbox[3] - bbox[1]


def draw_box(draw, xy, label, fill, outline, text_color=NAVY, sub=None, f_title=None, f_sub=None):
    x0, y0, x1, y1 = xy
    draw.rounded_rectangle(xy, radius=10, fill=fill, outline=outline, width=2)
    f_title = f_title or font(18, bold=True)
    tw, th = text_wh(draw, label, f_title)
    cx, cy = (x0 + x1) / 2, (y0 + y1) / 2
    if sub:
        f_sub = f_sub or font(13)
        sw, sh = text_wh(draw, sub, f_sub)
        draw.text((cx - tw / 2, cy - th - 2), label, font=f_title, fill=text_color)
        draw.text((cx - sw / 2, cy + 6), sub, font=f_sub, fill=GRAY)
    else:
        draw.text((cx - tw / 2, cy - th / 2), label, font=f_title, fill=text_color)


def arrow(draw, x0, y0, x1, y1, color=GRAY, width=3):
    draw.line((x0, y0, x1, y1), fill=color, width=width)
    import math
    angle = math.atan2(y1 - y0, x1 - x0)
    size = 10
    for da in (0.5, -0.5):
        ax = x1 - size * math.cos(angle - da)
        ay = y1 - size * math.sin(angle - da)
        draw.line((x1, y1, ax, ay), fill=color, width=width)


def draw_architecture_diagram(path):
    W, H = 1200, 660
    img = Image.new("RGB", (W, H), WHITE)
    d = ImageDraw.Draw(img)

    f_title = font(20, bold=True)
    f_sub = font(13)
    f_h1 = font(24, bold=True)

    d.text((40, 30), "Architecture — how data flows through the pipeline", font=f_h1, fill=NAVY)

    # Sources (left column)
    src_x0, src_w = 40, 260
    shopify_box = (src_x0, 130, src_x0 + src_w, 220)
    mongo_box = (src_x0, 250, src_x0 + src_w, 340)
    draw_box(d, shopify_box, "Shopify", BLUE_LIGHT, BLUE, sub="Admin REST API + ShopifyQL", f_title=f_title, f_sub=f_sub)
    draw_box(d, mongo_box, "MongoDB", GREEN_LIGHT, GREEN, sub="checkout / buy-now / other events\nweb vitals", f_title=f_title, f_sub=f_sub)

    # Pipeline (center)
    pipe_x0 = 420
    pipe_box = (pipe_x0, 150, pipe_x0 + 340, 340)
    draw_box(d, pipe_box, "Shopify Pipeline", AMBER_LIGHT, AMBER, sub="Docker container on EC2\nevery 10 min or on-demand", f_title=f_title, f_sub=f_sub)

    # Outputs (right column)
    out_x0, out_w = 850, 300
    mysql_box = (out_x0, 90, out_x0 + out_w, 185)
    qstash_box = (out_x0, 215, out_x0 + out_w, 310)
    cache_box = (out_x0, 340, out_x0 + out_w, 435)
    draw_box(d, mysql_box, "Per-brand MySQL DBs", BLUE_LIGHT, BLUE, sub="orders, summaries, rollups", f_title=f_title, f_sub=f_sub)
    draw_box(d, qstash_box, "QStash", GREEN_LIGHT, GREEN, sub="sales + web-vitals alerts", f_title=f_title, f_sub=f_sub)
    draw_box(d, cache_box, "Cache Pipeline", GRAY_LIGHT, GRAY, sub="direct GET, same Docker network\n(no longer via QStash)", f_title=f_title, f_sub=f_sub)

    # Arrows in
    arrow(d, src_x0 + src_w, 175, pipe_x0, 200)
    arrow(d, src_x0 + src_w, 295, pipe_x0, 260)
    # Arrows out
    arrow(d, pipe_x0 + 340, 190, out_x0, 137)
    arrow(d, pipe_x0 + 340, 260, out_x0, 262)
    arrow(d, pipe_x0 + 340, 320, out_x0, 387)

    # Bottom strip: brands
    d.text((40, 480), "Each brand (TMC, PTS, Ajmal, BBB, Neulife, Vama, ...) has its own isolated database and Shopify store,",
           font=f_sub, fill=GRAY)
    d.text((40, 502), "processed independently every pipeline run — one brand's failure never blocks another.",
           font=f_sub, fill=GRAY)

    # Trigger note
    d.text((40, 560), "Triggered by: APScheduler cron (:00 :10 :20 :30 :40 :50) — or POST /trigger for an immediate manual run.",
           font=f_sub, fill=GRAY)
    d.text((40, 582), "After each run completes, the pipeline fires a background trigger to Cache Pipeline and does not wait for it to finish.",
           font=f_sub, fill=GRAY)

    img.save(path)


def draw_project_structure_diagram(path):
    W, H = 1200, 600
    img = Image.new("RGB", (W, H), WHITE)
    d = ImageDraw.Draw(img)

    f_h1 = font(24, bold=True)
    f_title = font(16, bold=True)
    f_sub = font(12)

    d.text((40, 30), "Project structure — aws_background.py + the pipeline/ package", font=f_h1, fill=NAVY)

    # Top: facade
    top_box = (40, 90, 1160, 160)
    draw_box(d, top_box, "aws_background.py", AMBER_LIGHT, AMBER,
             sub="entrypoint • brand config • re-exports everything below", f_title=font(20, bold=True), f_sub=f_sub)

    modules = [
        ("state.py", "shared config & singletons"),
        ("db.py", "DB connections"),
        ("schema_migrations.py", "table DDL"),
        ("shopify_orders.py", "order fetch/load"),
        ("summary_incremental.py", "dashboard summaries"),
        ("mongo_events.py", "checkout/buy-now events"),
        ("web_vitals.py", "page speed sync"),
        ("shopifyql_sessions.py", "session analytics"),
        ("utm_traffic.py", "UTM & referrers"),
        ("notifications.py", "QStash / webhooks"),
        ("orchestration.py", "runs it all together"),
    ]

    cols = 4
    box_w, box_h = 270, 92
    gap_x, gap_y = 20, 20
    start_x, start_y = 40, 230

    # Single horizontal "spine" connecting the facade to the top row, plus
    # short vertical stubs per column - avoids lines running through boxes.
    spine_y = (top_box[3] + start_y) / 2
    col_centers = [start_x + c * (box_w + gap_x) + box_w / 2 for c in range(cols)]
    d.line((col_centers[0], spine_y, col_centers[-1], spine_y), fill=GRAY_LIGHT, width=2)
    for cx in col_centers:
        d.line((cx, top_box[3], cx, spine_y), fill=GRAY_LIGHT, width=2)
        d.line((cx, spine_y, cx, start_y), fill=GRAY_LIGHT, width=2)

    for i, (name, desc) in enumerate(modules):
        row, col = divmod(i, cols)
        x0 = start_x + col * (box_w + gap_x)
        y0 = start_y + row * (box_h + gap_y)
        box = (x0, y0, x0 + box_w, y0 + box_h)
        draw_box(d, box, name, BLUE_LIGHT, BLUE, sub=desc, f_title=f_title, f_sub=f_sub)

    d.text((40, H - 40), "Every module is re-exported through aws_background.py, so existing scripts keep working unchanged.",
           font=f_sub, fill=GRAY)

    img.save(path)


if __name__ == "__main__":
    import os
    out_dir = os.path.join(os.path.dirname(__file__), "images")
    os.makedirs(out_dir, exist_ok=True)
    draw_architecture_diagram(os.path.join(out_dir, "architecture.png"))
    draw_project_structure_diagram(os.path.join(out_dir, "project-structure.png"))
    print("Diagrams written to", out_dir)
