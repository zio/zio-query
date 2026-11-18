"""Build the ZIO Query brand kit: SVG sources, PNG exports, social cards and brand-sheet boards.

Usage (fontTools and uharfbuzz are the Python dependencies; Chromium does the rasterising):
    uv run --with fonttools --with uharfbuzz python tools/build_brand.py \
        <Sora[wght].ttf> <JetBrainsMono[wght].ttf> <brand dir> <boards dir>

<boards dir> holds the canvas boards (Brand*.dc.html); brand/sheet/ works once it has been built.
The README banners are cut out of board 7 at full size. The social cards on board 8 are drawn at
50% (the talk slide at a third), so they are rendered at 4x (6x) to get 2x exports at full size.
All boards are copied to sheet/ and rendered to PNG; board 0 also becomes brand-sheet.html/png.
"""
import os
import re
import shutil
import struct
import subprocess
import sys
import tempfile

import uharfbuzz as hb
from fontTools.pens.boundsPen import BoundsPen
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont
from fontTools.varLib.instancer import instantiateVariableFont

SANS, MONO, OUT, BOARDS = (os.path.abspath(p) for p in sys.argv[1:5])
CHROME = shutil.which("chromium") or shutil.which("google-chrome-stable") or shutil.which("chromium-browser")

# ---------------------------------------------------------------- palette (board 3)
INK, SNOW, PAPER = "#101521", "#F4F5F8", "#FFFFFF"
COBALT, COBALT_400, TINT = "#2563EB", "#60A5FA", "#BFDBFE"

# ---------------------------------------------------------------- the mark (120-unit grid, board 1.2)
# A tally of four lines turned on its side; the strike runs corner to corner and, with the first and
# last lines, draws the Z of ZIO. Lines and strike share one weight: 10 units, round ends.
LINES = "M18 24H102M18 48H102M18 72H102M18 96H102"
STRIKE = "M102 24L18 96"
# The small cut, for 16-24 px: the inner lines merge into one short middle line; all strokes heavier.
SMALL_OUTER, SMALL_MID, SMALL_STRIKE = "M18 26H102M18 94H102", "M40 60H80", "M102 26L18 94"


def mark(lines, strike):
    return (f'<path d="{LINES}" fill="none" stroke="{lines}" stroke-width="10" stroke-linecap="round"/>'
            f'<path d="{STRIKE}" fill="none" stroke="{strike}" stroke-width="10" stroke-linecap="round"/>')


def small_mark(lines, strike):
    return (f'<path d="{SMALL_OUTER}" fill="none" stroke="{lines}" stroke-width="15" stroke-linecap="round"/>'
            f'<path d="{SMALL_MID}" fill="none" stroke="{lines}" stroke-width="13" stroke-linecap="round"/>'
            f'<path d="{SMALL_STRIKE}" fill="none" stroke="{strike}" stroke-width="15" stroke-linecap="round"/>')


def scaled(inner, s):
    return f'<g transform="translate(60 60) scale({s}) translate(-60 -60)">{inner}</g>'


def tile(bg, lines, strike, rx=26, s=0.62, small=False):
    shape = f'<rect width="120" height="120" rx="{rx}" fill="{bg}"/>' if rx else f'<rect width="120" height="120" fill="{bg}"/>'
    return shape + scaled(small_mark(lines, strike) if small else mark(lines, strike), s)


def svg(w, h, vb, body, title=None):
    t = f"<title>{title}</title>" if title else ""
    return f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="{vb}">{t}{body}</svg>\n'


# ---------------------------------------------------------------- wordmark outlines (HarfBuzz shaping, kerning on)
class Face:
    def __init__(self, path, wght):
        self.tt = instantiateVariableFont(TTFont(path), {"wght": wght})
        self.hb = hb.Font(hb.Face(hb.Blob.from_file_path(path)))
        self.hb.set_variations({"wght": wght})
        self.upm = self.tt["head"].unitsPerEm
        self.gs = self.tt.getGlyphSet()
        self.order = self.tt.getGlyphOrder()

    def outline(self, text, size, x, y, tracking_em):
        """Path data for text set at (x, baseline y), CSS letter-spacing after every glyph.
        Returns (d, x_end, bounds) with bounds as (xmin, ymin, xmax, ymax) in output space."""
        buf = hb.Buffer()
        buf.add_str(text)
        buf.guess_segment_properties()
        hb.shape(self.hb, buf, {"kern": True, "liga": False, "calt": False})
        sc = size / self.upm
        pen, bounds = SVGPathPen(self.gs), BoundsPen(self.gs)
        cx = x
        for info, pos in zip(buf.glyph_infos, buf.glyph_positions):
            t = (sc, 0, 0, -sc, cx + pos.x_offset * sc, y - pos.y_offset * sc)
            glyph = self.gs[self.order[info.codepoint]]
            glyph.draw(TransformPen(pen, t))
            glyph.draw(TransformPen(bounds, t))
            cx += pos.x_advance * sc + tracking_em * size
        return pen.getCommands(), cx, bounds.bounds


SORA_BOLD, SORA = Face(SANS, 700), Face(SANS, 400)
EM = 100.0
TRACK = -0.02


def z_mark(x, base, lines, strike):
    """As on the boards: an inline SVG 0.82 em x 0.78 em, viewBox "10 14 100 92", vertical-align -0.02 em.
    The viewBox fits by width, so one unit is 0.0082 em and the content sits 0.013 em below the box top."""
    k = 0.82 * EM / 100
    top = base + 0.02 * EM - 0.78 * EM + 0.013 * EM
    g = f'<g transform="translate({x - 10 * k:.3f} {top - 14 * k:.3f}) scale({k:.4f})">{mark(lines, strike)}</g>'
    # The strike's round caps reach units 11 to 109 across and 17 to 103 down.
    box = (x + (11 - 10) * k, top + (17 - 14) * k, x + (109 - 10) * k, top + (103 - 14) * k)
    return g, x + 0.82 * EM, box


def union(*bs):
    bs = [b for b in bs if b]
    return min(b[0] for b in bs), min(b[1] for b in bs), max(b[2] for b in bs), max(b[3] for b in bs)


def logo(text, strike, stacked=False):
    base = 100.0
    m, x, mb = z_mark(0, base, text, strike)
    x += 0.04 * EM
    # "IO" in Sora 700 and "Query" in Sora 400: weight, not colour, separates the two words.
    if stacked:
        d_io, _, b1 = SORA_BOLD.outline("IO", EM, x, base, TRACK)
        d_q, _, b2 = SORA.outline("Query", EM, 0, base + 1.02 * EM, TRACK)
    else:
        d_io, x, b1 = SORA_BOLD.outline("IO ", EM, x, base, TRACK)
        d_q, _, b2 = SORA.outline("Query", EM, x, base, TRACK)
    x0, y0, x1, y1 = union(mb, b1, b2)
    pad = 4
    vb = (x0 - pad, y0 - pad, x1 - x0 + 2 * pad, y1 - y0 + 2 * pad)
    h = 240 if stacked else 120
    w = round(h * vb[2] / vb[3])
    body = f'{m}<path d="{d_io}" fill="{text}"/><path d="{d_q}" fill="{text}"/>'
    return svg(w, h, " ".join(f"{v:.2f}" for v in vb), body, "ZIO Query")


# ---------------------------------------------------------------- write SVG sources
def write(name, content):
    with open(os.path.join(OUT, name), "w") as f:
        f.write(content)


os.makedirs(OUT, exist_ok=True)
V = "0 0 120 120"
write("zio-query-logo-light.svg", logo(INK, COBALT))
write("zio-query-logo-dark.svg", logo(SNOW, COBALT_400))
write("zio-query-logo-stacked-light.svg", logo(INK, COBALT, stacked=True))
write("zio-query-logo-stacked-dark.svg", logo(SNOW, COBALT_400, stacked=True))
write("zio-query-mark-light.svg", svg(240, 240, V, mark(INK, COBALT), "ZIO Query mark"))
write("zio-query-mark-dark.svg", svg(240, 240, V, mark(SNOW, COBALT_400), "ZIO Query mark"))
write("zio-query-mark-mono.svg", svg(240, 240, V, mark("currentColor", "currentColor"), "ZIO Query mark"))
write("zio-query-symbol.svg", svg(240, 240, V, tile(COBALT, TINT, PAPER), "ZIO Query"))
write("zio-query-symbol-dark.svg", svg(240, 240, V, tile(INK, SNOW, COBALT_400), "ZIO Query"))
write("favicon.svg", svg(16, 16, V, tile(COBALT, TINT, PAPER, s=0.68, small=True)))
write("icon-maskable.svg", svg(512, 512, V, tile(COBALT, TINT, PAPER, rx=0, s=0.56)))
TMP = tempfile.mkdtemp(prefix="zq-brand-")
for name, body in [("apple-touch-icon.svg", tile(COBALT, TINT, PAPER, rx=0, s=0.62)),
                   ("avatar.svg", tile(COBALT, TINT, PAPER, rx=0, s=0.6))]:
    with open(os.path.join(TMP, name), "w") as f:
        f.write(svg(120, 120, V, body))

with open(os.path.join(OUT, "tokens.css"), "w") as f:
    f.write("""/* ZIO Query brand tokens. Light by default, dark through prefers-color-scheme. */
:root {
  --zq-cobalt: #2563EB;        /* the strike, the tile, buttons; 5.17:1 on white */
  --zq-cobalt-700: #1D4ED8;    /* links and small cobalt text; 6.70:1 on white */
  --zq-cobalt-400: #60A5FA;    /* the strike on dark; 7.17:1 on ink */
  --zq-cobalt-300: #93C5FD;    /* links and code keywords on dark; 10.12:1 on ink */
  --zq-tint: #BFDBFE;          /* lines of the mark on the cobalt tile; shapes only */
  --zq-ink: #101521;
  --zq-paper: #FFFFFF;
  --zq-snow: #F4F5F8;          /* panels, code on light */
  --zq-bg: var(--zq-paper);
  --zq-surface: var(--zq-snow);
  --zq-line: #E3E6EC;          /* borders, never text */
  --zq-text: var(--zq-ink);    /* 18.24:1 on paper */
  --zq-text-2: #566074;        /* slate; 6.32:1 on paper */
  --zq-accent: var(--zq-cobalt);
  --zq-on-accent: #FFFFFF;     /* 5.17:1 on cobalt */
  --zq-link: var(--zq-cobalt-700);
  --zq-tile: var(--zq-cobalt);
  --zq-code-keyword: var(--zq-cobalt-700);
  --zq-font-sans: "Sora", system-ui, sans-serif;
  --zq-font-mono: "JetBrains Mono", ui-monospace, monospace;
  --zq-radius-tile: 21.7%;     /* 26 of 120 units */
}
@media (prefers-color-scheme: dark) {
  :root {
    --zq-bg: var(--zq-ink);
    --zq-surface: #171C28;
    --zq-line: #232833;
    --zq-text: var(--zq-snow);  /* 16.73:1 on ink */
    --zq-text-2: #A7AEBD;       /* 8.19:1 on ink */
    --zq-accent: var(--zq-cobalt-400);
    --zq-on-accent: var(--zq-ink);
    --zq-link: var(--zq-cobalt-300);
    --zq-code-keyword: var(--zq-cobalt-300);
  }
}
/* The mark: lines use --zq-text, the strike uses --zq-accent. On the tile: tint lines, white strike. */
""")

if not CHROME:
    sys.exit("Chromium not found: SVG sources written, PNGs skipped.")

# ---------------------------------------------------------------- rasterising
FONTFACE = (f"@font-face{{font-family:'Sora';src:url('file://{SANS}') format('truetype');font-weight:100 800}}"
            f"@font-face{{font-family:'JetBrains Mono';src:url('file://{MONO}') format('truetype');font-weight:100 800}}")


def shoot(html, w, h, out, scale=1, transparent=False):
    p = os.path.join(TMP, "p.html")
    with open(p, "w") as f:
        f.write(html)
    args = [CHROME, "--headless", "--no-sandbox", "--disable-gpu", "--hide-scrollbars", "--allow-file-access-from-files",
            f"--window-size={w},{h}", f"--force-device-scale-factor={scale}", f"--screenshot={out}",
            "--virtual-time-budget=4000"]
    if transparent:
        args.append("--default-background-color=00000000")
    subprocess.run(args + ["file://" + p], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def shoot_svg(path, size, out):
    shoot(f"<body style='margin:0;background:transparent'><img src='file://{path}' width={size} height={size} "
          f"style='display:block'>", size, size, os.path.join(OUT, out), transparent=True)


for src, size, out in [(os.path.join(OUT, "favicon.svg"), 16, "favicon-16.png"),
                       (os.path.join(OUT, "favicon.svg"), 32, "favicon-32.png"),
                       (os.path.join(OUT, "favicon.svg"), 48, "favicon-48.png"),
                       (os.path.join(TMP, "apple-touch-icon.svg"), 180, "apple-touch-icon.png"),
                       (os.path.join(OUT, "zio-query-symbol.svg"), 192, "icon-192.png"),
                       (os.path.join(OUT, "zio-query-symbol.svg"), 512, "icon-512.png"),
                       (os.path.join(OUT, "icon-maskable.svg"), 512, "icon-maskable-512.png"),
                       (os.path.join(TMP, "avatar.svg"), 500, "avatar-500.png")]:
    shoot_svg(src, size, out)

pngs = [open(os.path.join(OUT, f"favicon-{n}.png"), "rb").read() for n in (16, 32, 48)]
off, ents, data = 6 + 16 * 3, b"", b""
for n, p in zip((16, 32, 48), pngs):
    ents += struct.pack("<BBBBHHII", n, n, 0, 0, 1, 32, len(p), off + len(data))
    data += p
with open(os.path.join(OUT, "favicon.ico"), "wb") as f:
    f.write(struct.pack("<HHH", 0, 1, 3) + ents + data)


# ---------------------------------------------------------------- boards: cut-outs and sheet
def read_board(name):
    return open(os.path.join(BOARDS, f"{name}.dc.html")).read()


def defs_of(s):
    return re.search(r'<svg width="0" height="0".*?</defs></svg>', s, re.S).group(0)


def element(s, start):
    depth = 0
    for t in re.finditer(r"<div\b|</div>", s[start:]):
        depth += 1 if t.group(0) == "<div" else -1
        if depth == 0:
            return s[start:start + t.end()]
    raise ValueError("unbalanced div")


def cutouts(s, w, h):
    """Artboards drawn on a board: <div style="width: Wpx; height: Hpx; ... background: ..."> in document order."""
    return [element(s, m.start()) for m in re.finditer(rf'<div style="width: {w}px; height: {h}px;[^"]*background:', s)]


def flat(el):
    """Drop the board's preview frame (rounded corners, border) so the export is a plain rectangle."""
    head, rest = el.split(">", 1)
    head = re.sub(r"border-radius: \d+px; ?", "", head)
    head = re.sub(r"border: 1px solid #[0-9A-Fa-f]{6}; ?", "", head)
    return head + ">" + rest


def page(defs, body):
    return (f'<!doctype html><html><head><meta charset="utf-8"><style>{FONTFACE}body{{margin:0;font-family:\'Sora\',sans-serif}}'
            f'</style></head><body>{defs}{body}</body></html>')


social = read_board("Brand8Social")
d8 = defs_of(social)
gh, og, sl = cutouts(social, 640, 320), cutouts(social, 600, 315), cutouts(social, 640, 360)
for name, el, w, h, scale in [("social-github-light.png", gh[0], 640, 320, 4), ("social-github-dark.png", gh[1], 640, 320, 4),
                              ("social-og-light.png", og[0], 600, 315, 4), ("social-og-dark.png", og[1], 600, 315, 4),
                              ("release-card.png", og[2], 600, 315, 4), ("slide-title.png", sl[0], 640, 360, 6)]:
    shoot(page(d8, flat(el)), w, h, os.path.join(OUT, name), scale=scale)


def board_to_html(src, local_fonts=True):
    s = open(src).read()
    helmet = re.search(r"<helmet>(.*?)</helmet>", s, re.S).group(1)
    if local_fonts:
        helmet = f"<style>{FONTFACE}</style>" + re.sub(r'<link[^>]+fonts\.google[^>]*>', "", helmet)
    body = re.search(r"<x-dc>(.*?)</x-dc>", s, re.S).group(1)
    body = re.sub(r"<helmet>.*?</helmet>", "", body, flags=re.S)
    w, h = (int(x) for x in re.search(r'"\$preview":\{"width":(\d+),"height":(\d+)\}', s).groups())
    title = re.search(r"<title>(.*?)</title>", s).group(1)
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>{title}</title>{helmet}</head>'
            f'<body>{body}</body></html>'), w, h


sheet = os.path.join(OUT, "sheet")
os.makedirs(sheet, exist_ok=True)
for name in sorted((f[:-8] for f in os.listdir(BOARDS) if re.match(r"Brand\d+\w*\.dc\.html$", f)),
                   key=lambda n: int(re.match(r"Brand(\d+)", n).group(1))):
    src = os.path.join(BOARDS, f"{name}.dc.html")
    dst = os.path.join(sheet, f"{name}.dc.html")
    if os.path.abspath(src) != os.path.abspath(dst):
        shutil.copy(src, dst)
    html, w, h = board_to_html(src)
    shoot(html, w, h, os.path.join(sheet, f"{name}.png"))
    if name.startswith("Brand0"):
        shutil.copy(os.path.join(sheet, f"{name}.png"), os.path.join(OUT, "brand-sheet.png"))
        with open(os.path.join(OUT, "brand-sheet.html"), "w") as f:
            f.write(board_to_html(src, local_fonts=False)[0])

shutil.rmtree(TMP, ignore_errors=True)
print("done:", OUT)
