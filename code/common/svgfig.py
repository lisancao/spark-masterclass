"""Small SVG figure kit for the book's hand-drawn figures.

Each figure is drawn once with colour tokens and saved twice, as
book/media/figures/<name>-light.svg and <name>-dark.svg, embedded with
#only-light / #only-dark. `check(name)` renders both to one PNG for review.

Usage:
    from svgfig import Fig
    f = Fig(760, 300)
    f.box(40, 40, 160, 60, "Driver", "SparkSession", kind="acc")
    f.arrow(200, 70, 300, 70, "plan")
    f.save("ch06-example")
"""
from __future__ import annotations

import html
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "book" / "media" / "figures"
CHECK = Path(os.environ.get("FIG_CHECK_DIR", "/tmp/svgfig-check"))

PAL = {
    "light": {
        "fg": "#1f2328", "mute": "#57606a", "line": "#8c959f", "grid": "#d0d7de", "bg": "#ffffff",
        "n": ("#f3f4f6", "#8c959f", "#1f2328"),
        "acc": ("#fff0e6", "#e8590c", "#9a3412"),
        "ok": ("#e7f5ec", "#2f9e44", "#1e6b30"),
        "bad": ("#fdeceb", "#d63c3c", "#9b1c1c"),
        "blue": ("#e8f1fd", "#3b82f6", "#1e4f9c"),
        "purple": ("#f3ecfd", "#8b5cf6", "#5b2ca0"),
        "ghost": ("none", "#8c959f", "#57606a"),
        "ink": ("#1f2328", "#1f2328", "#ffffff"),
        "code": ("#f6f8fa", "#d0d7de", "#1f2328"),
    },
    "dark": {
        "fg": "#e6edf3", "mute": "#9da7b3", "line": "#7d8590", "grid": "#3b424c", "bg": "#1e2129",
        "n": ("#2b3038", "#6e7781", "#e6edf3"),
        "acc": ("#3a2416", "#ff8a4c", "#ffbe99"),
        "ok": ("#17301f", "#4cc26a", "#9be6ae"),
        "bad": ("#3a1a1a", "#ff6b6b", "#ffb3b3"),
        "blue": ("#16263d", "#5b9cf5", "#a8caff"),
        "purple": ("#271c3d", "#a78bfa", "#d4c4ff"),
        "ghost": ("none", "#7d8590", "#9da7b3"),
        "ink": ("#e6edf3", "#e6edf3", "#1e2129"),
        "code": ("#161b22", "#3b424c", "#e6edf3"),
    },
}
SANS = "Inter, 'DM Sans', 'Helvetica Neue', Arial, sans-serif"
MONO = "'JetBrains Mono', 'Monaspace Neon', Menlo, Consolas, monospace"


def esc(s) -> str:
    return html.escape(str(s), quote=True)


class Fig:
    def __init__(self, w: int, h: int, title: str = ""):
        self.w, self.h, self.title = w, h, title
        self.items: list = []  # callables palette -> str

    # --- primitives -----------------------------------------------------
    def raw(self, fn):
        self.items.append(fn)

    def text(self, x, y, s, size=14, weight=400, anchor="middle", c="fg", mono=False, italic=False):
        def f(p):
            col = p[c] if isinstance(p.get(c), str) else p[c][2]
            fam = MONO if mono else SANS
            st = ' font-style="italic"' if italic else ""
            lines = str(s).split("\n")
            out = []
            for i, ln in enumerate(lines):
                out.append(
                    f'<text x="{x}" y="{y + i * size * 1.25:.1f}" font-family="{fam}" font-size="{size}" '
                    f'font-weight="{weight}" text-anchor="{anchor}" fill="{col}"{st} xml:space="preserve">{esc(ln)}</text>')
            return "".join(out)
        self.items.append(f)

    def rect(self, x, y, w, h, kind="n", r=8, dash=False, sw=1.5, fill=None):
        def f(p):
            fl, stroke, _ = p[kind]
            if fill is not None:
                fl = fill if fill == "none" else p[fill][0]
            d = ' stroke-dasharray="5 4"' if dash else ""
            return (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fl}" '
                    f'stroke="{stroke}" stroke-width="{sw}"{d}/>')
        self.items.append(f)

    def box(self, x, y, w, h, title="", sub="", kind="n", r=8, dash=False, size=14, mono=False,
            subsize=None, sub_mono=False, weight=600):
        self.rect(x, y, w, h, kind, r, dash)
        subsize = subsize or max(10, size - 3)
        n_sub = len(str(sub).split("\n")) if sub else 0
        block = size + n_sub * subsize * 1.25
        top = y + h / 2 - block / 2 + size * 0.8
        if title:
            self.text(x + w / 2, top, title, size, weight, c=kind, mono=mono)
        if sub:
            self.text(x + w / 2, top + size * 0.35 + subsize * 1.1, sub, subsize, 400, c=kind if kind != "n" else "mute",
                      mono=sub_mono)

    def line(self, x1, y1, x2, y2, c="line", sw=1.5, dash=False):
        def f(p):
            col = p[c] if isinstance(p[c], str) else p[c][1]
            d = ' stroke-dasharray="5 4"' if dash else ""
            return f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{col}" stroke-width="{sw}"{d}/>'
        self.items.append(f)

    def arrow(self, x1, y1, x2, y2, label="", c="line", sw=1.6, dash=False, lsize=12, loff=-7, lc="mute",
              both=False):
        self.path([(x1, y1), (x2, y2)], c=c, sw=sw, dash=dash, head=True, tail=both)
        if label:
            self.text((x1 + x2) / 2, (y1 + y2) / 2 + loff, label, lsize, 500, c=lc)

    def path(self, pts, c="line", sw=1.6, dash=False, head=True, tail=False):
        def f(p):
            col = p[c] if isinstance(p[c], str) else p[c][1]
            d = ' stroke-dasharray="5 4"' if dash else ""
            pts_s = " ".join(f"{a},{b}" for a, b in pts)
            out = f'<polyline points="{pts_s}" fill="none" stroke="{col}" stroke-width="{sw}"{d} stroke-linejoin="round"/>'
            for (ax, ay), (bx, by), on in ((pts[-2], pts[-1], head), (pts[1], pts[0], tail)):
                if not on:
                    continue
                import math
                ang = math.atan2(by - ay, bx - ax)
                L, W = 9, 4.5
                p1 = (bx - L * math.cos(ang) + W * math.sin(ang), by - L * math.sin(ang) - W * math.cos(ang))
                p2 = (bx - L * math.cos(ang) - W * math.sin(ang), by - L * math.sin(ang) + W * math.cos(ang))
                out += f'<polygon points="{bx},{by} {p1[0]:.1f},{p1[1]:.1f} {p2[0]:.1f},{p2[1]:.1f}" fill="{col}"/>'
            return out
        self.items.append(f)

    def circle(self, cx, cy, r, kind="acc", fill=True):
        def f(p):
            fl, st, _ = p[kind]
            return f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{st if fill else fl}" stroke="{st}" stroke-width="1.5"/>'
        self.items.append(f)

    # --- composites -----------------------------------------------------
    def table(self, x, y, widths, rows, header=True, rh=26, size=13, mono_cols=(), hi_rows=(), hi_kind="acc",
              align=None, bad_rows=(), ok_rows=()):
        """rows[0] is the header when header=True. hi_rows index into rows."""
        W = sum(widths)
        align = align or ["start"] * len(widths)
        for i, row in enumerate(rows):
            yy = y + i * rh
            kind = None
            if header and i == 0:
                kind = "n"
            elif i in hi_rows:
                kind = hi_kind
            elif i in bad_rows:
                kind = "bad"
            elif i in ok_rows:
                kind = "ok"
            if kind:
                self.rect(x, yy, W, rh, kind, r=0, sw=0)
            xx = x
            for j, cell in enumerate(row):
                a = align[j]
                tx = xx + 8 if a == "start" else (xx + widths[j] - 8 if a == "end" else xx + widths[j] / 2)
                c = (kind if kind and kind != "n" else "fg")
                self.text(tx, yy + rh / 2 + size * 0.35, cell, size, 700 if (header and i == 0) else 400, anchor=a,
                          c=c, mono=(j in mono_cols and not (header and i == 0)))
                xx += widths[j]
            self.line(x, yy + rh, x + W, yy + rh, c="grid", sw=1)
        self.line(x, y, x + W, y, c="line", sw=1.2)
        self.line(x, y + len(rows) * rh, x + W, y + len(rows) * rh, c="line", sw=1.2)

    def code(self, x, y, w, lines, size=12.5, lh=None, hl=(), title=""):
        lh = lh or size * 1.5
        top = 22 if title else 10
        h = top + len(lines) * lh + 8
        self.rect(x, y, w, h, "code", r=6)
        if title:
            self.text(x + 10, y + 15, title, 11, 600, anchor="start", c="mute")
        for i, ln in enumerate(lines):
            if i in hl:
                self.rect(x + 2, y + top + i * lh - 1, w - 4, lh, "acc", r=2, sw=0)
            self.text(x + 10, y + top + i * lh + lh * 0.7, ln, size, 400, anchor="start",
                      c=("acc" if i in hl else "code"), mono=True)
        return h

    def note(self, s="schematic, not to scale", x=None, y=None):
        self.text(x if x is not None else self.w - 8, y if y is not None else self.h - 8, s, 11, 400,
                  anchor="end", c="mute", italic=True)

    # --- output ---------------------------------------------------------
    def render(self, mode: str) -> str:
        p = PAL[mode]
        body = "".join(fn(p) for fn in self.items)
        t = f"<title>{esc(self.title)}</title>" if self.title else ""
        return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" width="{self.w}" '
                f'height="{self.h}" role="img">{t}{body}</svg>\n')

    def save(self, name: str, check: bool = True):
        OUT.mkdir(parents=True, exist_ok=True)
        for mode in ("light", "dark"):
            (OUT / f"{name}-{mode}.svg").write_text(self.render(mode))
        if check:
            self.check(name)

    def check(self, name: str):
        CHECK.mkdir(parents=True, exist_ok=True)
        page = CHECK / f"{name}.html"
        page.write_text(
            "<html><body style='margin:0;display:flex;flex-direction:column'>"
            f"<div style='background:#ffffff;padding:10px'><img src='file://{OUT}/{name}-light.svg'></div>"
            f"<div style='background:#1e2129;padding:10px'><img src='file://{OUT}/{name}-dark.svg'></div>"
            "</body></html>")
        png = CHECK / f"{name}.png"
        subprocess.run(["google-chrome", "--headless=new", "--disable-gpu", "--no-sandbox", "--hide-scrollbars",
                        "--force-device-scale-factor=1", f"--window-size={self.w + 20},{2 * self.h + 40}",
                        f"--screenshot={png}", f"file://{page}"], check=True, capture_output=True)
        print(png)
