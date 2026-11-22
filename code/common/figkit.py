"""Small SVG kit for the book's figures (light and dark variants from one drawing).

Each figure script builds a `Fig`, draws with role colours (ink, mute, line, and the
accents br, si, go, ok, bad, gr, pu), and calls `fig.save("chNN-name")`. That writes
`book/media/figures/chNN-name-light.svg` and `-dark.svg`. The two files differ only in
the CSS variables at the top. `render_png` makes PNG checks with headless Chrome on a
white and a slate background (the two Material schemes).
"""
from __future__ import annotations

import html
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "book" / "media" / "figures"

PALETTES = {
    "light": dict(ink="#1f2937", mute="#5b6472", line="#9aa3af", soft="#eef0f3",
                  br="#8a5a2b", si="#3a5f96", go="#c0661a", ok="#3f7d58", bad="#b03a2e",
                  gr="#4b5563", pu="#6b4fa0"),
    "dark": dict(ink="#e5e7eb", mute="#a3abb8", line="#6b7280", soft="#2a2e38",
                 br="#c99560", si="#7aa2dc", go="#f0924a", ok="#72c08f", bad="#ef7666",
                 gr="#aab2bf", pu="#a98be0"),
}
BG = {"light": "#ffffff", "dark": "#1e2129"}
SANS = "Inter, 'DM Sans', 'Segoe UI', Helvetica, Arial, sans-serif"
MONO = "'JetBrains Mono', 'Fira Code', Menlo, Consolas, monospace"


def esc(s) -> str:
    return html.escape(str(s), quote=True)


class Fig:
    def __init__(self, w: int, h: int, title: str):
        self.w, self.h, self.title = w, h, title
        self.parts: list[str] = []

    # primitives -----------------------------------------------------------------
    def rect(self, x, y, w, h, c="gr", fill=0.16, rx=8, stroke=1.6, dash=None, sc=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        fc = f"var(--{c})" if fill else "none"
        self.parts.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fc}" '
            f'fill-opacity="{fill}" stroke="var(--{sc or c})" stroke-width="{stroke}"{d}/>')

    def text(self, x, y, s, size=14, c="ink", weight=400, anchor="start", mono=False, italic=False):
        fam = MONO if mono else SANS
        it = ' font-style="italic"' if italic else ""
        self.parts.append(
            f'<text x="{x}" y="{y}" font-family="{esc(fam)}" font-size="{size}" font-weight="{weight}" '
            f'fill="var(--{c})" text-anchor="{anchor}"{it}>{esc(s)}</text>')

    def lines(self, x, y, rows, size=14, gap=None, **kw):
        gap = gap or size * 1.35
        for i, r in enumerate(rows):
            self.text(x, y + i * gap, r, size=size, **kw)

    def line(self, x1, y1, x2, y2, c="line", w=1.6, dash=None, arrow=False):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        m = f' marker-end="url(#ah-{c})"' if arrow else ""
        self.parts.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="var(--{c})" '
                          f'stroke-width="{w}"{d}{m}/>')

    def path(self, d, c="line", w=1.6, fill="none", dash=None, arrow=False, fill_op=1):
        da = f' stroke-dasharray="{dash}"' if dash else ""
        m = f' marker-end="url(#ah-{c})"' if arrow else ""
        f = f"var(--{fill})" if fill != "none" else "none"
        self.parts.append(f'<path d="{d}" stroke="var(--{c})" stroke-width="{w}" fill="{f}" '
                          f'fill-opacity="{fill_op}"{da}{m}/>')

    def circle(self, x, y, r, c="gr", fill=1.0, stroke=0, sc=None):
        self.parts.append(f'<circle cx="{x}" cy="{y}" r="{r}" fill="var(--{c})" fill-opacity="{fill}" '
                          f'stroke="var(--{sc or c})" stroke-width="{stroke}"/>')

    def box(self, x, y, w, h, title, sub=None, c="gr", size=14, mono=False, fill=0.16, dash=None):
        self.rect(x, y, w, h, c=c, fill=fill, dash=dash)
        if sub is None:
            self.text(x + w / 2, y + h / 2 + size * 0.36, title, size=size, weight=600, anchor="middle", mono=mono)
        else:
            subs = sub if isinstance(sub, list) else [sub]
            top = y + h / 2 - (len(subs) * size * 1.15) / 2 + size * 0.3
            self.text(x + w / 2, top, title, size=size, weight=600, anchor="middle", mono=mono)
            for i, s in enumerate(subs):
                self.text(x + w / 2, top + (i + 1) * size * 1.2, s, size=size * 0.86, c="mute", anchor="middle")

    def table(self, x, y, cols, rows, head=None, size=13, rowh=24, mono=True, hi=None, hic="go",
              align=None, strike=None):
        """cols = list of widths. hi = {row_index: colour}. strike = set of row indexes."""
        hi = hi or {}
        strike = strike or set()
        align = align or ["start"] * len(cols)
        tw = sum(cols)
        yy = y
        if head:
            xx = x
            for wcol, hcell, al in zip(cols, head, align):
                tx = xx + 8 if al == "start" else xx + wcol - 8
                self.text(tx, yy + rowh * 0.68, hcell, size=size, c="mute", weight=600, anchor=al, mono=mono)
                xx += wcol
            yy += rowh
            self.line(x, yy, x + tw, yy, c="line", w=1.2)
        for i, r in enumerate(rows):
            if i in hi:
                self.rect(x, yy + 1, tw, rowh - 2, c=hi[i], fill=0.18, rx=3, stroke=0)
            xx = x
            for wcol, cell, al in zip(cols, r, align):
                tx = xx + 8 if al == "start" else xx + wcol - 8
                self.text(tx, yy + rowh * 0.68, cell, size=size, mono=mono, anchor=al,
                          c="mute" if i in strike else "ink")
                xx += wcol
            if i in strike:
                self.line(x + 4, yy + rowh / 2, x + tw - 4, yy + rowh / 2, c="bad", w=1.4)
            yy += rowh
        return yy

    def note(self, s, y=None):
        self.text(self.w - 10, y or self.h - 8, s, size=11, c="mute", anchor="end", italic=True)

    # output ---------------------------------------------------------------------
    def svg(self, scheme: str) -> str:
        p = PALETTES[scheme]
        vars_ = ";".join(f"--{k}:{v}" for k, v in p.items())
        markers = "".join(
            f'<marker id="ah-{k}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
            f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="var(--{k})"/></marker>'
            for k in p)
        return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" '
                f'width="{self.w}" height="{self.h}" role="img" aria-label="{esc(self.title)}">'
                f'<title>{esc(self.title)}</title><style>svg{{{vars_}}}</style><defs>{markers}</defs>'
                + "".join(self.parts) + "</svg>\n")

    def save(self, name: str, check_dir: Path | None = None):
        OUT.mkdir(parents=True, exist_ok=True)
        for scheme in PALETTES:
            f = OUT / f"{name}-{scheme}.svg"
            f.write_text(self.svg(scheme))
            if check_dir:
                render_png(f, scheme, self.w, self.h, check_dir / f"{name}-{scheme}.png")
        return name


def render_png(svg: Path, scheme: str, w: int, h: int, png: Path):
    png.parent.mkdir(parents=True, exist_ok=True)
    page = png.with_suffix(".html")
    page.write_text(f'<html><body style="margin:0;background:{BG[scheme]}">'
                    f'<img src="file://{svg}" width="{w}" height="{h}"></body></html>')
    subprocess.run(["google-chrome", "--headless=new", "--disable-gpu", "--hide-scrollbars",
                    f"--window-size={w},{h}", f"--screenshot={png}", f"file://{page}"],
                   check=True, capture_output=True)


def embed(name: str, alt: str, caption: str, indent: str = "") -> str:
    """The markdown for a chapter: both variants in one <figure>."""
    return (f'{indent}<figure markdown>\n'
            f'{indent}  ![{alt}](../media/figures/{name}-light.svg#only-light)\n'
            f'{indent}  ![{alt}](../media/figures/{name}-dark.svg#only-dark)\n'
            f'{indent}  <figcaption>{caption}</figcaption>\n'
            f'{indent}</figure>\n')
