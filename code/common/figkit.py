"""figkit: small SVG figure builder for the book's static figures.

Each figure is drawn once and saved twice, as
book/media/figures/<name>-light.svg and <name>-dark.svg, with the same
geometry and a palette for each Material scheme. Embed both:

    ![alt](../media/figures/<name>-light.svg#only-light)
    ![alt](../media/figures/<name>-dark.svg#only-dark)

Colours are tokens (fg, mute, line, acc, ok, blue, warn, red, soft) and
"<token>f" for the light fill of the same hue. Backgrounds stay transparent.
"""
from __future__ import annotations

import html
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "book" / "media" / "figures"

PALETTES = {
    "light": dict(fg="#1B1F23", mute="#5F6670", line="#B4ADA3", acc="#D9531A", accf="#FBE3D6",
                  ok="#2F7D5B", okf="#DCEFE4", blue="#2B6CB0", bluef="#DCE8F5", warn="#A86A12",
                  warnf="#FAEBC8", red="#C0392B", redf="#F8DAD5", soft="#F3EEE6", softf="#F3EEE6",
                  mutef="#E6E3DE", fgf="#E6E3DE", bg="#FFFFFF", none="none"),
    "dark": dict(fg="#E4E0D8", mute="#A3A9B3", line="#5A606B", acc="#F07A3C", accf="#4A2A1C",
                 ok="#7BC496", okf="#1E3A2B", blue="#7FB0E6", bluef="#1D3049", warn="#E6B455",
                 warnf="#3D3018", red="#F08070", redf="#46221E", soft="#2A2D36", softf="#2A2D36",
                 mutef="#353944", fgf="#353944", bg="#1E2029", none="none"),
}
SANS = "'DM Sans', Roboto, 'Helvetica Neue', Arial, sans-serif"
MONO = "'DM Mono', 'Roboto Mono', Menlo, Consolas, monospace"

# box kinds: (fill, stroke, title colour, sub colour)
KINDS = {
    "paper": ("soft", "line", "fg", "mute"),
    "ghost": ("none", "line", "mute", "mute"),
    "accent": ("accf", "acc", "fg", "mute"),
    "ok": ("okf", "ok", "fg", "mute"),
    "blue": ("bluef", "blue", "fg", "mute"),
    "warn": ("warnf", "warn", "fg", "mute"),
    "red": ("redf", "red", "fg", "mute"),
    "mute": ("mutef", "mute", "fg", "mute"),
}


def esc(s) -> str:
    return html.escape(str(s), quote=True)


class Fig:
    def __init__(self, w: int, h: int, title: str = ""):
        self.w, self.h, self.title = w, h, title
        self.items = []  # callables palette -> svg string
        self.n_arrow = 0

    # primitives -------------------------------------------------------
    def raw(self, fn):
        self.items.append(fn)

    def rect(self, x, y, w, h, fill="soft", stroke="line", rx=8, sw=1.5, dash=None, opacity=1):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.items.append(lambda p: f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" '
                          f'fill="{p[fill]}" stroke="{p[stroke]}" stroke-width="{sw}"{d} opacity="{opacity}"/>')

    def text(self, x, y, s, size=14, color="fg", anchor="middle", weight=400, mono=False, italic=False):
        fam = MONO if mono else SANS
        st = ' font-style="italic"' if italic else ""
        lines = str(s).split("\n")
        def f(p, lines=lines):
            out = []
            for i, ln in enumerate(lines):
                yy = y + i * size * 1.25
                out.append(f'<text x="{x}" y="{yy}" font-family="{fam}" font-size="{size}" font-weight="{weight}"{st} '
                           f'fill="{p[color]}" text-anchor="{anchor}" dominant-baseline="middle">{esc(ln)}</text>')
            return "".join(out)
        self.items.append(f)

    def line(self, x1, y1, x2, y2, color="line", sw=1.5, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.items.append(lambda p: f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{p[color]}" '
                          f'stroke-width="{sw}"{d}/>')

    def path(self, d, color="line", sw=1.5, fill="none", dash=None, arrow=False):
        da = f' stroke-dasharray="{dash}"' if dash else ""
        def f(p):
            m = f' marker-end="url(#ah-{color})"' if arrow else ""
            return f'<path d="{d}" stroke="{p[color]}" stroke-width="{sw}" fill="{p[fill]}"{da}{m}/>'
        self.items.append(f)

    def arrow(self, x1, y1, x2, y2, color="mute", sw=1.6, dash=None, label=None, lsize=12, lcolor=None, loff=-9):
        da = f' stroke-dasharray="{dash}"' if dash else ""
        self.items.append(lambda p: f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{p[color]}" '
                          f'stroke-width="{sw}"{da} marker-end="url(#ah-{color})"/>')
        if label:
            self.text((x1 + x2) / 2, (y1 + y2) / 2 + loff, label, size=lsize, color=lcolor or color)

    def circle(self, cx, cy, r, fill="acc", stroke="none", sw=1.5):
        self.items.append(lambda p: f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{p[fill]}" stroke="{p[stroke]}" stroke-width="{sw}"/>')

    # composites -------------------------------------------------------
    def box(self, x, y, w, h, title="", sub="", kind="paper", size=14, subsize=None, mono=False, rx=8,
            dash=None, weight=600, submono=None):
        fill, stroke, tc, sc = KINDS[kind]
        self.rect(x, y, w, h, fill=fill, stroke=stroke, rx=rx, dash=dash)
        subsize = subsize or round(size * 0.82)
        if submono is None:
            submono = mono
        tl = str(title).split("\n") if title else []
        sl = str(sub).split("\n") if sub else []
        total = len(tl) * size * 1.25 + len(sl) * subsize * 1.25
        y0 = y + h / 2 - total / 2 + size * 0.62
        if tl:
            self.text(x + w / 2, y0, "\n".join(tl), size=size, color=tc, weight=weight, mono=mono)
        if sl:
            self.text(x + w / 2, y0 + len(tl) * size * 1.25 - size * 0.62 + subsize * 0.62, "\n".join(sl),
                      size=subsize, color=sc, mono=submono)

    def table(self, x, y, cols, rows, widths, rh=26, size=13, head_color="mute", hi_rows=(), hi="acc",
              mono_cols=(), align=None, zebra=True):
        """A simple table. widths: column widths. align: list of 'start'|'end'|'middle'."""
        align = align or ["start"] * len(cols)
        W = sum(widths)
        self.line(x, y + rh, x + W, y + rh, color="line")
        def cell(r_y, j, v, color, weight, mono):
            cx = x + sum(widths[:j])
            a = align[j]
            tx = cx + 8 if a == "start" else (cx + widths[j] - 8 if a == "end" else cx + widths[j] / 2)
            self.text(tx, r_y + rh / 2, v, size=size, color=color, anchor=a, weight=weight, mono=mono)
        for j, c in enumerate(cols):
            cell(y, j, c, head_color, 600, False)
        for i, r in enumerate(rows):
            ry = y + rh * (i + 1)
            if i in hi_rows:
                self.rect(x, ry + 1, W, rh - 2, fill=hi + "f", stroke="none", rx=3)
            elif zebra and i % 2 == 1:
                self.rect(x, ry + 1, W, rh - 2, fill="soft", stroke="none", rx=3, opacity=0.6)
            for j, v in enumerate(r):
                cell(ry, j, v, "fg", 600 if i in hi_rows else 400, j in mono_cols)
        return y + rh * (len(rows) + 1)

    def note(self, s, y=None):
        """A small grey line at the bottom, e.g. 'schematic, not measured'."""
        self.text(self.w / 2, y if y is not None else self.h - 10, s, size=11.5, color="mute", italic=True)

    # output -----------------------------------------------------------
    def svg(self, scheme: str) -> str:
        p = PALETTES[scheme]
        markers = "".join(
            f'<marker id="ah-{k}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" '
            f'orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="{p[k]}"/></marker>'
            for k in ("mute", "line", "fg", "acc", "ok", "blue", "warn", "red"))
        body = "".join(fn(p) for fn in self.items)
        t = f"<title>{esc(self.title)}</title>" if self.title else ""
        return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" width="{self.w}" '
                f'height="{self.h}" role="img">{t}<defs>{markers}</defs>{body}</svg>\n')

    def save(self, name: str):
        OUT.mkdir(parents=True, exist_ok=True)
        for s in ("light", "dark"):
            (OUT / f"{name}-{s}.svg").write_text(self.svg(s))
        return [OUT / f"{name}-{s}.svg" for s in ("light", "dark")]
