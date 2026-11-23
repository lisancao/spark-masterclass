"""Small SVG kit for book figures that read on light and dark pages.

Transparent background. Boxes have solid mid-tone fills with white text, so they
read on both page colours. Text outside boxes uses a mid grey or the accent.
"""
from html import escape

BLUE = "#3a5f96"
ORANGE = "#c0661a"
GRAY = "#4b5563"
GREEN = "#2f7d5b"
RED = "#a8323a"
PURPLE = "#6b4fa0"
TEAL = "#1f7a80"
MUTE = "#8a919e"   # text and lines outside boxes
ACC = "#d9661f"    # accent text outside boxes
SANS = "Roboto, 'Helvetica Neue', Arial, sans-serif"
MONO = "'Roboto Mono', 'DejaVu Sans Mono', Menlo, monospace"


class Svg:
    def __init__(self, w, h, title=""):
        self.w, self.h = w, h
        self.parts = []
        self.title = title

    def add(self, s):
        self.parts.append(s)

    def text(self, x, y, s, size=14, color=MUTE, anchor="middle", weight=400, mono=False, italic=False):
        lines = s.split("\n") if isinstance(s, str) else s
        fam = MONO if mono else SANS
        st = ' font-style="italic"' if italic else ""
        for i, ln in enumerate(lines):
            self.add(f'<text x="{x}" y="{y + i * size * 1.25:.1f}" font-family="{fam}" font-size="{size}" '
                     f'fill="{color}" text-anchor="{anchor}" font-weight="{weight}"{st}>{escape(ln)}</text>')

    def box(self, x, y, w, h, label="", fill=BLUE, size=14, mono=False, sub=None, subsize=None, r=6,
            stroke=None, color="#ffffff", weight=600, opacity=1.0, dash=False):
        """x, y = top-left. label may have newlines. sub = smaller second block."""
        sk = f' stroke="{stroke}" stroke-width="1.5"' if stroke else ""
        if dash:
            sk += ' stroke-dasharray="5 4"'
        self.add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" fill-opacity="{opacity}"{sk}/>')
        lines = label.split("\n") if label else []
        sublines = sub.split("\n") if sub else []
        ss = subsize or round(size * 0.82)
        total = len(lines) * size * 1.25 + len(sublines) * ss * 1.25
        cy = y + h / 2 - total / 2 + size * 0.95
        for ln in lines:
            self.text(x + w / 2, cy, ln, size, color, "middle", weight, mono)
            cy += size * 1.25
        cy += (ss - size) * 0.2
        for ln in sublines:
            self.text(x + w / 2, cy, ln, ss, color, "middle", 400, mono)
            cy += ss * 1.25

    def frame(self, x, y, w, h, label="", color=MUTE, size=13, r=10, dash=True, anchor="start"):
        d = ' stroke-dasharray="6 5"' if dash else ""
        self.add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="none" stroke="{color}" stroke-width="1.5"{d}/>')
        if label:
            tx = x + 12 if anchor == "start" else x + w / 2
            self.text(tx, y + size + 6, label, size, color, anchor, 600)

    def line(self, x1, y1, x2, y2, color=MUTE, width=2, dash=False, arrow=True, both=False):
        d = ' stroke-dasharray="6 5"' if dash else ""
        mid = self._marker(color)
        me = f' marker-end="url(#{mid})"' if arrow else ""
        ms = f' marker-start="url(#{mid}s)"' if both else ""
        self.add(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" stroke-width="{width}"{d}{me}{ms}/>')

    def path(self, d, color=MUTE, width=2, dash=False, arrow=True, fill="none"):
        ds = ' stroke-dasharray="6 5"' if dash else ""
        mid = self._marker(color)
        me = f' marker-end="url(#{mid})"' if arrow else ""
        self.add(f'<path d="{d}" fill="{fill}" stroke="{color}" stroke-width="{width}"{ds}{me}/>')

    _markers = None

    def _marker(self, color):
        if self._markers is None:
            self._markers = {}
        mid = "m" + color.strip("#")
        self._markers[mid] = color
        return mid

    def rect(self, x, y, w, h, fill=MUTE, opacity=1.0, r=0, stroke=None):
        sk = f' stroke="{stroke}" stroke-width="1"' if stroke else ""
        self.add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill}" fill-opacity="{opacity}"{sk}/>')

    def circle(self, cx, cy, r, fill=ORANGE, stroke=None):
        sk = f' stroke="{stroke}" stroke-width="1.5"' if stroke else ""
        self.add(f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{fill}"{sk}/>')

    def svg(self):
        defs = ""
        for mid, c in (self._markers or {}).items():
            defs += (f'<marker id="{mid}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
                     f'<path d="M0,0 L10,5 L0,10 z" fill="{c}"/></marker>'
                     f'<marker id="{mid}s" viewBox="0 0 10 10" refX="1" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
                     f'<path d="M10,0 L0,5 L10,10 z" fill="{c}"/></marker>')
        t = f"<title>{escape(self.title)}</title>" if self.title else ""
        return (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {self.w} {self.h}" width="{self.w}" height="{self.h}" '
                f'role="img">{t}<defs>{defs}</defs>' + "\n".join(self.parts) + "</svg>\n")

    def save(self, path):
        with open(path, "w") as f:
            f.write(self.svg())
        return path


FIG = str(__import__("pathlib").Path(__file__).resolve().parents[2] / "book" / "media" / "figures") + "/"
