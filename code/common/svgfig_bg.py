"""A small SVG helper for the book's figures: one drawing function, two files (light and dark).

A figure is a function f(c) -> list of SVG elements, where c is a palette. write() calls it with
the light palette and the dark palette and saves book/media/figures/<name>-light.svg and
<name>-dark.svg. Embed them with #only-light and #only-dark so each mode gets its own file.
"""
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "book" / "media" / "figures"

LIGHT = dict(bg="#ffffff", fg="#1f2328", mute="#5b6370", line="#9aa1ab", panel="#f3f4f6",
             acc="#c0661a", blue="#3a5f96", green="#2f7d4f", purple="#6b4a9b", red="#b42318",
             acc_soft="#f6e3d3", blue_soft="#dbe5f3", green_soft="#d9efe1", purple_soft="#e8e0f3",
             grey_soft="#e5e7eb")
DARK = dict(bg="#1e2129", fg="#e6e8eb", mute="#a3a9b3", line="#5c6370", panel="#272b34",
            acc="#e08a3e", blue="#7aa2e0", green="#5fbf86", purple="#a88ad8", red="#f07167",
            acc_soft="#4a3020", blue_soft="#24344f", green_soft="#1f3f2c", purple_soft="#352a4a",
            grey_soft="#343843")
FONT = "Inter, 'DM Sans', 'Helvetica Neue', Arial, sans-serif"
MONO = "'JetBrains Mono', 'DejaVu Sans Mono', Menlo, monospace"


def text(x, y, s, c, size=14, color=None, weight=400, anchor="middle", mono=False, italic=False):
    fam = MONO if mono else FONT
    style = ' font-style="italic"' if italic else ""
    return (f'<text x="{x}" y="{y}" font-family="{fam}" font-size="{size}" font-weight="{weight}" '
            f'fill="{color or c["fg"]}" text-anchor="{anchor}"{style}>{escape(str(s))}</text>')


def rect(x, y, w, h, c, fill=None, stroke=None, r=8, sw=1.5, dash=None):
    d = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" fill="{fill or c["panel"]}" '
            f'stroke="{stroke or c["line"]}" stroke-width="{sw}"{d}/>')


def box(x, y, w, h, c, label, sub=None, fill=None, stroke=None, size=14, color=None, r=8, dash=None):
    out = [rect(x, y, w, h, c, fill, stroke, r, dash=dash)]
    if sub:
        out.append(text(x + w / 2, y + h / 2 - 3, label, c, size, color, 600))
        out.append(text(x + w / 2, y + h / 2 + size, sub, c, size - 3, c["mute"]))
    else:
        out.append(text(x + w / 2, y + h / 2 + size / 3, label, c, size, color, 600))
    return out


def arrow(x1, y1, x2, y2, c, color=None, sw=1.8, dash=None, head=True):
    col = color or c["mute"]
    d = f' stroke-dasharray="{dash}"' if dash else ""
    out = [f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{col}" stroke-width="{sw}"{d}/>']
    if head:
        import math
        a = math.atan2(y2 - y1, x2 - x1)
        p1 = (x2 - 9 * math.cos(a - 0.4), y2 - 9 * math.sin(a - 0.4))
        p2 = (x2 - 9 * math.cos(a + 0.4), y2 - 9 * math.sin(a + 0.4))
        out.append(f'<polygon points="{x2},{y2} {p1[0]:.1f},{p1[1]:.1f} {p2[0]:.1f},{p2[1]:.1f}" fill="{col}"/>')
    return out


def cylinder(x, y, w, h, c, label, fill=None, stroke=None, size=13):
    """A disk symbol."""
    f, s = fill or c["grey_soft"], stroke or c["line"]
    e = 7
    return [f'<path d="M{x},{y + e} v{h - 2 * e} a{w / 2},{e} 0 0 0 {w},0 v{-(h - 2 * e)}" fill="{f}" stroke="{s}" stroke-width="1.5"/>',
            f'<ellipse cx="{x + w / 2}" cy="{y + e}" rx="{w / 2}" ry="{e}" fill="{f}" stroke="{s}" stroke-width="1.5"/>',
            text(x + w / 2, y + h / 2 + 8, label, c, size, c["fg"], 600)]


def flat(items):
    for i in items:
        if isinstance(i, (list, tuple)):
            yield from flat(i)
        else:
            yield i


def write(name, w, h, draw):
    """Save <name>-light.svg and <name>-dark.svg. The background is transparent."""
    OUT.mkdir(parents=True, exist_ok=True)
    paths = []
    for mode, c in (("light", LIGHT), ("dark", DARK)):
        body = "\n".join(flat(draw(c)))
        svg = (f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" width="{w}" height="{h}">\n'
               f'{body}\n</svg>\n')
        p = OUT / f"{name}-{mode}.svg"
        p.write_text(svg)
        paths.append(p)
    return paths
