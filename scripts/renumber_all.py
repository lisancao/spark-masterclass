#!/usr/bin/env python3
"""Renumber chapters with any permutation (two-phase renames), then rewrite chNN tokens and
"Chapter N" references in one pass per file. Usage: renumber_all.py WORKTREE OLD:NEW ...
Shared files (nav, roadmap, coverage, README) are skipped; the integrator rewrites them."""
import os, re, subprocess, sys
wt = sys.argv[1]; m = dict(tuple(int(x) for x in a.split(':')) for a in sys.argv[2:])
assert len(set(m.values())) == len(m), 'targets must be unique'
ROOTS = ['book/chapters', 'book/media/clips', 'book/media/figures', 'code', 'code/film', 'code/figures', 'notes']
def git(*a): subprocess.run(['git', '-C', wt, *a], check=True)
def moves(src_fmt, dst_fmt, pairs):
    for old, new in pairs:
        o, n = src_fmt.format(old), dst_fmt.format(new)
        for root in ROOTS:
            d = os.path.join(wt, root)
            if not os.path.isdir(d): continue
            for name in sorted(os.listdir(d)):
                if name == o or any(name.startswith(o + c) for c in '-._'):
                    git('mv', os.path.join(root, name), os.path.join(root, n + name[len(o):]))
pairs = [(o, n) for o, n in m.items() if o != n]
moves('ch{:02d}', 'zz{:02d}', [(o, o) for o, _ in pairs])
moves('zz{:02d}', 'ch{:02d}', pairs)
pat_ch = re.compile(r'\bch(\d{2})\b|\bch(\d{2})(?=[-_./])')
pat_word = re.compile(r'\b(Chapters?|chapter|Ch\.?) (\d+)((?:(?:, | and | or | to |-)\d+)*)')
sub_ch = lambda mo: f'ch{m.get(int(mo.group(1) or mo.group(2)), int(mo.group(1) or mo.group(2))):02d}'
def sub_word(mo):
    return mo.group(1) + ' ' + re.sub(r'\d+', lambda x: str(m.get(int(x.group()), int(x.group()))), mo.group(2) + mo.group(3))
SHARED = {'notes/assignments.md', 'notes/renumber.md', 'notes/chapter-brief.md', 'book/roadmap.md', 'book/coverage.md', 'mkdocs.yml', 'README.md', 'NOTES.md', 'book/index.md'}
changed = 0
for f in subprocess.run(['git', '-C', wt, 'ls-files'], capture_output=True, text=True).stdout.split():
    if f in SHARED or not re.search(r'\.(md|py|sh|tape|tsv|txt|yml|yaml|toml|java|scala|json|html|rs|swift|go|js|mjs)$', f): continue
    p = os.path.join(wt, f)
    try: s = open(p).read()
    except Exception: continue
    t = pat_word.sub(sub_word, pat_ch.sub(sub_ch, s))
    if t != s: open(p, 'w').write(t); changed += 1
print(f'rewrote {changed} files')
