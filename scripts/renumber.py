#!/usr/bin/env python3
"""Renumber one part's chapters in a worktree: file names, code dirs, clips, tapes and in-text references.
Usage: renumber.py WORKTREE OLD:NEW [OLD:NEW ...]   (only the given chapters are touched)"""
import os, re, subprocess, sys
wt = sys.argv[1]; pairs = [tuple(int(x) for x in a.split(':')) for a in sys.argv[2:]]
m = dict(pairs)
def git(*a): subprocess.run(['git', '-C', wt, *a], check=True)
# 1. rename paths, highest first so 56->59 cannot clobber 59->62
for old, new in sorted(pairs, reverse=True):
    o, n = f'ch{old:02d}', f'ch{new:02d}'
    for root in ['book/chapters', 'book/media/clips', 'code', 'code/film', 'notes']:
        d = os.path.join(wt, root)
        if not os.path.isdir(d): continue
        for name in sorted(os.listdir(d)):
            if name == o or name.startswith(o + '-') or name.startswith(o + '.') or name.startswith(o + '_'):
                git('mv', os.path.join(root, name), os.path.join(root, n + name[len(o):]))
# 2. rewrite references in text files, in one pass per file (so a number is mapped once)
pat_ch = re.compile(r'\bch(\d{2})\b|\bch(\d{2})(?=[-_./])')
pat_word = re.compile(r'\b(Chapters?|chapter|Ch\.?) (\d+)((?:(?:, | and | to |-)\d+)*)')
def sub_ch(mo):
    n = int(mo.group(1) or mo.group(2)); return f'ch{m.get(n, n):02d}'
def sub_word(mo):
    nums = re.sub(r'\d+', lambda x: str(m.get(int(x.group()), int(x.group()))), mo.group(2) + mo.group(3))
    return f'{mo.group(1)} {nums}'
files = subprocess.run(['git', '-C', wt, 'ls-files'], capture_output=True, text=True).stdout.split()
changed = 0
SHARED = {'notes/assignments.md', 'notes/renumber.md', 'notes/chapter-brief.md', 'book/roadmap.md', 'book/coverage.md', 'mkdocs.yml', 'README.md', 'NOTES.md'}
for f in files:
    if f in SHARED: continue
    p = os.path.join(wt, f)
    if not re.search(r'\.(md|py|sh|tape|tsv|txt|yml|yaml|toml|java|scala)$', f): continue
    try: s = open(p).read()
    except Exception: continue
    t = pat_word.sub(sub_word, pat_ch.sub(sub_ch, s))
    if t != s: open(p, 'w').write(t); changed += 1
print(f'renamed and rewrote {changed} files')
