#!/usr/bin/env python3
"""Re-film the terminal clips from their tapes and cut each clip from its beats.

For each tape: `film render <tape> --format chapter` (under the bench lock), then each clip
is its beat clips joined and encoded into book/media/clips/, then tighten_clips.py shortens
long static waits and makes the poster from the last frame.
Most clips are one beat with the same id as the clip name (ch79-where is beat `where` of
ch79.tape); EXTRA lists the others. The Chapter 42 UI clips come from a browser
(film/ch42_ui.py), not a tape, and are not re-filmed here.
Usage: refilm.py [tape-stem ...]   (default: every tape that has clips)"""
import re, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FILM, CLIPS = ROOT / 'code' / 'film', ROOT / 'book' / 'media' / 'clips'
KEEP_WAITS = {'ch28-retry', 'ch50-clock'}       # captions describe a real wait
SKIP = {'ch42-ui-stages', 'ch42-ui-storage-sql'}  # browser clips
EXTRA = {                                        # clip: (tape stem, [beat ids])
    'ch12-rdds': ('ch12', None), 'ch13-pandas': ('ch13', None), 'ch14-datasets': ('ch14', None),
    'ch34-unnest': ('ch28u', ['ch34-unnest']),
    'ch40-aqe-join': ('ch40', ['join']),
    'ch45-orc': ('ch45-orc', ['orc']), 'ch45-crypto': ('ch45-orc', ['crypto']),
    'ch48-restart': ('ch48', ['crash', 'resume']),
    'ch63-barrier': ('ch63-barrier', ['barrier']),
}

def beats(tape):
    return re.findall(r'^# beat: *([A-Za-z0-9_-]+)', (FILM / f'{tape}.tape').read_text(), re.M)

def plan():
    """{tape stem: [(clip, [beat ids])]}"""
    out = {}
    for mp4 in sorted(CLIPS.glob('ch*.mp4')):
        clip = mp4.stem
        if '-anim-' in clip or clip in SKIP:
            continue
        tape, ids = EXTRA.get(clip, (clip.split('-')[0], None))
        if clip not in EXTRA:
            suffix = clip.split('-', 1)[1]
            ids = [b for b in (suffix, clip) if b in beats(tape)][:1]
            if not ids:
                sys.exit(f'{clip}: no beat {suffix!r} in {tape}.tape; add it to EXTRA')
        out.setdefault(tape, []).append((clip, ids or beats(tape)))
    return out

def run(*a, **k):
    return subprocess.run(a, **k)

def cut(tape, clip, ids):
    order = beats(tape)
    parts = [FILM / 'renders' / 'chapter' / f'{order.index(b) + 1:02d}-{b}.mp4' for b in ids]
    lst = FILM / 'renders' / f'{clip}.concat.txt'
    lst.write_text(''.join(f"file '{p}'\n" for p in parts))
    dst = CLIPS / f'{clip}.mp4'
    r = run('ffmpeg', '-v', 'error', '-y', '-f', 'concat', '-safe', '0', '-i', str(lst), '-an',
            '-c:v', 'libx264', '-crf', '26', '-preset', 'slow', '-pix_fmt', 'yuv420p',
            '-movflags', '+faststart', str(dst))
    lst.unlink()
    return r.returncode == 0

def main():
    p = plan()
    tapes = sys.argv[1:] or list(p)
    for tape in tapes:
        print(f'== {tape}', flush=True)
        r = run('flock', str(ROOT / 'data' / '.bench.lock'),
                'film', 'render', str(FILM / f'{tape}.tape'), '--format', 'chapter')
        if r.returncode:
            print(f'FAIL render {tape}', flush=True)
            continue
        done = [c for c, ids in p[tape] if cut(tape, c, ids)]
        for c, _ in p[tape]:
            if c not in done:
                print(f'FAIL cut {c}', flush=True)
        tight = [str(CLIPS / f'{c}.mp4') for c in done if c not in KEEP_WAITS]
        keep = [c for c in done if c in KEEP_WAITS]
        if tight:
            run(sys.executable, str(ROOT / 'scripts' / 'tighten_clips.py'), *tight)
        for c in keep:   # poster only
            f = str(CLIPS / f'{c}.mp4')
            d = float(run('ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of',
                          'csv=p=0', f, capture_output=True, text=True).stdout)
            run('ffmpeg', '-v', 'error', '-y', '-ss', f'{max(d - 0.5, 0):.2f}', '-i', f,
                '-frames:v', '1', '-c:v', 'libwebp', '-quality', '80', f[:-4] + '.webp')
        print(f'OK {tape}: {" ".join(done)}', flush=True)

if __name__ == '__main__':
    main()
