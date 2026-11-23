#!/usr/bin/env python3
"""Shorten long static waits in terminal clips and remake posters from the last frame.

Interior stretches where no pixel changes for more than LONG seconds (a command
that runs before its output appears) are cut down to KEEP seconds. The last
stretch (the final hold for reading) is kept. Animations (*-anim-*) are skipped.
Clips whose captions describe a real wait (ch28-retry, ch50-clock) were left as filmed.
Usage: tighten_clips.py [clip.mp4 ...]   (default: every film clip in book/media/clips)"""
import glob, os, re, subprocess, sys
LONG, KEEP = 6.0, 3.0
D = os.path.join(os.path.dirname(__file__), '..', 'book', 'media', 'clips')
def run(*a): return subprocess.run(a, capture_output=True, text=True)
def duration(f): return float(run('ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'csv=p=0', f).stdout)
def freezes(f):
    err = run('ffmpeg', '-v', 'info', '-i', f, '-vf', f'freezedetect=n=0.001:d={LONG}', '-map', '0:v', '-f', 'null', '-').stderr
    s = [float(x) for x in re.findall(r'freeze_start: ([0-9.]+)', err)]
    e = [float(x) for x in re.findall(r'freeze_end: ([0-9.]+)', err)]
    return list(zip(s, e))
def poster(f):
    d = duration(f)
    run('ffmpeg', '-v', 'error', '-y', '-ss', f'{max(d - 0.5, 0):.2f}', '-i', f, '-frames:v', '1', '-c:v', 'libwebp', '-quality', '80', f[:-4] + '.webp')
clips = sys.argv[1:] or sorted(c for c in glob.glob(os.path.join(D, 'ch*.mp4')) if '-anim-' not in c)
for f in clips:
    d = duration(f)
    cut = [(s + KEEP, e) for s, e in freezes(f) if e < d - 0.5]
    if cut:
        expr = '*'.join(f'not(between(t,{a:.3f},{b:.3f}))' for a, b in cut)
        tmp = f[:-4] + '.tmp.mp4'
        r = run('ffmpeg', '-v', 'error', '-y', '-i', f, '-vf', f"select='{expr}',setpts=N/FRAME_RATE/TB",
                '-an', '-c:v', 'libx264', '-crf', '26', '-preset', 'slow', '-pix_fmt', 'yuv420p', '-movflags', '+faststart', tmp)
        if r.returncode: print('FAIL', f, r.stderr[-300:]); continue
        os.replace(tmp, f)
        print(f'{os.path.basename(f)}: {d:.0f}s -> {duration(f):.0f}s')
    poster(f)
