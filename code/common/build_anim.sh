#!/usr/bin/env bash
# Render one book animation: code/chNN/anim/<name>/index.html -> book/media/clips/chNN-anim-<name>.{mp4,webp}
# Usage: code/common/build_anim.sh ch02 drivers [poster_second]
# Needs the create-animation kit (`anim` on PATH). Renders landscape, paper theme, then scales to 1280x720.
set -euo pipefail
ch=$1; name=$2; poster=${3:-}
root=$(cd "$(dirname "$0")/../.." && pwd)
src=$root/code/$ch/anim/$name
work=${ANIM_WORK:-/tmp/anim-work}/$ch-$name
rm -rf "$work"; mkdir -p "$(dirname "$work")"
anim new "$work" --backend hf --theme paper --title "$name" >/dev/null
cp "$src"/index.html "$work"/
[ -f "$src/storyboard.md" ] && cp "$src/storyboard.md" "$work"/
dur=$(grep -o 'data-duration="[0-9.]*"' "$src/index.html" | head -1 | grep -o '[0-9.]*')
anim render "$work" --formats landscape --themes paper >"$work.log" 2>&1 || { tail -30 "$work.log"; exit 1; }
mp4=$(ls "$work"/renders/*-paper-landscape.mp4)
out=$root/book/media/clips/$ch-anim-$name
ffmpeg -loglevel error -y -i "$mp4" -vf scale=1280:720:flags=lanczos -c:v libx264 -preset slow -crf 26 \
  -pix_fmt yuv420p -an -movflags +faststart "$out.mp4"
p=${poster:-$(python3 -c "print(max(0.5, $dur - 0.6))")}
ffmpeg -loglevel error -y -ss "$p" -i "$out.mp4" -frames:v 1 -c:v libwebp -quality 80 "$out.webp"
ls -la "$out.mp4" "$out.webp"
