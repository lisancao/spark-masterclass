#!/usr/bin/env bash
# Cut book clips from per-beat film renders: each row of a clips TSV (clip, source, start, end, poster_at)
# becomes book/media/clips/<clip>.mp4 (1280x720, no audio) and a .webp poster.
#   code/common/cut_clips.sh book/media/clips/clips-connect-clients.tsv
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
tail -n +2 "$1" | while IFS=$'\t' read -r clip src start end poster _; do
  in=$root/code/film/renders/$src; out=$root/book/media/clips/$clip
  ffmpeg -loglevel error -y -ss "$start" -to "$end" -i "$in" -vf scale=1280:720 -c:v libx264 -preset slow -crf 28 \
    -pix_fmt yuv420p -an -movflags +faststart "$out.mp4" </dev/null
  ffmpeg -loglevel error -y -ss "$poster" -i "$out.mp4" -frames:v 1 -c:v libwebp -quality 80 "$out.webp" </dev/null
  ls -la "$out.mp4" | awk '{print $5, $NF}'
done
