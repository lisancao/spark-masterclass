# Sourced, off camera, by code/film/ch44.tape: a package folder for Step 1 and a filter that
# shortens the long paths in the output so that the lines fit the clip.
rm -rf /tmp/pkg && mkdir -p /tmp/pkg/messytools && echo 'VERSION = "1.0"' > /tmp/pkg/messytools/__init__.py
CODE=$PWD
short() {
  sed -E "s#$CODE/.venv/lib/python3.12/site-packages/pyspark/python/lib/#<pyspark>/lib/#; s#$CODE#<code>#g; s#/tmp/spark-[^/]*/userFiles-[^ ]*#<tmp>/userFiles#; s#/tmp/pkg#<pkg>#g"
}
