#!/usr/bin/env bash
# Chapter 77, Step 15: a GPU discovery script. Spark runs it and reads one JSON object from stdout.
# A real script asks the hardware, for example:
#   nvidia-smi --query-gpu=index --format=csv,noheader | sed -e ':a' -e 'N' -e'$!ba' -e 's/\n/","/g'
# This laptop has no GPU for Spark to use, so the script reports two pretend addresses.
echo '{"name": "gpu", "addresses": ["0", "1"]}'
