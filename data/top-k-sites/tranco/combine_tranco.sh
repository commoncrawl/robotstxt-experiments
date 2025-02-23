#!/bin/bash

DIR=$(dirname $0)

for l in "$DIR"/*-subdomain/*; do
    zcat "$l"
done \
    | sort -t, -k2,2 \
    | python3 "$DIR"/../combine_ranked_lists.py \
    | sort -t, -k1,1gr \
    | cut -d, -f2 \
    | perl -lne 'print $., "\t", $_' \
    | gzip >"$DIR"/tranco_combined.txt.gz