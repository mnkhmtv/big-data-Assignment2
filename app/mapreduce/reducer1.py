#!/usr/bin/env python3
import sys

current_key = None
tf = 0
doc_length = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    key, dl = line.split("\t")
    dl = int(dl)

    if key == current_key:
        tf += 1
    else:
        if current_key is not None:
            word, doc_id = current_key.split("|", 1)
            print(f"{word}\t{doc_id}\t{tf}\t{doc_length}")
        current_key = key
        tf = 1
        doc_length = dl

if current_key is not None:
    word, doc_id = current_key.split("|", 1)
    print(f"{word}\t{doc_id}\t{tf}\t{doc_length}")
