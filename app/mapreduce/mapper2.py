#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t")
    if len(parts) != 4:
        continue

    word, doc_id, tf, doc_length = parts
    print(f"{word}\t{doc_id}:{tf}:{doc_length}")
