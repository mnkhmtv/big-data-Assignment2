#!/usr/bin/env python3
import sys
import re

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    parts = line.split("\t", 2)
    if len(parts) < 3:
        continue

    doc_id, title, text = parts[0], parts[1], parts[2]
    words = re.findall(r'[a-z]+', text.lower())
    doc_length = len(words)

    print(f"STATS\t{doc_id}\t{doc_length}\t{title}")
