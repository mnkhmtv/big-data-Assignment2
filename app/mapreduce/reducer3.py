#!/usr/bin/env python3
import sys

docs = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) != 4:
        continue
    _, doc_id, doc_length, title = parts
    docs.append((doc_id, int(doc_length), title))

n = len(docs)
avg_dl = sum(dl for _, dl, _ in docs) / n if n > 0 else 0
print(f"N\t{n}")
print(f"AVG_DL\t{avg_dl:.4f}")
for doc_id, dl, title in docs:
    print(f"DOC\t{doc_id}\t{dl}\t{title}")
