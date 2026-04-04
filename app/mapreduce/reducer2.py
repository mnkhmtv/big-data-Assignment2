#!/usr/bin/env python3
import sys

current_word = None
postings = []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue
    word, posting = parts
    if word == current_word:
        postings.append(posting)
    else:
        if current_word is not None:
            df = len(postings)
            print(f"{current_word}\t{df}\t{','.join(postings)}")
        current_word = word
        postings = [posting]

if current_word is not None:
    df = len(postings)
    print(f"{current_word}\t{df}\t{','.join(postings)}")
