import sys
import re
import math
from pyspark import SparkContext, SparkConf
from cassandra.cluster import Cluster

K1 = 1.0
B = 0.75
TOP_K = 10

def get_query():
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:])
    return input("Enter query:")

def tokenize(text):
    return list(set(re.findall(r'[a-z]+', text.lower())))

def main():
    query = get_query()
    terms = tokenize(query)

    print(f"\nQuery:'{query}'")
    print(f"Terms:{terms}")

    if not terms:
        print("No valid terms in query.")
        return

    cluster = Cluster(['cassandra-server'])
    session = cluster.connect('search_engine')

    n_row = session.execute(
        "SELECT value FROM corpus_stats WHERE key='N'").one()
    avg_dl_row = session.execute(
        "SELECT value FROM corpus_stats WHERE key='AVG_DL'").one()
    N = float(n_row.value) if n_row else 1.0
    avg_dl = float(avg_dl_row.value) if avg_dl_row else 1.0

    print(f"Corpus: N={int(N)}, avg_dl={avg_dl:.2f}")

    all_postings = []
    for term in terms:
        vocab_row = session.execute(
            "SELECT df FROM vocabulary WHERE word=%s", (term,)
        ).one()

        if vocab_row is None:
            print(f"'{term}' - not in vocabulary, skipping")
            continue

        df = vocab_row.df
        rows = session.execute(
            "SELECT doc_id, tf, doc_length FROM inverted_index WHERE word=%s", (term,))
        for row in rows:
            all_postings.append((row.doc_id, term, row.tf, row.doc_length, df))

    cluster.shutdown()

    if not all_postings:
        print("\nNo results found.")
        return

    print(f"Postings collected:{len(all_postings)}")

    conf = SparkConf().setAppName("BM25_Search")
    sc = SparkContext(conf=conf)

    def bm25_score(record):
        doc_id, term, tf, dl, df = record
        idf = math.log(N / df) if df > 0 else 0.0
        num = (K1 + 1) * tf
        denom = K1 * ((1 - B) + B * (dl / avg_dl)) + tf
        return (doc_id, idf * (num / denom))

    top10 = (
        sc.parallelize(all_postings)
        .map(bm25_score)
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
        .take(TOP_K)
    )

    sc.stop()

    cluster2 = Cluster(['cassandra-server'])
    session2 = cluster2.connect('search_engine')

    print(f"\n{'+'*60}")
    print(f"Top {TOP_K} results for: '{query}'")
    print(f"{'+'*60}")
    print(f"{'#':<4} {'doc_id':<15} {'score':<10} title")

    for rank, (doc_id, score) in enumerate(top10, 1):
        info = session2.execute(
            "SELECT title FROM doc_info WHERE doc_id=%s", (doc_id,)
        ).one()
        title = info.title if info else "Unknown"
        print(f"{rank:<4} {doc_id:<15} {score:<10.4f} {title}")

    cluster2.shutdown()

if __name__ == "__main__":
    main()