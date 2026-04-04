from cassandra.cluster import Cluster
import subprocess
import time
import sys

CASSANDRA_HOST = 'cassandra-server'
KEYSPACE = 'search_engine'
MAX_RETRIES = 15
RETRY_DELAY = 10

def wait_for_cassandra():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local")
            print(f"[OK] Cassandra ready (attempt {attempt}/{MAX_RETRIES})")
            return cluster, session
        except Exception as e:
            print(
                f"[WAIT] Cassandra not ready (attempt {attempt}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_DELAY)
    print("[ERROR] Could not connect to Cassandra after max retries. Exiting.")
    sys.exit(1)

def create_schema(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace(KEYSPACE)

    session.execute("""
        CREATE TABLE IF NOT EXISTS vocabulary (
            word TEXT PRIMARY KEY,
            df   INT
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS inverted_index (
            word       TEXT,
            doc_id     TEXT,
            tf         INT,
            doc_length INT,
            PRIMARY KEY (word, doc_id)
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS corpus_stats (
            key   TEXT PRIMARY KEY,
            value DOUBLE
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS doc_info (
            doc_id     TEXT PRIMARY KEY,
            title      TEXT,
            doc_length INT
        )
    """)
    print("[OK] Schema created.")

def truncate_tables(session):
    tables = ['vocabulary', 'inverted_index', 'corpus_stats', 'doc_info']
    for table in tables:
        session.execute(f"TRUNCATE {KEYSPACE}.{table}")
        print(f"  [TRUNCATE] {table}")
    print("[OK] All tables truncated.")


def read_hdfs(glob_path):
    result = subprocess.run(
        f'hdfs dfs -cat {glob_path}',
        shell=True,
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        print(
            f"[WARNING] Could not read {glob_path}:\n{result.stderr.strip()}")
        return []
    return [line for line in result.stdout.split('\n') if line.strip()]

def load_corpus_stats(session):
    print("\n[STEP] Loading corpus stats from /indexer/stats/part-* ...")
    lines = read_hdfs('/indexer/stats/part-*')
    if not lines:
        print("[ERROR] No stats data found. Did Pipeline 3 run successfully?")
        sys.exit(1)

    stmt_corpus = session.prepare(
        "INSERT INTO corpus_stats (key, value) VALUES (?, ?)")
    stmt_doc_info = session.prepare(
        "INSERT INTO doc_info (doc_id, title, doc_length) VALUES (?, ?, ?)"
    )

    N = 0
    avg_dl = 0.0
    doc_count = 0

    for line in lines:
        parts = line.split('\t')
        if parts[0] == 'N' and len(parts) == 2:
            try:
                N = int(parts[1])
            except ValueError:
                print(f"[WARNING] Bad N value: {line}")
        elif parts[0] == 'AVG_DL' and len(parts) == 2:
            try:
                avg_dl = float(parts[1])
            except ValueError:
                print(f"[WARNING] Bad AVG_DL value: {line}")
        elif parts[0] == 'DOC' and len(parts) == 4:
            _, doc_id, dl_str, title = parts
            try:
                dl = int(dl_str)
                session.execute(stmt_doc_info, (doc_id, title, dl))
                doc_count += 1
            except Exception as e:
                print(f"[WARNING] Could not insert doc_info for {doc_id}: {e}")

    session.execute(stmt_corpus, ('N', float(N)))
    session.execute(stmt_corpus, ('AVG_DL', avg_dl))
    print(
        f"[OK] Corpus stats: N={N}, avg_dl={avg_dl:.4f}, docs_loaded={doc_count}")
    return N, avg_dl

def load_inverted_index(session):
    print("\n[STEP] Loading inverted index from /indexer/index/part-* ...")
    lines = read_hdfs('/indexer/index/part-*')
    if not lines:
        print("[ERROR] No index data found. Did Pipeline 2 run successfully?")
        sys.exit(1)

    print(f"  Index lines to process: {len(lines)}")

    stmt_vocab = session.prepare(
        "INSERT INTO vocabulary (word, df) VALUES (?, ?)")
    stmt_index = session.prepare(
        "INSERT INTO inverted_index (word, doc_id, tf, doc_length) VALUES (?, ?, ?, ?)"
    )

    word_count = 0
    posting_count = 0
    skipped_lines = 0
    skipped_posts = 0

    for line in lines:
        parts = line.split('\t')
        if len(parts) != 3:
            skipped_lines += 1
            continue

        word, df_str, postings_str = parts
        try:
            df = int(df_str)
        except ValueError:
            skipped_lines += 1
            continue

        try:
            session.execute(stmt_vocab, (word, df))
        except Exception as e:
            print(f"[WARNING] vocab insert failed for '{word}': {e}")
            continue

        for posting in postings_str.split(','):
            p = posting.split(':')
            if len(p) != 3:
                skipped_posts += 1
                continue
            doc_id, tf_str, dl_str = p
            try:
                tf = int(tf_str)
                dl = int(dl_str)
                session.execute(stmt_index, (word, doc_id, tf, dl))
                posting_count += 1
            except Exception:
                skipped_posts += 1

        word_count += 1
        if word_count % 1000 == 0:
            print(f"  ... {word_count} words, {posting_count} postings loaded")

    print(f"[OK] Index loaded: {word_count} words, {posting_count} postings")
    if skipped_lines > 0:
        print(f"[WARN] Skipped lines (bad format): {skipped_lines}")
    if skipped_posts > 0:
        print(f"[WARN] Skipped postings (bad format): {skipped_posts}")
    return word_count, posting_count


def main():
    print("+" * 60)
    print(" Storing index to Cassandra")
    print("+" * 60)

    cluster, session = wait_for_cassandra()
    create_schema(session)
    truncate_tables(session)

    N, avg_dl = load_corpus_stats(session)
    word_count, _ = load_inverted_index(session)

    cluster.shutdown()

    print("+" * 60)
    print(f"DONE")
    print(f"Words in vocabulary:{word_count}")
    print(f"Total documents:{N}")
    print(f"Avg document length:{avg_dl:.2f}")
    print("+" * 60)


if __name__ == "__main__":
    main()