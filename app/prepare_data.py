from pyspark.sql import SparkSession
import os
import subprocess
import shutil
from pathvalidate import sanitize_filename

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()
sc = spark.sparkContext

if os.path.exists("a.parquet"):

    df = spark.read.parquet("/a.parquet")
    n  = 100
    df = df.select(['id', 'title', 'text']) \
           .sample(fraction=100 * n / df.count(), seed=0) \
           .limit(n)
    df = df.filter(df.text.isNotNull() & (df.text != ""))

    if os.path.exists("data"):
        shutil.rmtree("data")
    os.makedirs("data", exist_ok=True)

    def create_doc(row):
        filename = (
            "data/"
            + sanitize_filename(str(row['id']) + "_" + row['title'])
              .replace(" ", "_")
            + ".txt"
        )
        with open(filename, "w") as f:
            f.write(row['text'])

    df.foreach(create_doc)

else:
    if not os.path.exists("data") or len(os.listdir("data")) == 0:
        print("[ERROR] data/ folder is empty and a.parquet is missed")
        import sys; sys.exit(1)

subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/data"])
subprocess.run(["hdfs", "dfs", "-put", "data", "/"])

def parse_doc(pair):
    path, content = pair
    fname  = path.split("/")[-1].replace(".txt", "")
    sep    = fname.find("_")
    if sep == -1:
        return None
    doc_id    = fname[:sep]
    doc_title = fname[sep + 1:].replace("_", " ")
    text      = content.replace("\t", " ").replace("\n", " ").strip()
    if not text:
        return None
    return doc_id + "\t" + doc_title + "\t" + text

subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"])

files_rdd = sc.wholeTextFiles("hdfs:///data/")
files_rdd \
    .map(parse_doc) \
    .filter(lambda x: x is not None) \
    .coalesce(1) \
    .saveAsTextFile("hdfs:///input/data")

print("DONE DATA PREPARATION")