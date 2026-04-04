from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession
import os
import subprocess
import shutil


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()

sc = spark.sparkContext

df = spark.read.parquet("/a.parquet")
n = 100
df = df.select(['id', 'title', 'text']).sample(
    fraction=100 * n / df.count(), seed=0).limit(n)
df = df.filter(df.text.isNotNull() & (df.text != ""))

if os.path.exists("data"):
    shutil.rmtree("data")
os.makedirs("data", exist_ok=True)


def create_doc(row):
    filename = "data/" + \
        sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])


df.foreach(create_doc)

subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/data"])
subprocess.run(["hdfs", "dfs", "-put", "data", "/"])

files_rdd = sc.wholeTextFiles("hdfs:///data/")

def parse_doc(pair):
    path, content = pair
    fname = path.split("/")[-1].replace(".txt", "")
    sep = fname.find("_")
    doc_id = fname[:sep]
    doc_title = fname[sep + 1:].replace("_", " ")
    text = content.replace("\t", " ").replace("\n", " ").strip()
    return doc_id + "\t" + doc_title + "\t" + text

subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"])
files_rdd.map(parse_doc).coalesce(1).saveAsTextFile("hdfs:///input/data")