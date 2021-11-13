from tqdm import tqdm
import json
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import time


def create_spark_session():
    """Create a Spark Session"""
    _ = load_dotenv()
    return (
        SparkSession
        .builder
        .appName("ExperiMind")
        .master("local[5]")
        .getOrCreate()
    )


spark = create_spark_session()
spark.conf.set("spark.sql.caseSensitive", "true")
PATH_BIGDATA = 'data/raw/Toys_and_Games_5.json'
raw_sdf = spark.read.json(PATH_BIGDATA)


# def read_json_to_sdf(path: str):
#     data = []
#     with open(path, 'r') as f:
#         # i = 0
#         for line in tqdm(f):
#             data.append(json.loads(line))
#             # i += 1
#             # if i == 5000:
#             #     break
#     df = spark.sparkContext.parallelize(data).map(lambda d: json.dumps(data))
#     df = spark.read.json(df)
#     return df


# raw_sdf = read_json_to_sdf(PATH_BIGDATA)


COL_NAME_MAP = {
    "overall": "overall",
    "verified": "verified",
    "reviewTime": "review_time",
    "reviewerID": "reviewer_id",
    "asin": "asin",
    "reviewerName": "reviewer_name",
    "reviewText": "review_text",
    "summary": "summary",
    "unixReviewTime": "unix_review_time",
    "style": "style",
    "vote": "vote",
    "image": "image"
}


def rename_columns(df, column_map):
    for old, new in column_map.items():
        df = df.withColumnRenamed(old, new)
    return df


raw_sdf = rename_columns(raw_sdf, COL_NAME_MAP)

SELECTED_COLUMNS = [
    "reviewer_id",
    "asin",
    "review_text",
    "summary",
    "verified",
    "overall",
    "vote",
    "unix_review_time",
    "review_time",
]

raw_sdf = raw_sdf.select(*SELECTED_COLUMNS)


def create_path_snapshot():
    path_fixed = 'data/snapshot/pyspark/snapshot_{}'
    current_unix_time = int(time.time())
    return path_fixed.format(current_unix_time)


PATH_SNAPSHOT = create_path_snapshot()

raw_sdf = raw_sdf.repartition("asin").sortWithinPartitions("unix_review_time")
raw_sdf.write.partitionBy("asin").mode("overwrite").parquet(PATH_SNAPSHOT)