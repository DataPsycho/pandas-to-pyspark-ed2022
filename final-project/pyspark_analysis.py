import yaml
import typing as t
from pathlib import Path
from utils.session import create_spark_session
from pyspark.sql import functions as fn
from pyspark.sql.functions import col
from pyspark.sql import DataFrame as SparkDf
from pprint import pprint

CONFIG_PATH = Path("configs/default.yaml")

# TODO: Task1
# It is not a python command but you have to run it  into shell
# Solution: Check the git: data/raw/datadownloader.sh where I download
# all the data using wget and put them into the same directory


# Todo: Task 2 (A) Read the Data
# Extra Function for clarity
def read_config(path: Path) -> t.Dict[t.Any, t.Any]:
    """
    A config file reader where there is all the config file path
    and other stuffs which are pretty much static for the project
    :param path: A Path like object to the location of the file
    :return: A dictionary of json
    """
    with open(path, "r") as stream:
        try:
            config = yaml.safe_load(stream)
            return config
        except yaml.YAMLError as exc:
            print(exc)


CONFIG = read_config(CONFIG_PATH)
spark = create_spark_session()
spark.conf.set("spark.sql.caseSensitive", "true")

raw_sdf_main = spark.read.json(
    CONFIG.get("file_path")["raw_data_hnk_main"]
)


def rename_columns(df, column_map) -> SparkDf:
    """
    Rename the columns of a given dataframe
    :param df: Spark DataFrame input
    :param column_map: New Column map
    :return: DataFrame renamed
    """
    for old, new in column_map.items():
        df = df.withColumnRenamed(old, new)
    return df


raw_sdf_meta = spark.read.json(
    CONFIG.get("file_path")["raw_data_hnk_meta"]
)

# TODO: Task 2 (b) Rename the columns and Keep the relevant
raw_sdf_main = rename_columns(raw_sdf_main, CONFIG["column_rename_map_main"])

raw_sdf_main = raw_sdf_main.select(*CONFIG["selected_column_main"])

# TODO: Task 2 (c) Repartition and save the data
raw_sdf_main = raw_sdf_main.withColumn('reviewed_at', fn.from_unixtime(col('unix_review_time')))
raw_sdf_main = raw_sdf_main.withColumn("reviewed_year", fn.year(col("reviewed_at")))
raw_sdf_main = raw_sdf_main.withColumn("reviewed_month", fn.month(col("reviewed_at")))

raw_sdf_main = raw_sdf_main.repartition('reviewed_year', 'reviewed_month').sortWithinPartitions("asin")
raw_sdf_main.write.mode("overwrite").parquet(CONFIG["file_path"]["snapshot_main"])

# Removing actual data from memory as we will use new snapshot
raw_sdf_main = None

# TODO: Task 3 (a) Read the new snapshot
main_df = spark.read.parquet(CONFIG["file_path"]["snapshot_main"])

# TODO: Impute NaN vote with Zero
main_df = main_df.fillna({'vote': 0})


# Todo: task 3 (b) Show distribution of vote column
def show_vote_stat(df: SparkDf) -> None:

    summary_df = (
        main_df
        .groupby("asin")
        .agg(fn.mean(col("vote")).alias("mean_vote"))
        .select("mean_vote")
        .summary("count", "min", "25%", "75%", "max")
    )

    summary = summary_df.rdd.map(lambda row: row.asDict(recursive=True)).collect()
    pprint(summary)


show_vote_stat(main_df)

# Todo: task 3 (c) Show distribution of review_text column
main_df = main_df.fillna({'review_text': ''})
main_df = main_df.withColumn('review_text_len', fn.length(col('review_text')))


def show_review_text_stat(df: SparkDf) -> None:
    """
    Show general Stats for review text lenght
    :param df: Dataframe
    :return: Nothing
    """
    summary_df = df.select('review_text_len').summary("count", "min", "25%", "75%", "max")
    summary = summary_df.rdd.map(lambda row: row.asDict(recursive=True)).collect()
    print("Review Length Stat")
    pprint(summary)
    weired_reviews = df.filter(col('review_text_len') <= 1).count()
    print(f"Reviews with length one or less: {weired_reviews}")


show_review_text_stat(main_df)


# TODO: Task 4 (a)

median_review_per_product = (
    main_df
    .filter(col("reviewed_year") == 2016)
    .orderBy('asin', ascending=True)
)


