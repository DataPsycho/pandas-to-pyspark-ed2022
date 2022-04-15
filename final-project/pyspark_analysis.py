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
# Solution: Check the git: data/raw/datadownloader.sh shell script, where I used wget cli app
# I download all the data and put them into the same directory


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
    """
    Show summary status about the vote
    :param df: A Dataframe having asin and vote column
    :return: No Return
    """
    summary_df = (
        df
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
    summary_df = (
        df
        .filter(col("review_text_len") > 0)
        .select('review_text_len')
        .summary("count", "min", "25%", "75%", "max")
    )
    summary = summary_df.rdd.map(lambda row: row.asDict(recursive=True)).collect()
    print("Review Length Stat")
    pprint(summary)
    weired_reviews = df.filter(col('review_text_len') <= 1).count()
    print(f"Reviews with length one or less: {weired_reviews}")


show_review_text_stat(main_df)


# TODO: Task 4 (a) Create count on aggregated data
total_review_per_product_2016_df = (
    main_df
    .filter(col("reviewed_year") == 2016)
    .select("asin")
    .groupby("asin")
    .count()
    .orderBy("count", ascending=False)
)


# TODO: Task 4 (b) calculate actual result on the aggregation
def compare_median_top_vs_overall(df: SparkDf) -> None:
    """
    Calculate median review up on giving a grouped dataframe
    :param df: Input Grouped dataframe
    :return: No Return
    """
    median_2016 = (
        df
        .select(fn.percentile_approx('count', 0.5).alias('median'))
        # rdd return a list of [Row(median=<value>)] object using the map
        # reduction we are selecting the median field of the row object
        .rdd.map(lambda row: row["median"]).collect()[0]
    )

    percentile_2016 = (
        df
        .select('count')
        .summary("75%")
        # The count column has the value of 75th percentile
        .rdd.map(lambda row: int(row["count"])).collect()[0]
    )

    median_2016_top = (
        df
        .filter(col("count") > percentile_2016)
        .select(fn.percentile_approx('count', 0.5).alias('median'))
        .rdd.map(lambda row: row["median"]).collect()[0]
    )
    print("Median Over View 2016 Report")
    print("=============================")
    print(f"median Over All:\t{median_2016}")
    print("=============================")
    print(f"median Top Product:\t{median_2016_top}")
    print("=============================")


compare_median_top_vs_overall(total_review_per_product_2016_df)

# TODO: Task 5 (a) preprocess the metadata dataframe

raw_sdf_meta = spark.read.json(
    CONFIG.get("file_path")["raw_data_hnk_meta"]
)

meta_df = (
    raw_sdf_meta
    # Explode will remove any null values and create multiple row out of the array
    .select("asin", "brand", fn.explode("category"))
    .groupby("asin", "brand")
    # concat will combine multiple row into string
    .agg(fn.concat_ws(", ", fn.collect_list(col("col"))).alias("category"))
)


# TODO: Task 5 (b) combine both dataframe into one
#  so that category of each asin appears in single dataframe
combined_df = main_df.join(meta_df.drop_duplicates(), on="asin")


# TODO: Task 5 (c) Group the data to show mostly purchased/reviewed categories
def show_popular_product(df: SparkDf) -> None:
    """

    :param df:
    :return:
    """
    product_by_category = (
        df
        .groupby("category")
        .agg(fn.countDistinct("asin").alias("total_product"))
        .orderBy("total_product", ascending=False)
        .limit(20)
        .rdd.collect()
    )
    for item in product_by_category:
        print("=======================================")
        print("Category:\t {}".format(item["category"]))
        print("Unique Product Sold:\t {}".format(item["total_product"]))
        print("=======================================")


show_popular_product(combined_df)

# TODO: Task 6

# Method 1: To check if there is enough data or not
raw_sdf_meta.filter(fn.array_contains(col("category"), "Sports & Outdoors category")).show()

# Method 2: To check if there is enough data or not
raw_sdf_meta.select("asin", fn.explode("category")).filter(col("col") == "Sports & Outdoors").show()


# Method 3: To check if there is enough data or not
def show_all_category(df: SparkDf) -> None:
    all_cat = df.select("asin", fn.explode("category")).select("col").distinct().collect()
    all_cat = [item["col"] for item in all_cat]
    all_cat.sort()
    for item in all_cat:
        print(item)


show_all_category(raw_sdf_meta)
# 🤡 Prank: There is no data about Sports & Outdoors! 🤡

# This is a data for Home & Kitchen so there will be no data about Sports & Outdoors category
# as you are going to have this kind of situation at work too. That you ask something, and you
# get something else back from data engineering / operations team so always validate your data that what you
# have asked for, and what you are receiving. This is the import first step before even you start a project

# THANK YOU !
# LinkedIn: www.linkedin.com/in/mr-data-psycho
# Twitter: @MrDataPsycho






