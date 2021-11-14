from pyspark.sql import DataFrame as SparkDf
from pyspark.sql import functions as fn
from pyspark.sql.functions import col
from pathconfig import create_path_snapshot_spark, load_latest_parquet_path
from utils.session import create_spark_session
from pyspark.sql import SparkSession
from pprint import pprint


spark = create_spark_session()


def read_latest_snapshot(ctx: SparkSession) -> SparkDf:
    """
    Read parquet data source from latest metadata
    :param ctx: A Spark session context
    :return: Spark Dataframe
    """
    path = load_latest_parquet_path()
    df = ctx.read.parquet(str(path))
    return df


main_df = read_latest_snapshot(spark)
main_df.show(10)


# TODO: Convert Unix Review Time to Python Datetime
main_df = main_df.withColumn('reviewed_at', fn.from_unixtime(col('unix_review_time')))

# TODO: Impute NaN vote with Zero
main_df = main_df.fillna({'vote': 0})


# TODO: Average Review per Product
def average_review(df: SparkDf) -> float:
    unique_asin = df.select('asin').distinct().count()
    total_review = df.count()
    avg_review = unique_asin / total_review
    print(f"Average Review per product {avg_review:.2f}")
    return avg_review


average_review(main_df)

# TODO: Total Number of Review by Product
review_by_product = main_df.groupby('asin').count()

# TODO: Distribution of review_text length and Other Statistics
# object of type 'float' has no len() in direct apply
main_df = main_df.fillna({'review_text': ''})
main_df = main_df.withColumn('review_text_len', fn.length(col('review_text')))


def show_review_text_stat(df: SparkDf) -> None:
    """
    Show general Stats for review text lenght
    :param df: Dataframe
    :return: Nothing
    """
    summary_df = df.select('review_text_len').summary("count", "min", "25%", "75%", "max")
    summary = summary_df.rdd.map(lambda row: row.asDict(True)).collect()
    print("Review Length Stat")
    pprint(summary)
    weired_reviews = df.filter(col('review_text_len') <= 1).count()
    print(f"Reviews with length one or less: {weired_reviews}")


show_review_text_stat(main_df)


# TODO: Median Number of Reviews per Year
main_df = main_df.withColumn('review_year', fn.year(col('reviewed_at')))
main_df = main_df.withColumn('review_month', fn.month(col('reviewed_at')))

median_review_by_year_df = (
    main_df
    .groupby('asin', 'review_year')
    .agg(fn.count('asin').alias('count'))
    .groupby('review_year')
    .agg(fn.percentile_approx('count', 0.5).alias('median'))
    .orderBy('review_year', ascending=True)
)

# TODO: Find median yearly review of the top products
review_top_item_by_year = (
    main_df.filter(col('overall') > 4)
    .groupby('asin', 'review_year')
    .agg(fn.count('asin').alias('count'))
    .groupby('review_year')
    .agg(fn.percentile_approx('count', 0.5).alias('median'))
    .orderBy('review_year', ascending=True)
)

# TODO: Top Reviews of 2017
top_reviews_2017 = (
    main_df
    .filter(
        (col('review_year') == 2017) &
        (col('vote') >= 20)
    )
)


# TODO: Drop The columns we Do not Need
column_list = ['unix_review_time', 'review_time', 'summary']
main_df = main_df.drop(*column_list)


# TODO: Store The Snapshot

PATH_SNAPSHOT = create_path_snapshot_spark()
main_df = main_df.repartition('review_year', 'review_month').sortWithinPartitions("asin")
main_df.write.partitionBy('review_year', 'review_month').mode("overwrite").parquet(PATH_SNAPSHOT)