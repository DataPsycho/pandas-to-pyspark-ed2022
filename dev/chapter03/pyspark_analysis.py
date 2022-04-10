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

main_df = main_df.withColumn("reviewed_year", fn.year(col("reviewed_at")))
main_df = main_df.withColumn("reviewed_month", fn.month(col("reviewed_at")))


# TODO: Impute NaN vote with Zero
main_df = main_df.fillna({'vote': 0})


# TODO: Average Review per Product
def average_review(df: SparkDf) -> float:
    """
    Calculate average review per product
    :param df: Pandas dataframe
    :return: Average
    """
    unique_asin = df.select('asin').distinct().count()
    total_review = df.count()
    avg_review = total_review / unique_asin
    print(f"Average Review per product {avg_review:.2f}")
    return avg_review


mean_over_all_review = average_review(main_df)

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
    summary = summary_df.rdd.map(lambda row: row.asDict(recursive=True)).collect()
    print("Review Length Stat")
    pprint(summary)
    weired_reviews = df.filter(col('review_text_len') <= 1).count()
    print(f"Reviews with length one or less: {weired_reviews}")


show_review_text_stat(main_df)


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

# TODO: Compare Total Monthly Review of 2017 and 2018
total_review_by_mth_df = (
    main_df
    .groupBy('review_year', 'review_month')
    .agg(fn.count(col("asin")).alias("total_review"))
    .orderBy('review_year', 'review_month')
)

total_review_2016 = total_review_by_mth_df.filter(col("review_year") == 2016)
total_review_2017 = total_review_by_mth_df.filter(col("review_year") == 2017)

merged_20_16_17 = (
    total_review_2016
    .select(
        "review_month",
        col("total_review").alias("total_review_2016")
    )
    .join(
        total_review_2017.select("review_month", col("total_review").alias("total_review_2017")),
        on="review_month"
    )
)


# TODO: Functional representation of same problem
def aggregate_by_year(df: SparkDf) -> SparkDf:
    """
    Aggregate the data frame by year and month
    :param df: A DataFrame
    :return: A transformed DataFrame
    """
    _df = (
        df
        .groupBy('review_year', 'review_month')
        .agg(fn.count(col("asin")).alias("total_review"))
        .orderBy('review_year', 'review_month')
    )
    return _df


def compare_monthly_total_review_by_year(df: SparkDf, year_x: int, year_y: int) -> SparkDf:
    """
    Compare two years of total review
    :param df: A DataFrame
    :param year_x: base Year to Compare
    :param year_y: Ahead year to Compare from Base year
    :return: A transformed DataFrame
    """
    year_x_df = df.filter(col("review_year") == year_x)
    year_y_df = df.filter(col("review_year") == year_y)

    merged_df = (
        year_x_df
        .select(
            "review_month",
            col("total_review").alias(f"total_review_{year_x}")
        ).join(
            year_y_df.select("review_month", col("total_review").alias(f"total_review_{year_y}")),
            on="review_month"
        )
    )
    return merged_df


def execute_pipeline(df: SparkDf, year_x: int, year_y: int) -> SparkDf:
    """
    An execution pipeline example
    :param df: A DataFrame
    :param year_x: base Year to Compare
    :param year_y: Ahead year to Compare from Base year
    :return: A transformed DataFrame
    """
    _df = aggregate_by_year(df)
    _df = compare_monthly_total_review_by_year(df=_df, year_x=year_x, year_y=year_y)
    return _df


temp_df = execute_pipeline(main_df, 2016, 2017)


merged_20_16_17_united = total_review_2016.union(total_review_2017)

merged_20_16_17_pivoted = (
    merged_20_16_17_united
    .groupBy("review_month")
    .pivot("review_year")
    .sum("total_review")
    .orderBy("review_month")
)


# TODO: Drop The columns we Do not Need
column_list = ['unix_review_time', 'review_time', 'summary']
main_df = main_df.drop(*column_list)


# TODO: Store The Snapshot
PATH_SNAPSHOT = create_path_snapshot_spark()
main_df = main_df.repartition('review_year', 'review_month').sortWithinPartitions("asin")
main_df.write.partitionBy('review_year', 'review_month').mode("overwrite").parquet(PATH_SNAPSHOT)
