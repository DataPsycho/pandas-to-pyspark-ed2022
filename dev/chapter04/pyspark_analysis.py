from pyspark.sql import DataFrame as SparkDf
from pyspark.sql import functions as fn
from pyspark.sql.functions import col
import pyspark.sql.types as st
from pathconfig import create_path_snapshot_spark, load_latest_parquet_path
from utils.session import create_spark_session
from pyspark.sql import SparkSession


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


def summarise_review(text: str) -> dict:
    text = str(text)
    text_len = len(text)
    if text_len > 1000:
        cat = "Large"
    elif 500 < text_len < 1000:
        cat = "medium"
    else:
        cat = "small"
    unique_words = list(set(text.split()))
    return {"len": text_len, "category": cat, "token": unique_words}


udf_summarise_text = fn.udf(
    f=summarise_review,
    returnType=st.StructType([
        st.StructField("len", st.IntegerType()),
        st.StructField("category", st.StringType()),
        st.StructField("token", st.ArrayType(st.StringType(), True))
    ])
)

main_df = main_df.withColumn("review_summary", udf_summarise_text(col("review_text")))
main_df.select(col("review_summary.category"), col("review_summary.token")).show()


# TODO: Store The Snapshot
PATH_SNAPSHOT = create_path_snapshot_spark()
main_df.write.partitionBy('review_year', 'review_month').mode("overwrite").parquet(PATH_SNAPSHOT)

