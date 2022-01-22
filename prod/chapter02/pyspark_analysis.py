# prod/chapter02/pyspark_analysis.py
from pyspark.sql import DataFrame as SparkDf
from pathconfig import PATH_TOY_GAME_DATA, create_path_snapshot
from utils.session import create_spark_session
from utils.tableschema import COL_NAME_MAP, SELECTED_COLUMNS


def rename_columns(df: SparkDf, column_map: dict) -> SparkDf:
    """

    :param df: Spark Dataframe to Rename Columns
    :param column_map: Old and New column map
    :return: Spark Dataframe after rename
    """
    for old, new in column_map.items():
        df = df.withColumnRenamed(old, new)
    return df


def execute_pipeline():
    path_snapshot = create_path_snapshot()
    spark = create_spark_session()
    raw_sdf = spark.read.json(PATH_TOY_GAME_DATA)
    raw_sdf = rename_columns(raw_sdf, COL_NAME_MAP)
    raw_sdf = raw_sdf.select(*SELECTED_COLUMNS)
    raw_sdf = raw_sdf.repartition("asin").sortWithinPartitions("unix_review_time")
    raw_sdf.write.partitionBy("asin").mode("overwrite").parquet(str(path_snapshot.resolve()))


execute_pipeline()
