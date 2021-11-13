# utils/session.py
from pyspark.sql import SparkSession
from dotenv import load_dotenv


def create_spark_session():
    """Create a Spark Session"""
    _ = load_dotenv()
    return (
        SparkSession
        .builder
        .appName("ExperiMind")
        .master("local[4]")
        .getOrCreate()
    )
