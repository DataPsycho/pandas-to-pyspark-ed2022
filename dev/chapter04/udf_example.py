from utils.session import create_spark_session
from pyspark.sql.types import FloatType, IntegerType, ArrayType,  StringType,  StructType,  StructField,  BooleanType
import pyspark.sql.functions as fn
from pyspark.sql.functions import col
import re

spark = create_spark_session()


# Example 1: Calculate Sample Mean
def get_sample_mean(data: list):
    return sum(data)/(len(data) - 1)


udf_get_sample_mean = fn.udf(
    f=get_sample_mean,
    returnType=FloatType()
)


# Example 2
def split_text(data: str):
    names = data.replace("\n",  "").split()
    if len(names) > 2:
        return [names[0],  names[-1]]
    return names


udf_split_text = fn.udf(
    f=split_text,
    returnType=ArrayType(StringType(),  True)
)


data = [
    ("James", "", "Smith", "36636", "M", "Down Hill, 123456, CA"),
    ("Michael", "Rose", "", "40288", "M", "Up Hill, 123457, CA"),
    ("Robert", "", "Williams", "42114", "M", "Central Hill, 123458, CA"),
    ("Maria", "Anne", "Jones", "39192", "F", "Down Town, 123459, CA"),
    ("Jen", "Mary", "Brown", "", "F", "West Wild, 123452, CA")
]

df = spark.createDataFrame(
    data=data,
    schema=["firstname", "middlename", "lastname", "id", "gender", "address"]
)

df.show()

# Alternate way of defining Specific Schema
schema = StructType([
    StructField("firstname", StringType(), True),
    StructField("middlename", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
])

df = spark.createDataFrame(
    data=data,
    schema=schema
)


# Example 3
def make_summary(text: str) -> dict:
    """
    Summarize an address
    :param text: An address
    :return: Dictionary of Information
    """
    has_number = False
    zipcode = ""
    len_text = len(text)
    numbers = re.search(r'\d+', text)
    if numbers:
        has_number = True
        zipcode = numbers.group()
    return {"len": len_text,  "num_present": has_number, "zipcode": zipcode}


udf_make_summery = fn.udf(
    f=make_summary, 
    returnType=StructType([
        StructField('len',  IntegerType(),  False),
        StructField('num_present',  BooleanType()),
        StructField('zipcode', StringType())
    ])
)

df_with_summary = df.withColumn("summary", udf_make_summery("address"))
df_with_summary.select(col("summary.len").alias("address_length")).show()
df_with_summary.select("address", "summary").show()
df_with_summary.select("id", "summary.zipcode", "summary.num_present").show()
