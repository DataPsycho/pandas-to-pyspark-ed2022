import os
from utils.session import create_spark_session
from dotenv import load_dotenv

assert load_dotenv()
print(os.environ["SPARK_HOME"])
spark = create_spark_session()

columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
rdd = spark.sparkContext.parallelize(data)
dfFromRDD1 = rdd.toDF()
dfFromRDD1.printSchema()
