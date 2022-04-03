import pyspark.sql as ps
from dotenv import load_dotenv


# If you are using DataBricks like environment
# a default session with the name spark will already be available
# So you do not need that function
def create_spark_session():
    """Create a Spark Session"""
    _ = load_dotenv()
    return (
        ps.SparkSession
        .builder
        .appName("ExperiMind")
        .master("local[5]")
        .getOrCreate()
    )


# You do not need that spark session when working in DataBricks like environment
spark = create_spark_session()

# Read the data from previous Challenge
MAIN_DF = spark.read.parquet("./data/challanges/challange01/summary.parquet")


# TASK1: Adding a new column with combined data
def combine_columns(df: ps.DataFrame) -> ps.DataFrame:
    """
    Combine all columns of a DataFrame into one column
    :param df: DataFrame to combine the columns
    :return: DataFrame returned with a new Combined Column
    """
    # Write code to combine the columns into a StructType
    _df = ...
    return _df


# Combined information in one column combined
df_col_accumulated = combine_columns(MAIN_DF)

# TASK 2:
# TODO: Read the list of data into SparkDataframe
LOCATION_DATA = [("AA", "Prague"), ("CC", "Dinajpur"), ("DD", "Dhaka")],
LOC_COLNAME =["depository_id", "location"]
LOCATION_DF = ...


def combine_df(df1: ps.DataFrame, df2: ps.DataFrame) -> ps.DataFrame:
    """
    Combine two dataframe in to one final dataframe
    :param df1: Warehouse DataFrame
    :param df2: Location DataFrame
    :return: Combined DataFrame
    """
    _df = ...  # join the df1 with df2 with necessary join type and key
    return _df


df_combined = combine_df(df_col_accumulated, LOCATION_DF)

# TODO: Write the df_combined as combined.parquet into the following location:
#  "./data/challanges/challange02/summary.parquet"
# df_combined.write...


