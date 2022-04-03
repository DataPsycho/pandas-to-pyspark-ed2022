import pandas as pd

pd.set_option('display.max_rows', 20)
MAIN_DF = pd.read_csv("./data/challanges/challange01/summary.csv")


# TASK1: Adding a new column with combined data
def combine_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine all columns of a DataFrame into one column
    :param df: DataFrame to combine the columns
    :return: DataFrame returned with a new Combined Column
    """
    _df = df.copy()
    combiner = lambda row: {
        "company": row['company'],
        "product": row["product"],
        "depository_id": row["depository_id"],
        "unit": row["unit"]
    }
    # schema = ["company", "product", "depository_id", "unit"]
    _df["combined"] = _df.apply(combiner, axis=1)
    return _df


# Combined information in one column combined
df_col_accumulated = combine_columns(MAIN_DF)

# TASK 2:
LOCATION_DF = pd.DataFrame.from_records(
    [("AA", "Prague"), ("CC", "Dinajpur"), ("DD", "Dhaka")],
    columns=["depository_id", "location"]
)


def combine_df(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """
    Combine two dataframe in to one final dataframe
    :param df1: Warehouse DataFrame
    :param df2: Location DataFrame
    :return: Combined DataFrame
    """
    _df = df1.merge(df2, on="depository_id", how="left")
    return _df


df_combined = combine_df(df_col_accumulated, LOCATION_DF)

df_combined.to_csv("./data/challanges/challange02/combined.csv")


