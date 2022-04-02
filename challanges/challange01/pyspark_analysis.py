import typing as t
import numpy as np
import pyspark.sql as ps

np.random.seed(13)


def select_random(scope: list) -> t.Optional[str]:
    return np.random.choice(scope)


def data_generator(sample_size=25) -> t.Dict:
    """
    Generate random data
    :return: Data with schema definition of pharma product sales
    """
    company = ["MSD", "NOVARTIS", "PFIZER", "ROCHE"]
    product = ["A", "B", "C", None]
    depo_id = ["AA", "BB", "CC", "DD"]
    units = np.random.normal(2000, 100, sample_size)
    data_store = []

    for i in range(0, sample_size):
        cmp = select_random(company)
        prd = select_random(product)
        dep = select_random(depo_id)
        unt = int(units[i])
        data_store.append((cmp, prd, dep, unt))

    schema = ["company", "product", "depository_id", "unit"]
    return {"data": data_store, "schema": schema}


def create_sample_df(sample_size=20) -> ps.DataFrame:
    """
    Create a Dataframe of 20 observation
    :param sample_size: sample size
    :return: A dataframe
    """
    # TODO: write the data frame generation code for PySpark
    df = ps.DataFrame()
    return df


MAIN_DF = create_sample_df()


def impute_missing(df: ps.DataFrame) -> ps.DataFrame:
    """
    Impute the missing product name by the second latter of the depository id
    :param df: Dataframe of depo info
    :return: imputed data frame
    """
    # TODO: add imputation code which will remove nulls values from product column
    _df = ps.DataFrame()
    return _df


def total_units_available(df: ps.DataFrame) -> ps.DataFrame:
    """
    Aggregate the data to sum the unit available per company, product and dipo
    :param df: Data Frame un aggregated
    :return: Aggregated DataFrame
    """
    # TODO: Write the data code to aggregate the dataframe to show aggregated result
    _df = ps.DataFrame()
    return _df


imputed_df = impute_missing(MAIN_DF)
summary_df = total_units_available(imputed_df)

# TODO: Save the data as parquet format into one file: ./data/challanges/challange01/summary.parquet
