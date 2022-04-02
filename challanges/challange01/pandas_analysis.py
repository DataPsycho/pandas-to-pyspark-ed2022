import typing as t
import numpy as np
import pandas as pd

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


def create_sample_df(sample_size=20) -> pd.DataFrame:
    """
    Create a Dataframe of 20 observation
    :param sample_size: sample size
    :return: A dataframe
    """
    data_repo = data_generator(sample_size)
    df = pd.DataFrame.from_records(data_repo["data"], columns=data_repo["schema"])
    return df


MAIN_DF = create_sample_df()


def impute_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Impute the missing product name by the second latter of the depository id
    :param df: Dataframe of depo info
    :return: imputed data frame
    """
    temp_df = df.copy()
    temp_df["temp_product"] = temp_df["depository_id"].apply(lambda x: list(x)[1])
    temp_df["product"] = temp_df["product"].fillna(temp_df["temp_product"])
    temp_df = temp_df.drop("temp_product", axis=1)
    return temp_df


def total_units_available(df: pd.DataFrame) -> pd.DataFrame:
    _df = (
        df.groupby(['company', 'product', 'depository_id'])
        .agg({"unit": "sum"})
        .reset_index()
    )
    return _df


imputed_df = impute_missing(MAIN_DF)
summary_df = total_units_available(imputed_df)
summary_df.to_csv("./data/challanges/challange01/summary.csv")