import pandas as pd
from tqdm import tqdm
import json
import typing as t


def read_json_to_pdf(path: str, schema: t.Dict) -> pd.DataFrame:
    data = []
    with open(path, 'r') as f:
        for line in tqdm(f):
            data.append(json.loads(line))
    df = pd.DataFrame(data)
    df.rename(columns=schema, inplace=True)
    return df


def select_columns(df: pd.DataFrame, col_list: list) -> pd.DataFrame:
    return df[col_list]