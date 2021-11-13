# Dev Script Pandas Analysis
import pandas as pd
from tqdm import tqdm
import json
import time

PATH_BIGDATA = 'data/raw/Toys_and_Games_5.json'


pd_config = [
    ('display.max_rows', 500),
    ('display.max_columns', 500),
    ('display.width', 1000)
]

# raw_pdf = pd.read_json(PATH_BIGDATA)


def set_pd_display(setting):
    for item in setting:
        pd.set_option(*item)


set_pd_display(pd_config)


def read_json_to_pdf(path: str) -> pd.DataFrame:
    data = []
    with open(path, 'r') as f:
        for line in tqdm(f):
            data.append(json.loads(line))
    df = pd.DataFrame(data)
    return df


raw_pdf = read_json_to_pdf(PATH_BIGDATA)

COL_NAME_MAP = {
    "overall": "overall",
    "verified": "verified",
    "reviewTime": "review_time",
    "reviewerID": "reviewer_id",
    "asin": "asin",
    "reviewerName": "reviewer_name",
    "reviewText": "review_text",
    "summary": "summary",
    "unixReviewTime": "unix_review_time",
    "style": "style",
    "vote": "vote",
    "image": "image"
}

raw_pdf = raw_pdf.rename(columns=COL_NAME_MAP)


SELECTED_COLUMNS = [
    "reviewer_id",
    "asin",
    "review_text",
    "summary",
    "verified",
    "overall",
    "vote",
    "unix_review_time",
    "review_time",
]


raw_pdf = raw_pdf[SELECTED_COLUMNS]


def create_path_snapshot():
    path_fixed = 'data/snapshot/pandas/data_{}.json'
    current_unix_time = int(time.time())
    return path_fixed.format(current_unix_time)


PATH_SNAPSHOT = create_path_snapshot()
raw_pdf.to_json(PATH_SNAPSHOT)


