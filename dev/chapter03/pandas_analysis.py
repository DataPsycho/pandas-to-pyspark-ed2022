import pandas as pd
from pathconfig import PATH_TOY_GAME_DATA, SNAPSHOT
from config import set_pd_display
from utils.tableschema import COL_NAME_MAP, SELECTED_COLUMNS
from utils.pandas_utils import read_json_to_pdf, select_columns
from datetime import datetime
from pprint import pprint

# Pandas Display Setup
assert set_pd_display(pd)

# Read the Data
raw_pdf = read_json_to_pdf(PATH_TOY_GAME_DATA, COL_NAME_MAP)
raw_pdf = select_columns(raw_pdf, SELECTED_COLUMNS)

raw_pdf.info()
print(len(raw_pdf))

# TODO: Convert Unix Review Time to Python Datetime
# Be aware of series object has no attribute applymap
raw_pdf['reviewed_at'] = raw_pdf[['unix_review_time']].applymap(datetime.fromtimestamp)

# TODO: Impute NaN vote with Zero
raw_pdf['vote'] = raw_pdf['vote'].fillna(value=0)


# TODO: Average Review per Product

def average_review(df):
    unique_asin = len(df['asin'].unique())
    total_review = len(df)
    avg_review = unique_asin/total_review
    print(f"Average Review per product {avg_review:.2f}")
    return avg_review


mean_over_all_review = average_review(raw_pdf)

# TODO: Total Number of Review by Product
review_by_product = raw_pdf.groupby('asin')['asin'].count()
# TODO: Distribution of review_text length and Other Statistics
# object of type 'float' has no len() in direct apply
raw_pdf['review_text_len'] = raw_pdf[['review_text']].astype(str).applymap(len)


def show_review_text_stat(df):
    stat = df['review_text_len'].describe().to_dict()
    weired_reviews = len(df[df['review_text_len'] <= 1])
    print("Review Length Stat")
    pprint(stat)
    print(f"Reviews with length one or less: {weired_reviews}")


show_review_text_stat(raw_pdf)

# TODO: Median Number of Reviews per Year
raw_pdf['review_year'] = raw_pdf['reviewed_at'].dt.year
raw_pdf['review_month'] = raw_pdf['reviewed_at'].dt.month

median_review_by_year_df = (
    raw_pdf
    .groupby(['asin', 'review_year'])
    .agg(median_review=("asin", "count"))
    .reset_index()
    .groupby('review_year')
    .agg({'median_review': 'median'})
    .reset_index()
)
# TODO: Find median yearly review of the top products
review_top_item_by_year = (
    raw_pdf[raw_pdf['overall'] > 4]
    .groupby(['asin', 'review_year'])
    .agg(median_review=("asin", "count"))
    .reset_index()
    .groupby('review_year')
    .agg({'median_review': 'median'})
    .reset_index()
)

# TODO: Top Reviews of 2017
print(raw_pdf['vote'].describe())
raw_pdf['vote'] = raw_pdf['vote'].apply(lambda x: int(str(x).replace(',', '')))
raw_pdf['vote'] = raw_pdf['vote'].astype(int)
top_reviews_2017 = raw_pdf[(raw_pdf['review_year'] == 2017) & (raw_pdf['vote'] >= 20)]


# TODO: Drop The columns we Do not Need
column_list = ['unix_review_time', 'review_time', 'summary']
raw_pdf = raw_pdf.drop(columns=column_list, axis=1)

# TODO: Store The Snapshot
snapshot_path = SNAPSHOT.joinpath('pandas', 'snapshot_chapter03.json')
raw_pdf.to_json(snapshot_path, orient='records')
