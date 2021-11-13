import pandas as pd
from dateutil.relativedelta import relativedelta
from pathconfig import PATH_TOY_GAME_DATA
from config import set_pd_display
from utils.tableschema import COL_NAME_MAP, SELECTED_COLUMNS
from utils.pandas_utils import read_json_to_pdf, select_columns

assert set_pd_display(pd)


raw_pdf = read_json_to_pdf(PATH_TOY_GAME_DATA, COL_NAME_MAP)
raw_pdf = select_columns(raw_pdf, SELECTED_COLUMNS)


# === Cleaning
# Test Functions
def total_null_values(df: pd.DataFrame, col_name: str):
    return len(df[df[col_name].isnull()])


total_null_values(raw_pdf, "review_text")

# TODO: Remove All Review will Null
raw_pdf = raw_pdf[~raw_pdf["review_text"].isnull()]
# TODO: Replace Null Summary Value with Unknown
raw_pdf = raw_pdf[~raw_pdf["summary"].isnull()]
# TODO: Convert Unix time to General DateTime
raw_pdf["review_time"] = pd.to_datetime(raw_pdf["unix_review_time"], unit='s')
# TODO: Remove Unix Date column
raw_pdf = raw_pdf.drop(['unix_review_time'], axis=1)
# TODO: create Year Month from Date
raw_pdf["review_year"] = raw_pdf["review_time"].dt.year
raw_pdf["review_month"] = raw_pdf["review_time"].dt.month
# TODO: Keep always last 5 year of data


def select_last_five_years(df: pd.DataFrame):
    max_date = df['review_time'].max()
    begin_date = max_date - relativedelta(years=3)
    return raw_pdf[raw_pdf["review_time"] > begin_date]


raw_pdf = select_last_five_years(raw_pdf)

# TODO: Order the data by review_time by default ascending is True change to ascending=False if needed
raw_pdf = raw_pdf.sort_values(["review_time"], ascending=True)

# === Report Creation
# TODO: Create a table of Quarterly Total Reviewers by Year and Quarter
raw_pdf['review_quarter'] = 'Q' + raw_pdf['review_time'].dt.quarter.apply(str)

pdf_quarterly_reviewer = (
    raw_pdf
    .groupby(['review_year', 'review_quarter'])
    .agg()
)



