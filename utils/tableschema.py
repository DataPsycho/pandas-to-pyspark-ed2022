# utils/tableschema.py
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
