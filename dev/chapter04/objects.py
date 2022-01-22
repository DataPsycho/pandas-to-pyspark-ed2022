from dataclasses import dataclass


@dataclass
class Review:
    asin: str
    review_year: int
    review_month: int
    vote: int
    review_text: str


def get_asin(review):
    return review.asin
