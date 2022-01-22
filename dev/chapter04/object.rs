struct Review {
    asin: String,
    review_year: i64,
    review_month: i64,
    vote: i64,
    review_text: String
}

fn show_review(review: Review) -> String{
    review.asin
}