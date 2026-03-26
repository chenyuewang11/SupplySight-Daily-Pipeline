from datetime import datetime, timezone, timedelta
from load_daily_data import load_daily_data
from load_news import load_news_seafood_source, load_news_seafood_news
from evaluate_news import evaluate_news, write_sentiment_score


if __name__ == "__main__":
    date_today = datetime.now(timezone.utc).date()
    load_daily_data(date_today - timedelta(days = 2), date_today - timedelta(days = 1))
    load_news_seafood_source(start_date = date_today - timedelta(days = 1))
    load_news_seafood_news(start_date = date_today - timedelta(days = 1))
    evaluate_news(["shrimp"])
    write_sentiment_score()
