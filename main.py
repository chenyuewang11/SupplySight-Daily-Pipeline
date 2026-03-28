from datetime import datetime, timezone, timedelta
from load_daily_data import load_daily_data
from load_news import load_news_seafood_source, load_news_seafood_news
from evaluate_news import evaluate_news, write_sentiment_score


if __name__ == "__main__":
    date_today = datetime.now(timezone.utc).date()
    date_yesterday = date_today - timedelta(days = 1)
    print(f"Started pipeline. Today is {date_today}")
    
    load_daily_data(date_yesterday, date_yesterday)
    print(f"Loaded quantitative data for {date_yesterday}")

    load_news_seafood_source(date_yesterday, date_yesterday)
    print(f"Loaded news from seafood source for {date_yesterday}")

    load_news_seafood_news(date_yesterday, date_yesterday)
    print(f"Loaded news from seafood news for {date_yesterday}")

    evaluate_news(["shrimp"])
    print(f"Evaluated all articles for {date_yesterday}")

    write_sentiment_score()
    print(f"Completed sentiment score calculation for {date_yesterday}")
