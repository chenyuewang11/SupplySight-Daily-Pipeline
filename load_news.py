from get_news import get_news_seafood_source, get_news_seafood_news
from dotenv import load_dotenv
from psycopg2.extras import execute_batch
from datetime import date
import os, uuid, psycopg2

load_dotenv()
db_host = os.getenv("POSTGRES_HOST")
db_username = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_name = os.getenv("POSTGRES_DB")


def load_news_seafood_source(start_date: date, end_date: date):
    '''
    use yesterday's date for daily ingestion
    leave start_date empty to write all
    '''
    news_seafood_source = get_news_seafood_source()
    relevant_news = []

    for n in news_seafood_source:
        if start_date <= n["publication_date"] <= end_date:    # avoid overwrite using date check
            n["id"] = str(uuid.uuid4())
            n["source"] = "seafood_source"
            n["status"] = "new"
            relevant_news.append(n)
    
    print(f"Found {len(relevant_news)} pieces of news from seafoodsource.com between {start_date} and {end_date}.")

    try:
        conn = psycopg2.connect(f"dbname={db_name} user={db_username} password={db_password} host={db_host}")
        with conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO news (id, status, source, title, content, url, publication_date)
                    VALUES (%(id)s, %(status)s, %(source)s, %(title)s, %(content)s, %(url)s, %(publication_date)s)
                """

                execute_batch(cur, query, relevant_news)

    except Exception as e:
        print(f"Error loading news from seafood source into database: {e}")

    finally:
        if "conn" in locals():
            conn.close()


def load_news_seafood_news(start_date: date, end_date: date):
    news_seafood_news = get_news_seafood_news(start_date, end_date)
    relevant_news = []

    for n in news_seafood_news:
        n["id"] = str(uuid.uuid4())
        n["source"] = "seafood_news"
        n["status"] = "new"
        relevant_news.append(n)
    
    print(f"Found {len(relevant_news)} pieces of news from seafoodnews.com between {start_date} and {end_date}.")

    try:
        conn = psycopg2.connect(f"dbname={db_name} user={db_username} password={db_password} host={db_host}")
        with conn:
            with conn.cursor() as cur:
                query = """
                    INSERT INTO news (id, status, source, title, content, url, publication_date)
                    VALUES (%(id)s, %(status)s, %(source)s, %(title)s, %(content)s, %(url)s, %(publication_date)s)
                """

                execute_batch(cur, query, relevant_news)

    except Exception as e:
        print(f"Error loading news from seafood source into database: {e}")

    finally:
        if "conn" in locals():
            conn.close()

