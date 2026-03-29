from anthropic import Anthropic
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor, execute_batch
from datetime import datetime
import os, json, psycopg2, uuid

load_dotenv()
claude_api_key = os.getenv("CLAUDE_API_KEY")
db_host = os.getenv("POSTGRES_HOST")
db_username = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_name = os.getenv("POSTGRES_DB")

prompt_location = os.path.join("evaluate_news_prompts.json")
with open(prompt_location, 'r', encoding='utf-8') as f:
    prompts = json.load(f)


def get_news_evaluation(client: Anthropic, id: uuid, article_title: str, article_content: str, product_list: str) -> dict:
    '''
    mutliple items in product_list can cause relevant items to be ignored
    use single item in product_list for best result
    '''
    response = client.messages.create(
        model = "claude-haiku-4-5", # cheapest model
        max_tokens = 500,
        system = f"""
            You are an expert data extraction pipeline. 
            Analyze the provided article against this TARGET PRODUCT LIST: {product_list}.

            INSTRUCTIONS:
            1. If the article relates to ONE OR MORE target products, you MUST use the `evaluate_article_impact_on_products` tool. Record ONLY the products that are affected. (It is expected that some products in the list might not be mentioned—ignore them).
            2. If the article is completely unrelated to ANY of the target products (zero matches), do not use the tool. Reply exactly with the word "None". Do not include any other text or explanations.

            CONTEXT:
            {"\n".join([prompts[product] for product in product_list if product in prompts])}
        """,
        tools = [prompts["evaluate_article_impact_on_products"]],
        messages=[
            {
                "role": "user", 
                "content": f"""
                    <article_title>{article_title}</article_title>
                    <article_content>{article_content}</article_content>
                    """
            }
        ]
    )

    if response.stop_reason == "tool_use":
        tool_use_block = next(block for block in response.content if block.type == "tool_use")
        extracted_data = tool_use_block.input

        result = []
        for data in extracted_data["evaluations"]:
            article_evaluation = dict(data)
            article_evaluation["id"] = id
            article_evaluation["processed_time"] = datetime.now()
            result.append(article_evaluation)

        return result

    return None


def get_unevaluated_news():
    result = []
    try:
        conn = psycopg2.connect(f"dbname={db_name} user={db_username} password={db_password} host={db_host}", cursor_factory=RealDictCursor)

        with conn:
            with conn.cursor() as cur:
                query = """
                    UPDATE news
                    SET status = 'pending'
                    WHERE id in (
                        SELECT id from news
                        WHERE status = 'new'
                        FOR UPDATE SKIP LOCKED
                    )
                    RETURNING id, title, content
                """
                cur.execute(query)
                result = cur.fetchall()

    except Exception as e:
        print(f"Error getting unevaluated news: {e}")
    
    finally:
        if "conn" in locals():
            conn.close()

    return result


def get_evaluated_news(unevaluated_news: list[dict], product_list: list[str]):
    client = Anthropic(api_key = claude_api_key)
    evaulated_news = []

    if not unevaluated_news:
        print("No news articles to evaluate")
        return

    try:
        relevant_count = 0
        for i, news in enumerate(unevaluated_news):
            results = get_news_evaluation(client, news["id"], news["title"], news["content"], product_list)

            print(f"Evaluated {i+1}/{len(news)} articles")

            if results == None:
                continue

            if isinstance(results, Exception):
                print(f"An individual article evaluation failed: {results}")
                continue
            
            relevant_count += 1

            for result in results:
                if isinstance(result, dict):
                    evaulated_news.append(result)

        print(f"{relevant_count}/{len(news)} articles were found to be relevant")
        return evaulated_news

    except Exception as e:
        print(f"Error getting evaluated news: {e}")
    

def load_evaluated_news(evaulated_news: list[dict]):
    if not evaluate_news:
        print("No evaluated news articles to load")
        return
    
    try:
        conn = psycopg2.connect(f"dbname={db_name} user={db_username} password={db_password} host={db_host}", cursor_factory=RealDictCursor)

        with conn:
            with conn.cursor() as cur:
                write_query = """
                    INSERT INTO evaluated_news (id, product, relevancy_score, sentiment_score, processed_time) 
                    VALUES (%(id)s, %(product)s, %(relevancy_score)s, %(sentiment_score)s, %(processed_time)s)
                """
                execute_batch(cur, write_query, evaulated_news)

                update_query = """
                    UPDATE news
                    SET status = 'processed'
                    WHERE status = 'pending'
                """
                cur.execute(update_query)

    except Exception as e:
        print(f"Error loading evaluated news: {e}")

    finally:
        if "conn" in locals():
            conn.close()


def evaluate_news(product_list: list[str]):
    unevalated_news = get_unevaluated_news()
    evaluated_news = get_evaluated_news(unevalated_news, product_list)
    load_evaluated_news(evaluated_news)


def write_sentiment_score():
    try:
        conn = psycopg2.connect(f"dbname={db_name} user={db_username} password={db_password} host={db_host}", cursor_factory=RealDictCursor)

        with conn:
            with conn.cursor() as cur:
                update_query = """
                    UPDATE dates_shrimp ds
                    SET sentiment_score = daily_avg.avg_sentiment
                    FROM (
                        SELECT 
                            n.publication_date, 
                            AVG(en.sentiment_score) AS avg_sentiment
                        FROM news n
                        JOIN evaluated_news en ON n.id = en.id
                        WHERE en.product = 'shrimp' 
                        AND n.status = 'processed'
                        GROUP BY n.publication_date
                    ) AS daily_avg
                    WHERE ds.date = daily_avg.publication_date;
                """
                cur.execute(update_query)

    except Exception as e:
        print(f"Error writing sentiment score: {e}")

    finally:
        if "conn" in locals():
            conn.close()
