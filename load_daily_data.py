from get_daily_data import get_daily_df
from dotenv import load_dotenv
from datetime import date
import psycopg2
import io, os


load_dotenv()

db_host = os.getenv("POSTGRES_HOST")
db_username = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_name = os.getenv("POSTGRES_DB")


def load_daily_data(start_date: date, end_date: date):
    df_daily = get_daily_df(start_date, end_date)
    csv_buffer = io.StringIO()
    df_daily.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)
    
    try:
        conn = psycopg2.connect(f"dbname={db_name} user={db_username} password={db_password} host={db_host}")
        with conn:
            with conn.cursor() as cur:
                columns = ', '.join(list(df_daily.columns))
                sql = f"COPY dates_shrimp ({columns}) FROM STDIN WITH CSV"
                cur.copy_expert(sql, csv_buffer)
                
    except Exception as e:
        print(f"Error loading daily: {e}")

    finally:
        if "conn" in locals():
            conn.close()
