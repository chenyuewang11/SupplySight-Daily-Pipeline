from datetime import datetime, date
from bs4 import BeautifulSoup 
from dotenv import load_dotenv
import requests

load_dotenv()

seafood_source_url = "https://www.seafoodsource.com/pricing/archive"
seafood_news_url = "https://seafoodnews.com"
browser_header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}


def get_news_seafood_source() -> list[dict]: 
    response = requests.get(seafood_source_url, headers=browser_header)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        articles = soup.find_all('h2')

        all_news = []

        for article in articles:
            link_tag = article.find('a')

            if link_tag:
                news = dict()

                title = link_tag.get_text(strip=True)
                sub_url = link_tag.get('href')
                date = get_news_date_seafood_source(sub_url)

                news["title"] = title
                news["content"] = title # content paywalled, using title for summary
                news["url"] = f"https://www.seafoodsource.com{sub_url}"
                news["publication_date"] = date

                all_news.append(news)
                print(f"Retrieved article with title: {news["title"]}({len(all_news)})")

        return all_news
    
    
    print(f"Cannot fetch from seafood source")
    print(f"Status code: {response.status_code}")
    print(response.text)
    return []
             
             
def get_news_date_seafood_source(sub_url: str) -> datetime.date:
    full_url = f"https://www.seafoodsource.com{sub_url}"
    response = requests.get(full_url, headers=browser_header)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        date_div = soup.select_one('div.article__date')
        date_str = date_div.get_text(strip=True)
        date = datetime.strptime(date_str, "%B %d, %Y").date()
        return date
    
    print(f"Error getting date at: {seafood_source_url}{sub_url}")
    print(f"Status code: {response.status_code}")
    print(response.text)
    return None


def get_news_seafood_news(start_date: date, end_date: date):
    response = requests.get(seafood_news_url, headers=browser_header)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        story_blocks = soup.find_all("a", class_="headline")
        news = []

        for story in story_blocks:
            sub_url = story.get("href")

            content_type = sub_url.split("/")[1]
            if content_type == "Story":
                try:
                    detail = get_news_detail_seafood_news(sub_url, start_date, end_date)
                
                except DateException:
                    continue
                
                if not detail:
                    break

                detail["url"] = f"https://seafoodnews.com{sub_url}"
                if detail["content"] is not None and "summary" not in detail["title"].lower():
                    news.append(detail)
                    print(f"Retrieved article with title: {detail["title"]}({len(news)})")

        return news
    
    print(f"Cannot fetch from seafood news")
    print(f"Status code: {response.status_code}")
    print(response.text)
    return []


class DateException(Exception):
    pass

def get_news_detail_seafood_news(sub_url: str, start_date: date, end_date: date)-> dict:
    response = requests.get(seafood_news_url + sub_url, headers=browser_header)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")

        date_tag = soup.find('span', class_ = 'StoryNote')
        date_text = date_tag.get_text(strip = True) if date_tag else None 
        if date_text:
            publication_date = datetime.strptime(date_text, "%B %d, %Y").date()
        else:
            print(f"No publication date found at: https://seafoodnews.com{sub_url}")
            publication_date = None

        if publication_date > end_date:
            raise DateException()
        
        if publication_date < start_date:
            return None

        title_tag = soup.find('span', class_ = 'StoryTitle')
        title = title_tag.get_text(strip = True) if title_tag else None
        if not title:
            print(f"No title found at: https://seafoodnews.com{sub_url}")

        content = []
        if title_tag and title_tag.parent:

            for sibling in title_tag.parent.find_next_siblings():
                if sibling.name == 'p':
                    text = sibling.get_text(separator = " ", strip = True)

                    if text:
                        content.append(text)
        if not content:
            print(f"No content found at: https://seafoodnews.com{sub_url}")
        content = " ".join(content)

        return {
            "publication_date": publication_date,
            "title": title,
            "content": content
        }
    
    print(f"Error getting article details from Seafood News")
    print(f"Status code: {response.status_code}")
    print(response.text)
    return {}
