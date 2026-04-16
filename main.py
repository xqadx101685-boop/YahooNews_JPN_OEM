import os
import json
import time
import re
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple, Dict
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from google import genai
from google.genai import types
import comment_scraper

# ====== 設定 ======
SPREADSHEET_KEY = os.environ.get("SPREADSHEET_KEY")
KEYWORD_FILE = "keywords.txt"
TZ_JST = timezone(timedelta(hours=9))
AVAILABLE_API_KEYS = [os.environ.get(f"GOOGLE_API_KEY_{i}") for i in range(1, 6) if os.environ.get(f"GOOGLE_API_KEY_{i}")]
CURRENT_KEY_INDEX = 0

# ====== 基本ヘルパー ======
def jst_now(): return datetime.now(TZ_JST)

def build_gspread_client():
    info = json.loads(os.environ.get("GCP_SERVICE_ACCOUNT_KEY"))
    scope = ['https://spreadsheets.google.com/feeds', 'https://spreadsheets.google.com/auth/drive']
    return gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(info, scope))

def parse_post_date(raw, today_jst: datetime) -> Optional[datetime]:
    if not raw: return None
    s = re.sub(r"\([月火水木金土日]\)", "", str(raw)).replace('配信', '').strip()
    for fmt in ("%Y/%m/%d %H:%M:%S", "%y/%m/%d %H:%M", "%m/%d %H:%M", "%Y/%m/%d %H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt == "%m/%d %H:%M": dt = dt.replace(year=today_jst.year)
            if dt.replace(tzinfo=TZ_JST) > today_jst + timedelta(days=31): dt = dt.replace(year=dt.year - 1)
            return dt.replace(tzinfo=TZ_JST)
        except ValueError: continue
    return None

def get_yahoo_news_with_selenium(keyword: str) -> list[dict]:
    opts = Options()
    opts.add_argument("--headless=new"); opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.get(f"https://news.yahoo.co.jp/search?p={keyword}&ei=utf-8&categories=domestic,world,business,it,science,life,local")
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    data = []
    for art in soup.find_all("li", class_=re.compile("sc-1u4589e-0")):
        try:
            title = art.find("div", class_=re.compile("sc-3ls169-0")).text.strip()
            link = art.find("a", href=True)["href"]
            date_str = art.find("time").text.strip() if art.find("time") else ""
            data.append({"URL": link, "タイトル": title, "投稿日時": date_str, "ソース": ""})
        except: continue
    return data

def call_gemini(prompt: str) -> Dict:
    global CURRENT_KEY_INDEX
    client = genai.Client(api_key=AVAILABLE_API_KEYS[CURRENT_KEY_INDEX])
    try:
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json"))
        return json.loads(response.text.strip())
    except:
        CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(AVAILABLE_API_KEYS)
        return {"company_info": "N/A", "category": "N/A", "sentiment": "N/A", "nissan_related": "なし", "nissan_negative": "なし"}

def main():
    # --- デバッグ追加箇所 ---
    key_debug = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
    print(f"DEBUG: Key content: {key_debug}")
    # ----------------------
    gc = build_gspread_client()
    ws = gc.open_by_key(SPREADSHEET_KEY).worksheet("Yahoo")
    now = jst_now()
    four_days_ago = now - timedelta(days=4)
    one_day_ago = now - timedelta(hours=24)

    # 1. ニュース取得・4日フィルター
    keys = [l.strip() for l in open(KEYWORD_FILE, 'r', encoding='utf-8') if l.strip()]
    new_articles = []
    for k in keys:
        for item in get_yahoo_news_with_selenium(k):
            dt = parse_post_date(item['投稿日時'], now)
            if dt and dt >= four_days_ago:
                new_articles.append(item)

    # 重複除外して追加
    existing_urls = set(r[0] for r in ws.get_all_values()[1:] if r)
    ws.append_rows([[d['URL'], d['タイトル'], d['投稿日時'], d['ソース']] for d in new_articles if d['URL'] not in existing_urls], value_input_option='USER_ENTERED')

    # 2. 最新(24h以内)のみ分析
    rows = ws.get_all_values()[1:]
    for idx, row in enumerate(rows):
        row_num = idx + 2
        post_dt = parse_post_date(row[2], now)
        
        # 24時間以内の記事かつ分析未完了(または再分析要)であれば実行
        if post_dt and post_dt >= one_day_ago:
            print(f"Row {row_num}: 最新処理実行")
            # ここでコメント収集と分析を実施
            comment_scraper.run_comment_collection(gc, SPREADSHEET_KEY, "Yahoo", lambda text: call_gemini(f"分析: {text}"))
            
    print("最新分の処理完了")

if __name__ == '__main__':
    main()
