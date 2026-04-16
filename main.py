import os
import json
import time
import re
import random
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Optional, Set, Dict, Any
import sys
from urllib.parse import urlparse, parse_qs, urlunparse, urlencode

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# --- Gemini API 関連 (v1.0 SDK) ---
from google import genai
from google.genai import types
from google.api_core.exceptions import ResourceExhausted

# --- コメント収集用モジュールのインポート ---
import comment_scraper
# ------------------------------------

print("=== 実行開始しました ===", flush=True)
sys.stdout.flush()

# ====== 設定 ======
SHARED_SPREADSHEET_ID = os.environ.get("SPREADSHEET_KEY")
if not SHARED_SPREADSHEET_ID:
    print("エラー: 環境変数 'SPREADSHEET_KEY' が設定されていません。処理を中断します。")
    sys.exit(1)

KEYWORD_FILE = "keywords.txt"
SOURCE_SPREADSHEET_ID = SHARED_SPREADSHEET_ID
SOURCE_SHEET_NAME = "Yahoo"
MAX_SHEET_ROWS_FOR_REPLACE = 10000
MAX_PAGES = 20 

YAHOO_SHEET_HEADERS = ["URL", "タイトル", "投稿日時", "ソース", "本文", "コメント数", "対象企業", "カテゴリ分類", "ポジネガ分類", "日産関連文", "日産ネガ文"]
REQ_HEADERS = {"User-Agent": "Mozilla/5.0"}
TZ_JST = timezone(timedelta(hours=9))

ALL_PROMPT_FILES = [
    "prompt_gemini_role.txt",
    "prompt_target_company.txt",
    "prompt_category.txt",
    "prompt_posinega.txt",
    "prompt_nissan_mention.txt",
    "prompt_nissan_sentiment.txt",
    "prompt_comment_analysis.txt"
]

AVAILABLE_API_KEYS = []
for i in range(1, 6):
    key = os.environ.get(f"GOOGLE_API_KEY_{i}")
    if key: AVAILABLE_API_KEYS.append(key)
if not AVAILABLE_API_KEYS:
    single_key = os.environ.get("GOOGLE_API_KEY")
    if single_key: AVAILABLE_API_KEYS.append(single_key)

if not AVAILABLE_API_KEYS:
    print("警告: APIキー環境変数が設定されていません。")
    GEMINI_CLIENT = None
else:
    print(f"APIキーを {len(AVAILABLE_API_KEYS)} 個ロードしました。")

CURRENT_KEY_INDEX = 0
REQUEST_COUNT_PER_KEY = 0
MAX_REQUESTS_BEFORE_ROTATE = 20
NORMAL_WAIT_SECONDS = 35 

GEMINI_PROMPT_TEMPLATE = None
COMMENT_PROMPT_TEMPLATE = None

# ====== ヘルパー関数群 ======
def get_current_gemini_client() -> Optional[genai.Client]:
    if not AVAILABLE_API_KEYS: return None
    return genai.Client(api_key=AVAILABLE_API_KEYS[CURRENT_KEY_INDEX], http_options={'timeout': 6000000})

def rotate_api_key(reason="limit_reached"):
    global CURRENT_KEY_INDEX, REQUEST_COUNT_PER_KEY
    if not AVAILABLE_API_KEYS: return
    old_index = CURRENT_KEY_INDEX
    CURRENT_KEY_INDEX = (CURRENT_KEY_INDEX + 1) % len(AVAILABLE_API_KEYS)
    REQUEST_COUNT_PER_KEY = 0
    print(f"    [Key Rotation] 理由:{reason} | Key#{old_index + 1} -> Key#{CURRENT_KEY_INDEX + 1} に切り替えます。")

def increment_request_count():
    global REQUEST_COUNT_PER_KEY
    REQUEST_COUNT_PER_KEY += 1
    if REQUEST_COUNT_PER_KEY >= MAX_REQUESTS_BEFORE_ROTATE: rotate_api_key(reason="count_limit")

def gspread_util_col_to_letter(col_index: int) -> str:
    return re.sub(r'\d+', '', gspread.utils.rowcol_to_a1(1, col_index))

def jst_now() -> datetime: return datetime.now(TZ_JST)

def format_datetime(dt_obj) -> str: return dt_obj.strftime("%Y/%m/%d %H:%M:%S")

def parse_post_date(raw, today_jst: datetime) -> Optional[datetime]:
    if raw is None: return None
    if isinstance(raw, str):
        s = raw.strip()
        s = re.sub(r"\([月火水木金土日]\)", "", s).strip()
        s = s.replace('配信', '').strip()
        for fmt in ("%Y/%m/%d %H:%M:%S", "%y/%m/%d %H:%M", "%m/%d %H:%M", "%Y/%m/%d %H:%M"):
            try:
                dt = datetime.strptime(s, fmt)
                if fmt == "%m/%d %H:%M": dt = dt.replace(year=today_jst.year)
                if dt.replace(tzinfo=TZ_JST) > today_jst + timedelta(days=31): dt = dt.replace(year=dt.year - 1)
                return dt.replace(tzinfo=TZ_JST)
            except ValueError: pass
    return None

def build_gspread_client() -> gspread.Client:
    creds_str = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
    info = json.loads(creds_str)
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    return gspread.authorize(ServiceAccountCredentials.from_json_keyfile_dict(info, scope))

def load_keywords(filename: str) -> List[str]:
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    except: return []

def load_merged_prompt() -> str:
    global GEMINI_PROMPT_TEMPLATE
    if GEMINI_PROMPT_TEMPLATE: return GEMINI_PROMPT_TEMPLATE
    combined = []
    try:
        for fname in ALL_PROMPT_FILES[:-1]:
            with open(fname, 'r', encoding='utf-8') as f: combined.append(f.read().strip())
        base = combined[0] + "\n" + "\n".join(combined[1:])
        base += "\n\n【重要】\n該当する情報がない場合は『なし』とだけ出力してください。\n記事本文:\n{TEXT_TO_ANALYZE}"
        GEMINI_PROMPT_TEMPLATE = base
        return base
    except: return ""

def load_comment_prompt() -> str:
    global COMMENT_PROMPT_TEMPLATE
    if COMMENT_PROMPT_TEMPLATE: return COMMENT_PROMPT_TEMPLATE
    try:
        with open("prompt_comment_analysis.txt", 'r', encoding='utf-8') as f:
            COMMENT_PROMPT_TEMPLATE = f.read().strip().replace("{COMMENT_TEXT}", "{TEXT_TO_ANALYZE}")
            return COMMENT_PROMPT_TEMPLATE
    except: return ""

def request_with_retry(url: str, max_retries: int = 3) -> Optional[requests.Response]:
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=REQ_HEADERS, timeout=20)
            if res.status_code == 404: return None
            res.raise_for_status()
            return res
        except: time.sleep(2)
    return None

def set_row_height(ws: gspread.Worksheet, row_height_pixels: int):
    try: ws.spreadsheet.batch_update({"requests": [{"updateDimensionProperties": {"range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": 1, "endIndex": ws.row_count}, "properties": {"pixelSize": row_height_pixels}, "fields": "pixelSize"}}]})
    except: pass

def update_sheet_with_retry(ws, range_name, values, max_retries=3):
    for attempt in range(max_retries):
        try:
            ws.update(range_name=range_name, values=values, value_input_option='USER_ENTERED')
            return
        except: time.sleep(30)

def call_gemini_api(prompt: str, is_batch: bool = False, schema: dict = None) -> Any:
    increment_request_count()
    client = get_current_gemini_client()
    if not client: return None
    for attempt in range(10):
        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=schema)
            )
            return json.loads(response.text.strip())
        except ResourceExhausted:
            rotate_api_key(reason="429"); time.sleep(27)
        except Exception as e:
            if "429" in str(e): rotate_api_key(reason="429"); time.sleep(27)
            else: return None
    return None

def analyze_article_batch(texts: List[str]) -> Optional[List[Dict[str, str]]]:
    prompt = load_merged_prompt().replace("{TEXT_TO_ANALYZE}", "\n".join([f"\n【記事 {i+1}】\n{t[:4000]}" for i, t in enumerate(texts)]))
    prompt += f"\n\n※各記事について分析し、{len(texts)}個のオブジェクトを含むJSONリストで返してください。"
    schema = {"type": "array", "items": {"type": "object", "properties": {"company_info": {"type": "string"}, "category": {"type": "string"}, "sentiment": {"type": "string"}, "nissan_related": {"type": "string"}, "nissan_negative": {"type": "string"}}}}
    return call_gemini_api(prompt, is_batch=True, schema=schema)

def analyze_article_single(text: str) -> Dict[str, str]:
    prompt = load_merged_prompt().replace("{TEXT_TO_ANALYZE}", text[:4000])
    schema = {"type": "object", "properties": {"company_info": {"type": "string"}, "category": {"type": "string"}, "sentiment": {"type": "string"}, "nissan_related": {"type": "string"}, "nissan_negative": {"type": "string"}}}
    res = call_gemini_api(prompt, is_batch=False, schema=schema)
    return res if res else {"company_info": "N/A", "category": "N/A", "sentiment": "N/A", "nissan_related": "なし", "nissan_negative": "なし"}

def analyze_comment_summary(text: str) -> Dict[str, Any]:
    prompt = load_comment_prompt().replace("{TEXT_TO_ANALYZE}", text[:100000])
    schema = {"type": "object", "properties": {"nissan_product_neg": {"type": "string"}, "summaries": {"type": "array", "items": {"type": "string"}}, "topic_ranking": {"type": "array", "items": {"type": "string"}}}}
    res = call_gemini_api(prompt, is_batch=False, schema=schema)
    return res if res else {"nissan_product_neg": "なし", "summaries": ["-"], "topic_ranking": ["-"]}

def get_yahoo_news_with_selenium(keyword: str) -> list[dict]:
    opts = Options()
    opts.add_argument("--headless=new"); opts.add_argument("--no-sandbox"); opts.add_argument("--disable-gpu")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.get(f"https://news.yahoo.co.jp/search?p={keyword}")
    time.sleep(3)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()
    data = []
    for art in soup.find_all("li", class_=re.compile("sc-1u4589e-0")):
        try:
            link = art.find("a", href=True)["href"]
            if link.startswith("https://news.yahoo.co.jp/articles/"):
                data.append({"URL": link, "タイトル": art.text, "投稿日時": "", "ソース": ""})
        except: continue
    return data

def fetch_article_body_and_comments(base_url: str) -> Tuple[str, int, Optional[str]]:
    res = request_with_retry(base_url)
    if not res: return "本文取得不可", -1, None
    soup = BeautifulSoup(res.text, 'html.parser')
    body = soup.find('article')
    return body.get_text() if body else "本文取得不可", 0, None

def ensure_source_sheet(gc):
    sh = gc.open_by_key(SOURCE_SPREADSHEET_ID)
    try: return sh.worksheet(SOURCE_SHEET_NAME)
    except: return sh.add_worksheet(SOURCE_SHEET_NAME, MAX_SHEET_ROWS_FOR_REPLACE, len(YAHOO_SHEET_HEADERS))

def fetch_details_and_update_sheet(gc: gspread.Client):
    ws = ensure_source_sheet(gc)
    rows = ws.get_all_values()[1:]
    for idx, row in enumerate(rows):
        url = row[0]
        if not url.startswith('http'): continue
        body, cmt, date = fetch_article_body_and_comments(url)
        update_sheet_with_retry(ws, f'C{idx+2}:F{idx+2}', [[date, "ソース", body, cmt]])
        time.sleep(1)

def sort_yahoo_sheet(gc: gspread.Client):
    ws = ensure_source_sheet(gc)
    ws.sort((3, 'des'), range=f'A2:Z{len(ws.get_all_values())}')

def analyze_with_gemini_and_update_sheet(gc: gspread.Client):
    ws = ensure_source_sheet(gc)
    rows = ws.get_all_values()[1:]
    now = jst_now()
    one_day_ago = now - timedelta(hours=24)
    tasks = []
    for idx, row in enumerate(rows):
        dt = parse_post_date(row[2], now)
        if dt and dt < one_day_ago: break
        if not row[4] or row[4] == "本文取得不可": continue
        tasks.append({"row": idx + 2, "body": row[4]})
    
    for i, t in enumerate(tasks):
        if i < 3:
            res = analyze_article_single(t['body'])
            update_sheet_with_retry(ws, f"G{t['row']}:K{t['row']}", [[res["company_info"], res["category"], res["sentiment"], res["nissan_related"], res["nissan_negative"]]])
            time.sleep(NORMAL_WAIT_SECONDS)
        else: break

def main():
    gc = build_gspread_client()
    keys = load_keywords(KEYWORD_FILE)
    for k in keys:
        data = get_yahoo_news_with_selenium(k)
        ws = ensure_source_sheet(gc)
        ws.append_rows([[d['URL'], d['タイトル'], d['投稿日時'], d['ソース']] for d in data])
    fetch_details_and_update_sheet(gc)
    sort_yahoo_sheet(gc)
    analyze_with_gemini_and_update_sheet(gc)
    comment_scraper.run_comment_collection(gc, SHARED_SPREADSHEET_ID, SOURCE_SHEET_NAME, analyze_comment_summary)

if __name__ == '__main__':
    main()
