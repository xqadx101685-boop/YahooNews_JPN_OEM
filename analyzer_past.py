import os
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
import gspread
import requests
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
from google import genai
from google.genai import types

# ====== 設定 ======
SPREADSHEET_KEY = os.environ.get("SPREADSHEET_KEY")
TZ_JST = timezone(timedelta(hours=9))
AVAILABLE_API_KEYS = [os.environ.get(f"GOOGLE_API_KEY_{i}") for i in range(1, 6) if os.environ.get(f"GOOGLE_API_KEY_{i}")]
CURRENT_KEY_INDEX = 0

def jst_now(): return datetime.now(TZ_JST)

def build_gspread_client():
    info = json.loads(os.environ.get("GCP_SERVICE_ACCOUNT_KEY"))
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
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

def fetch_article_body(url: str) -> str:
    try:
        res = requests.get(url, timeout=20)
        soup = BeautifulSoup(res.text, 'html.parser')
        body = "\n".join([p.get_text() for p in soup.find_all('p', class_=re.compile(r'article_body|article_detail'))])
        return body if body else "本文取得不可"
    except: return "本文取得不可"

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
    gc = build_gspread_client()
    ws = gc.open_by_key(SPREADSHEET_KEY).worksheet("Yahoo")
    now = jst_now()
    four_days_ago = now - timedelta(days=4)
    one_day_ago = now - timedelta(hours=24)

    rows = ws.get_all_values()[1:]
    for idx, row in enumerate(rows):
        row_num = idx + 2
        # G列(インデックス6)が空なら未分析とみなす
        if len(row) > 6 and row[6].strip() != "": continue
        
        post_dt = parse_post_date(row[2], now)
        # 24時間〜4日以内の記事のみを対象
        if post_dt and (one_day_ago > post_dt >= four_days_ago):
            print(f"Row {row_num}: 過去分分析実行 ({row[1][:20]}...)")
            body = fetch_article_body(row[0])
            res = call_gemini(f"分析: {body}")
            ws.update(f'G{row_num}:K{row_num}', [[res["company_info"], res["category"], res["sentiment"], res["nissan_related"], res["nissan_negative"]]])
            time.sleep(2) # API負荷軽減

    print("過去分の分析完了")

if __name__ == '__main__':
    main()
