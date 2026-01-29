import time
import re
import requests
from bs4 import BeautifulSoup
import gspread

# --- Selenium 関連 ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# --- 設定・クラス名定義（Yahooの仕様変更時はここを修正） ---
COMMENTS_SHEET_NAME = "Comments"
REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}
MAX_SELENIUM_PAGES = 10  # Seleniumで取得するページ数上限

# YahooニュースのHTML構造（クラス名）
CLS_ARTICLE = "sc-169yn8p-3" # コメント1件を包む枠
CLS_USER_NAME = "sc-169yn8p-7" # 投稿者名
CLS_BODY = "sc-169yn8p-10"    # コメント本文
CLS_TIME = "sc-169yn8p-9"     # 投稿日時

def ensure_comments_sheet(sh: gspread.Spreadsheet):
    """ Commentsシートがなければ作成し、ヘッダーを設定する """
    try:
        ws = sh.worksheet(COMMENTS_SHEET_NAME)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=COMMENTS_SHEET_NAME, rows="1000", cols="300")
        headers = ["URL", "タイトル", "投稿日時", "ソース", "コメント数", "製品批判有無", "コメント要約(全体)", "話題ランキング(TOP5)"]
        for i in range(0, 240): 
            start = i * 10 + 1
            end = (i + 1) * 10
            headers.append(f"コメント：{start} - {end}")
        ws.update(range_name='A1', values=[headers])
        return ws
    return ws

def setup_driver():
    """ Seleniumドライバの初期化 """
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={REQ_HEADERS['User-Agent']}")
    options.add_argument("--disable-blink-features=AutomationControlled")
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        return driver
    except Exception as e:
        print(f"      ! Driver初期化失敗: {e}")
        return None

def extract_comments_from_soup(soup, seen_comments):
    """ BeautifulSoupから特定のクラスを指定してコメントを抽出する共通ロジック """
    extracted_data = []
    # コメント単位のarticleを検索
    articles = soup.find_all('article', class_=CLS_ARTICLE)
    
    ignore_words = ["このコメントを削除しますか", "コメントを削除しました", "違反報告する", "非表示・報告", "投稿を受け付けました"]

    for art in articles:
        # 本文の取得
        body_elem = art.find('p', class_=CLS_BODY)
        if not body_elem: continue
        comment_body = body_elem.get_text(strip=True)

        # 不要なシステムメッセージが含まれる場合はスキップ
        if any(word in comment_body for word in ignore_words): continue

        # ユーザー名の取得
        user_elem = art.find('a', class_=CLS_USER_NAME)
        user_name = user_elem.get_text(strip=True) if user_elem else "匿名"

        # フルテキスト化して重複チェック
        full_text = f"【投稿者: {user_name}】\n{comment_body}"
        if full_text in seen_comments: continue
        
        seen_comments.add(full_text)
        extracted_data.append(full_text)
        
    return extracted_data

def fetch_comments_hybrid(article_url: str) -> tuple[list[str], str]:
    """ ハイブリッド方式でコメントを取得（正確なクラス指定版） """
    base_url = article_url.split('?')[0]
    if not base_url.endswith('/comments'):
        base_url = base_url.split('/comments')[0] + '/comments' if '/comments' in base_url else f"{base_url}/comments"

    all_comments_data = [] 
    seen_comments = set()
    
    print(f"    - コメント取得開始(最適化版): {base_url}")

    # --- Phase 1: Selenium (Top 100件 / 本文展開あり) ---
    driver = setup_driver()
    if driver:
        for page in range(1, MAX_SELENIUM_PAGES + 1):
            try:
                driver.get(f"{base_url}?page={page}")
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, CLS_ARTICLE)))
                
                # 「もっと見る」ボタンをすべてクリック
                expand_buttons = driver.find_elements(By.XPATH, "//*[contains(text(), 'もっと見る') or contains(text(), '続きを読む')]")
                for btn in expand_buttons:
                    try:
                        if btn.is_displayed():
                            driver.execute_script("arguments[0].click();", btn)
                    except: pass
                
                time.sleep(0.5) # 展開待ち
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                new_data = extract_comments_from_soup(soup, seen_comments)
                if not new_data: break
                all_comments_data.extend(new_data)
                time.sleep(1)
            except Exception as e:
                print(f"      ! Selenium中断(p{page}): {e}")
                break
        driver.quit()

    ai_target_text = "\n".join(all_comments_data) # AI分析用（Selenium取得分）
    limit_for_ai = len(all_comments_data)

    # --- Phase 2: Requests (101件目以降 / 高速取得) ---
    start_page = (limit_for_ai // 10) + 2
    page = start_page
    while True:
        try:
            res = requests.get(f"{base_url}?page={page}", headers=REQ_HEADERS, timeout=10)
            if res.status_code != 200: break
            soup = BeautifulSoup(res.text, 'html.parser')
            new_data = extract_comments_from_soup(soup, seen_comments)
            if not new_data: break
            all_comments_data.extend(new_data)
            page += 1
            time.sleep(1)
        except: break

    # 保存用に10件ずつ結合
    merged_columns = ["\n\n".join(all_comments_data[i:i+10]) for i in range(0, len(all_comments_data), 10)]
    print(f"    - 全取得完了: 合計{len(all_comments_data)}件")
    return merged_columns, ai_target_text

def set_row_height(ws, pixels):
    try:
        requests = [{"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": 1, "endIndex": ws.row_count},
            "properties": {"pixelSize": pixels}, "fields": "pixelSize"}}]
        ws.spreadsheet.batch_update({"requests": requests})
    except: pass

def run_comment_collection(gc: gspread.Client, source_sheet_id: str, source_sheet_name: str, summarizer_func):
    print("\n=====   ステップ⑤ コメント収集・要約・保存 (最適化版) =====")
    sh = gc.open_by_key(source_sheet_id)
    try: source_ws = sh.worksheet(source_sheet_name)
    except: return

    dest_ws = ensure_comments_sheet(sh)
    dest_rows = dest_ws.get_all_values()
    existing_urls = set(row[0] for row in dest_rows[1:] if row) if len(dest_rows) > 1 else set()

    source_rows = source_ws.get_all_values()
    if len(source_rows) < 2: return
    
    # コメント数が多い順にソートして処理
    target_data = []
    for row in source_rows[1:]:
        if len(row) < 11: continue
        try: cnt = int(re.sub(r'\D', '', str(row[5])))
        except: cnt = 0
        target_data.append({"count": cnt, "data": row})
    
    target_data.sort(key=lambda x: x['count'], reverse=True)

    process_count = 0
    for item in target_data:
        row = item['data']
        url, title, post_date, source, comment_count_str = row[0], row[1], row[2], row[3], row[5]
        target_company, category, nissan_neg_text = row[6], row[7], row[10]

        if url in existing_urls: continue

        # 条件判定（日産系100件以上、またはネガ文あり）
        is_target = False
        if not category.startswith("その他") and item['count'] > 0:
            if target_company.startswith("日産") and item['count'] >= 100: is_target = True
            elif str(nissan_neg_text).strip() not in ["", "なし", "N/A", "-"]: is_target = True
        
        if is_target:
            print(f"  - 対象記事発見: {title[:20]}...")
            comment_cols, full_text_for_ai = fetch_comments_hybrid(url)
            
            if comment_cols:
                summary_data = summarizer_func(full_text_for_ai)
                prod_neg = summary_data.get("nissan_product_neg", "N/A")
                summary_combined = "\n\n".join(summary_data.get("summaries", [])) or "-"
                ranking_combined = "\n".join(summary_data.get("topic_ranking", [])) or "-"

                row_data = [url, title, post_date, source, comment_count_str, prod_neg, summary_combined, ranking_combined] + comment_cols
                dest_ws.append_rows([row_data], value_input_option='USER_ENTERED')
                process_count += 1
                time.sleep(60) # Gemini制限回避

    if process_count > 0:
        try:
            last_row = len(dest_ws.col_values(1))
            if last_row > 1: dest_ws.sort((3, 'des'), range=f'A2:KN{last_row}') 
        except: pass
        set_row_height(dest_ws, 21)

    print(f"   コメント収集完了: {process_count} 件処理しました。")
