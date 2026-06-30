import time
import re
import requests
from bs4 import BeautifulSoup
import gspread
from datetime import datetime, timedelta, timezone # 修正: JST対応

# --- Selenium 関連 ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

# --- 設定・クラス名定義 ---
COMMENTS_SHEET_NAME = "Comments"
REQ_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"}
MAX_SELENIUM_PAGES = 10 
TZ_JST = timezone(timedelta(hours=9)) # 修正: JST定義

# YahooニュースのHTML構造（クラス名）
CLS_ARTICLE = "sc-169yn8p-3" # コメント枠
CLS_USER_NAME = "sc-169yn8p-7" # 投稿者名
CLS_BODY = "sc-169yn8p-10"    # 本文
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
    articles = soup.find_all('article', class_=CLS_ARTICLE)
    ignore_words = ["このコメントを削除しますか", "コメントを削除しました", "違反報告する", "非表示・報告", "投稿を受け付けました"]

    for art in articles:
        body_elem = art.find('p', class_=CLS_BODY)
        if not body_elem: continue
        comment_body = body_elem.get_text(strip=True)
        if any(word in comment_body for word in ignore_words): continue

        user_elem = art.find('a', class_=CLS_USER_NAME)
        user_name = user_elem.get_text(strip=True) if user_elem else "匿名"

        full_text = f"【投稿者: {user_name}】\n{comment_body}"
        if full_text in seen_comments: continue
        
        seen_comments.add(full_text)
        extracted_data.append(full_text)
    return extracted_data

def fetch_comments_hybrid(article_url: str, max_limit: int) -> tuple[list[str], str]:
    """ ハイブリッド方式でコメントを取得 """
    base_url = article_url.split('?')[0]
    if not base_url.endswith('/comments'):
        base_url = base_url.split('/comments')[0] + '/comments' if '/comments' in base_url else f"{base_url}/comments"

    all_comments_data = [] 
    seen_comments = set()
    print(f"    - コメント取得開始 (上限:{max_limit}件): {base_url}")

    driver = setup_driver()
    if driver:
        for page in range(1, MAX_SELENIUM_PAGES + 1):
            if len(all_comments_data) >= max_limit: break
            try:
                driver.get(f"{base_url}?page={page}")
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, CLS_ARTICLE)))
                expand_buttons = driver.find_elements(By.XPATH, "//*[contains(text(), 'もっと見る') or contains(text(), '続きを読む')]")
                for btn in expand_buttons:
                    try:
                        if btn.is_displayed(): driver.execute_script("arguments[0].click();", btn)
                    except: pass
                time.sleep(0.5)
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                new_data = extract_comments_from_soup(soup, seen_comments)
                if not new_data: break
                all_comments_data.extend(new_data)
            except Exception: break
        driver.quit()

    ai_target_text = "\n".join(all_comments_data[:100]) # AI用は上位100件
    
    page = (len(all_comments_data) // 10) + 1
    while len(all_comments_data) < max_limit:
        try:
            res = requests.get(f"{base_url}?page={page}", headers=REQ_HEADERS, timeout=10)
            if res.status_code != 200: break
            soup = BeautifulSoup(res.text, 'html.parser')
            new_data = extract_comments_from_soup(soup, seen_comments)
            if not new_data: break
            all_comments_data.extend(new_data)
            page += 1
            if len(all_comments_data) >= max_limit: break
            time.sleep(0.5)
        except: break

    final_comments = all_comments_data[:max_limit]
    merged_columns = ["\n\n".join(final_comments[i:i+10]) for i in range(0, len(final_comments), 10)]
    print(f"    - 取得完了: {len(final_comments)}件")
    return merged_columns, ai_target_text

def set_row_height(ws, pixels):
    try:
        requests = [{"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "ROWS", "startIndex": 1, "endIndex": ws.row_count},
            "properties": {"pixelSize": pixels}, "fields": "pixelSize"}}]
        ws.spreadsheet.batch_update({"requests": requests})
    except: pass

def run_comment_collection(gc: gspread.Client, source_sheet_id: str, source_sheet_name: str, summarizer_func):
    
    print("\n=====   ステップ⑤ コメント収集・要約・保存 (最新最適化版) =====")
    sh = gc.open_by_key(source_sheet_id)
    try: source_ws = sh.worksheet(source_sheet_name)
    except: return

    dest_ws = ensure_comments_sheet(sh)
    dest_rows = dest_ws.get_all_values()
    existing_urls = set(row[0] for row in dest_rows[1:] if row) if len(dest_rows) > 1 else set()

    source_rows = source_ws.get_all_values()
    if len(source_rows) < 2: return
    
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

        # --- 3日前より古い記事を除外する判定 ---
        try:
            clean_date_str = re.sub(r'\([一-龠]\)', '', str(post_date)).strip() 
            dt_post = datetime.strptime(clean_date_str, '%Y/%m/%d %H:%M:%S')
            
            # JSTベースで比較
            three_days_ago = datetime.now(TZ_JST) - timedelta(days=3)
            if dt_post.replace(tzinfo=TZ_JST) < three_days_ago:
                continue 
        except Exception:
            try:
                dt_post = datetime.strptime(clean_date_str, '%Y/%m/%d %H:%M')
                if dt_post.replace(tzinfo=TZ_JST) < (datetime.now(TZ_JST) - timedelta(days=3)): continue
            except:
                pass 

        # --- 条件判定と取得上限の設定 ---
        is_target = False
        max_limit = 2000 
        if not category.startswith("その他") and item['count'] >= 30:
            if target_company.startswith("日産") and item['count'] >= 60:
                is_target, max_limit = True, 1500
            elif target_company.startswith("トヨタ") and item['count'] >= 100:
                is_target, max_limit = True, 1500
            elif target_company.startswith("ホンダ") and item['count'] >= 100:
                is_target, max_limit = True, 1500
            elif target_company.startswith("スズキ") and item['count'] >= 100:
                is_target, max_limit = True, 1500
            elif target_company.startswith("スバル") and item['count'] >= 100:
                is_target, max_limit = True, 1500
            elif target_company.startswith("マツダ") and item['count'] >= 100:
                is_target, max_limit = True, 1500
            elif target_company.startswith("三菱") and item['count'] >= 100:
                is_target, max_limit = True, 1500
            elif target_company.startswith("ダイハツ") and item['count'] >= 100:
                is_target, max_limit = True, 1500
            elif str(nissan_neg_text).strip() not in ["", "なし", "N/A", "-"]:
                is_target, max_limit = True, 1500
        
        if is_target:
            print(f"  - 対象記事発見: {title[:25]}...")
            comment_cols, full_text_for_ai = fetch_comments_hybrid(url, max_limit)
            
            if comment_cols:
                summary_data = summarizer_func(full_text_for_ai, target_company=target_company)
                prod_neg = summary_data.get("nissan_product_neg", "N/A")
                summary_combined = "\n\n".join(summary_data.get("summaries", [])) or "-"
                ranking_combined = "\n".join(summary_data.get("topic_ranking", [])) or "-"

                base_data = [url, title, post_date, source, comment_count_str, prod_neg, summary_combined, ranking_combined]
                
                chunk_size = 50
                comment_chunks = [comment_cols[i:i + chunk_size] for i in range(0, len(comment_cols), chunk_size)]

                try:
                    first_row_data = base_data + (comment_chunks[0] if len(comment_chunks) > 0 else [])
                    dest_ws.append_rows([first_row_data], value_input_option='USER_ENTERED')
                    
                    current_row_idx = len(dest_ws.col_values(1)) 
                    
                    if len(comment_chunks) > 1:
                        print(f"    > 残りのコメントを分割書き込み中...")

                        for j, chunk in enumerate(comment_chunks[1:]):
                            start_col = 9 + (j + 1) * chunk_size
                            dest_ws.update(
                                range_name=f"R{current_row_idx}C{start_col}",
                                values=[chunk],
                                value_input_option='USER_ENTERED',
                            )
                            time.sleep(1)

                except Exception as e:
                    print(f"      ! 書き込みエラー: {e}")

                process_count += 1
                time.sleep(60)

    if process_count > 0:
        try:
            last_row = len(dest_ws.col_values(1))
            if last_row > 1: dest_ws.sort((3, 'des'), range=f'A2:KN{last_row}') 
        except: pass
        set_row_height(dest_ws, 21)
    print(f"    コメント収集完了: {process_count} 件処理しました。")
