# --- 3日前より古い記事を除外する判定 ---
        try:
            # スプレッドシートの形式 "2026/03/30 15:15:00" に対応
            # 念のため曜日表記が含まれていても良いように正規表現は残す
            clean_date_str = re.sub(r'\([一-龠]\)', '', post_date).strip() 
            
            # 秒まである形式 "%Y/%m/%d %H:%M:%S" でパース
            dt_post = datetime.strptime(clean_date_str, '%Y/%m/%d %H:%M:%S')
            
            three_days_ago = datetime.now() - timedelta(days=3)
            
            if dt_post < three_days_ago:
                continue # 3日以上前ならスキップ
        except Exception as e:
            # パースに失敗した場合は念のため「分まで」の形式も試す
            try:
                dt_post = datetime.strptime(clean_date_str, '%Y/%m/%d %H:%M')
                if dt_post < (datetime.now() - timedelta(days=3)): continue
            except:
                print(f"      ! 日付判定スキップ({post_date}): {e}")
