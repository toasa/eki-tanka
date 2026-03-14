import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import re
import os

# ---  セッションと自動リトライの設定 ---


def create_session():
    session = requests.Session()
    # 429や500番台のエラー時に自動リトライする設定
    # backoff_factor=2 により、1回目2秒、2回目4秒、3回目8秒...と待機時間が増えます
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.verify = False
    return session


session = create_session()


def katakana_to_hiragana(text):
    return "".join(chr(ord(c) - 0x60) if 0x30A1 <= ord(c) <= 0x30F6 else c for c in text)


def get_kana_from_wikipedia(station_name):
    url = "https://ja.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "prop": "extracts",
        "titles": f"{station_name}駅",
        "format": "json",
        "exchars": "200",
        "explaintext": "1",
        "redirects": "1"
    }
    # --- User-Agent に自身のメールアドレス等を記載する ---
    headers = {
        "User-Agent": "StationKanaFetcher/1.1 (hogehoge@fugafuga.com) python-requests"
    }

    try:
        res = session.get(url, params=params, headers=headers, timeout=10)
        res.raise_for_status()
        data = res.json()

        pages = data.get('query', {}).get('pages', {})
        page = list(pages.values())[0]

        if 'missing' in page:
            return None

        extract = page.get('extract', '')

        match = re.search(r'（(?:[^）]*?・)?([ぁ-んァ-ヶー]+)えき', extract)
        if match:
            return katakana_to_hiragana(match.group(1))

        match2 = re.search(r'（([ぁ-んァ-ヶー]+)[）、]', extract)
        if match2:
            return katakana_to_hiragana(match2.group(1))

    except Exception as e:
        print(f"Wikipediaエラー: {e}")
        pass

    return None


def get_station_kana(station_name, postal_code):
    if pd.notna(postal_code):
        postal_str = str(postal_code).replace('-', '')
        url = "http://geoapi.heartrails.com/api/json"
        params = {
            "method": "getStations",
            "postal": postal_str
        }
        try:
            res = session.get(url, params=params, timeout=10)
            res.raise_for_status()
            data = res.json()
            if 'station' in data.get('response', {}):
                stations = data['response']['station']
                for st in stations:
                    if st['name'] == station_name and 'kana' in st:
                        return st['kana'], "HeartRails"
        except Exception as e:
            pass

    kana = get_kana_from_wikipedia(station_name)
    if kana:
        return kana, "Wikipedia"

    return None, "取得失敗"


# --- 実行部分 ---
input_file = './data/eki-data/station20260206free.csv'
output_file = './data/station_with_kana.csv'

df = pd.read_csv(input_file)

# --- 途中から再開できる仕組み ---
if os.path.exists(output_file):
    # すでにファイルがあれば、その行数を確認して途中から再開
    df_done = pd.read_csv(output_file)
    done_count = len(df_done)
    print(f"すでに {done_count} 件処理されています。続きから再開します...")
    df_todo = df.iloc[done_count:].copy()
else:
    print("最初から処理を開始します...")
    df_todo = df.copy()
    done_count = 0

# ヘッダーを書き込むかどうか（初回のみTrue）
write_header = (done_count == 0)

for index, row in df_todo.iterrows():
    station_name = row['station_name']

    kana, source = get_station_kana(station_name, row['post'])

    display_kana = kana if kana else "【! 取得不可 !】"
    print(f"[{index}] {station_name} -> {display_kana} ({source})")

    # 1件処理するごとにDataFrameを作り、CSVの末尾に「追記モード('a')」で保存
    row_result = row.copy()
    row_result['station_name_hiragana'] = kana
    row_result['source'] = source

    pd.DataFrame([row_result]).to_csv(
        output_file,
        mode='a',
        header=write_header,
        index=False,
        encoding='utf-8-sig'
    )

    # 次のループからはヘッダーは不要
    write_header = False

    # Wikipedia等の負荷軽減のため少し長めに待機
    time.sleep(1)

print("完了しました！")
