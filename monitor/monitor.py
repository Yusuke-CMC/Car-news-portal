#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
クルマ速報ボード - 新車情報 監視・自動公開スクリプト

各メーカーの公式ニュースリリースを定期的にチェックし、
前回実行時から増えた新着記事だけを review_queue.csv に書き出す。
車種関連キーワードに合致した記事は、カテゴリ/種別/日付/概要を自動で
組み立てたうえで cars.json に直接追記し、ポータルに自動反映する。

※ 自動生成される概要(summary)は記事タイトルをもとにした簡易なものであり、
  価格・寸法などのスペックは自動取得しない（誤情報を載せないため）。
  内容を精査したい場合は、あとから cars.json を直接編集すればよい。

※ ページ型（RSSでない）の情報源は、記事一覧から拾ったリンクの「本当の発表日」が
  分からないため、URLに埋め込まれた日付（例: subaru.co.jp/news/2026_08_06_xxxxx）
  を正規表現で抽出して使う。それでも日付が分からない記事や、抽出できても
  RECENCY_CUTOFF_DAYS より古い記事は「対象外」として扱い、cars.jsonには追加しない
  （何年も前のアーカイブ記事が「本日の新着」として紛れ込むのを防ぐため）。

使い方:
    pip install feedparser beautifulsoup4 requests
    python3 monitor.py

    ※ cron等で1日1回実行する運用を想定。
    ※ 初回実行時は既存記事が全件「新着」として出力されます（ベースライン作成のため）。
"""

import csv
import json
import os
import re
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

import feedparser
import requests
from bs4 import BeautifulSoup

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(BASE_DIR)
SOURCES_PATH = os.path.join(BASE_DIR, "sources.json")
STATE_PATH = os.path.join(BASE_DIR, "state.json")
QUEUE_CSV_PATH = os.path.join(BASE_DIR, "review_queue.csv")
NEW_ENTRIES_JSON_PATH = os.path.join(BASE_DIR, "new_entries_template.json")
CARS_JSON_PATH = os.path.join(REPO_ROOT, "cars.json")

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; CarNewsMonitor/1.0; +local-use)"}

TYPE_KEYWORD_MAP = [
    "フルモデルチェンジ", "一部仕様変更", "商品改良", "一部改良",
    "先行公開", "新型発表", "新型", "発売",
]

# 1回の実行・1メーカーあたりcars.jsonに自動追加できる件数の上限。
# 情報源が想定外に大きなアーカイブだった場合等の暴走防止のための安全装置。
MAX_NEW_PER_MAKER_PER_RUN = 15

# ページ型の情報源で、記事の実際の日付がこの日数より古いと判定された場合、
# （URLから日付を推定できた場合のみ判定し、推定できなければ許可する）
# 「新着」としては扱わずcars.jsonに追加しない。アーカイブ全体の取り込みを防ぐ。
RECENCY_CUTOFF_DAYS = 45

# ページのURLに埋め込まれた日付を抽出するためのパターン（サイトごとに形式が違う）。
# 例:
#   subaru.co.jp/news/2026_08_06_162650      -> 2026-08-06
#   mitsubishi-motors.com/.../20260817_2.html -> 2026-08-17
#   daihatsu.com/.../20260202-1.html          -> 2026-02-02
#   global.nissannews.com/.../260820-01-j     -> 2026-08-20 (YYMMDD)
URL_DATE_PATTERNS = [
    re.compile(r"(\d{4})_(\d{2})_(\d{2})"),
    re.compile(r"/(\d{4})(\d{2})(\d{2})[_\-]"),
    re.compile(r"/(\d{2})(\d{2})(\d{2})-\d+-"),
]


def load_json(path, default):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def is_relevant(title, keywords):
    return any(kw in title for kw in keywords)


def infer_type(title):
    """タイトルに含まれるキーワードから種別ラベルを推定する。どれにも合致しなければ「発表」。"""
    for kw in TYPE_KEYWORD_MAP:
        if kw in title:
            return kw
    return "発表"


def extract_date_from_url(url):
    """URLに埋め込まれた日付らしき文字列を抽出し、YYYY-MM-DD形式で返す。見つからなければNone。"""
    for pattern in URL_DATE_PATTERNS:
        m = pattern.search(url)
        if not m:
            continue
        g1, g2, g3 = m.group(1), m.group(2), m.group(3)
        year = g1 if len(g1) == 4 else ("20" + g1)
        try:
            candidate = f"{year}-{g2}-{g3}"
            dt = datetime.strptime(candidate, "%Y-%m-%d")
        except ValueError:
            continue
        # 明らかにおかしい日付（未来すぎる/古すぎる）は日付抽出の誤検知とみなして無視する
        if datetime(2015, 1, 1) <= dt <= datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(days=2):
            return candidate
    return None


def resolve_date(published_str, url_date):
    """RSSのpublished、URLから抽出した日付、の優先順で発表日を決定。どちらも無ければ今日の日付。"""
    if published_str:
        try:
            dt = parsedate_to_datetime(published_str)
            return dt.date().isoformat()
        except (TypeError, ValueError):
            pass
    if url_date:
        return url_date
    return datetime.now(timezone.utc).date().isoformat()


def is_too_old(date_str, url_date):
    """URLから日付が確実に抽出できていて、かつそれが古すぎる場合のみTrueを返す。
    日付が推定できない記事（url_dateがNone）は、判断材料が無いため許可する（Falseを返す）。
    """
    if not url_date:
        return False
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return False
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=RECENCY_CUTOFF_DAYS)
    return dt < cutoff


def fetch_rss(url):
    """RSS/Atomフィードから記事一覧を取得"""
    feed = feedparser.parse(url)
    items = []
    for e in feed.entries:
        link = getattr(e, "link", "")
        title = (getattr(e, "title", "") or "").strip()
        published = getattr(e, "published", "") or getattr(e, "updated", "")
        uid = getattr(e, "id", None) or link or title
        categories = [t.get("term", "") for t in getattr(e, "tags", [])] if getattr(e, "tags", None) else []
        items.append({
            "uid": uid, "title": title, "url": link, "published": published,
            "categories": categories, "url_date": None,
        })
    return items


def fetch_page_links(url):
    """RSSがないページから、リンクテキストを総当たりで抽出（簡易差分監視用）。
    個々の記事の本当の日付はここでは分からないため、URLに埋め込まれた日付があれば抽出しておく。
    """
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    if not resp.encoding or resp.encoding.lower() == "iso-8859-1":
        resp.encoding = resp.apparent_encoding
    soup = BeautifulSoup(resp.text, "html.parser")
    items = []
    seen_local = set()
    for a in soup.find_all("a"):
        text = (a.get_text() or "").strip()
        href = a.get("href") or ""
        if not text or len(text) < 8:
            continue
        if href.startswith("#") or href.lower().startswith("javascript:"):
            continue
        full_url = urljoin(url, href)
        key = (text, full_url)
        if key in seen_local:
            continue
        seen_local.add(key)
        items.append({
            "uid": full_url, "title": text, "url": full_url, "published": "",
            "categories": [], "url_date": extract_date_from_url(full_url),
        })
    return items


def main():
    config = load_json(SOURCES_PATH, {"sources": [], "relevance_keywords": []})
    state = load_json(STATE_PATH, {})
    keywords = config.get("relevance_keywords", [])

    new_rows = []
    new_entries_for_portal = []

    for src in config["sources"]:
        maker = src["maker"]
        label = src.get("maker_label", maker)
        url = src.get("url", "")
        stype = src.get("type", "page")

        if not url:
            print(f"[SKIP] {label}: URL未設定（sources.jsonを編集してください）")
            continue

        print(f"[CHECK] {label} ({stype}) ...")
        try:
            items = fetch_rss(url) if stype == "rss" else fetch_page_links(url)
        except Exception as e:
            print(f"  -> 取得失敗: {e}")
            continue

        seen = set(state.get(maker, []))
        newly_seen_uids = set()  # 「関連なし」または「古すぎる」記事。今後見なくてよいので即座にseen扱いにする。
        maker_new_count = 0
        too_old_count = 0

        for it in items:
            uid = it["uid"]
            if uid in seen:
                continue
            maker_new_count += 1
            relevant = is_relevant(it["title"], keywords)
            resolved_date = resolve_date(it["published"], it.get("url_date"))

            new_rows.append({
                "checked_at": datetime.now().isoformat(timespec="seconds"),
                "maker": label,
                "type": stype,
                "published": it["published"] or it.get("url_date") or "",
                "title": it["title"],
                "url": it["url"],
                "relevant": "○" if relevant else "",
            })

            if not relevant:
                newly_seen_uids.add(uid)
                continue

            if is_too_old(resolved_date, it.get("url_date")):
                # URLから確実に古い記事だと分かった場合はアーカイブ扱いにして、追加せずseenにする。
                too_old_count += 1
                newly_seen_uids.add(uid)
                continue

            effective_maker = maker
            if maker == "toyota" and "Lexus" in it.get("categories", []):
                effective_maker = "lexus"

            # 上限を超えて今回追加されなかった場合に備え、uidを持たせておく（seen登録の判断は後段で行う）。
            new_entries_for_portal.append({
                "_uid": uid,
                "_source_maker": maker,
                "maker": effective_maker,
                "name": it["title"],
                "cat": "未分類",
                "date": resolved_date,
                "type": infer_type(it["title"]),
                "summary": f"{it['title']}（自動検知・簡易概要。詳細は公式発表をご確認ください）",
                "specs": {"情報源": "自動検知（詳細未確認）"},
                "source": it["url"],
                "market": "国内",
                "auto": True,
            })

        state[maker] = list(seen | newly_seen_uids)
        extra = f" / {RECENCY_CUTOFF_DAYS}日超えのため対象外 {too_old_count}件" if too_old_count else ""
        print(f"  -> {len(items)}件取得 / 新規 {maker_new_count}件{extra}")

    if new_rows:
        write_header = not os.path.exists(QUEUE_CSV_PATH)
        with open(QUEUE_CSV_PATH, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f, fieldnames=["checked_at", "maker", "type", "published", "title", "url", "relevant"]
            )
            if write_header:
                writer.writeheader()
            writer.writerows(new_rows)
        print(f"\n{len(new_rows)}件の新着を review_queue.csv に追記しました。")
    else:
        print("\n新着はありませんでした。")

    if new_entries_for_portal:
        # ログとして雛形も残しておく（監査・後からの見直し用）
        save_json(NEW_ENTRIES_JSON_PATH, new_entries_for_portal)

        # cars.json に直接追記して自動公開する
        cars = load_json(CARS_JSON_PATH, [])
        existing_ids = [c.get("id", 0) for c in cars]
        next_id = (max(existing_ids) + 1) if existing_ids else 1
        existing_sources = {c.get("source") for c in cars if c.get("source")}

        added = 0
        added_per_maker = {}
        skipped_over_cap = {}
        for entry in new_entries_for_portal:
            if entry["source"] in existing_sources:
                continue  # 念のための二重掲載防止

            display_maker = entry["maker"]
            source_maker = entry["_source_maker"]
            count_so_far = added_per_maker.get(display_maker, 0)
            if count_so_far >= MAX_NEW_PER_MAKER_PER_RUN:
                # 想定外に大量の「新着」が出た場合（アーカイブ全体を含むフィード等）の暴走防止。
                # 上限を超えた分はcars.jsonに追加せず、review_queue.csvの記録のみに留める。
                skipped_over_cap[display_maker] = skipped_over_cap.get(display_maker, 0) + 1
                continue

            entry_clean = {k: v for k, v in entry.items() if k not in ("_uid", "_source_maker")}
            entry_with_id = {"id": next_id, **entry_clean}
            cars.append(entry_with_id)
            existing_sources.add(entry["source"])
            next_id += 1
            added += 1
            added_per_maker[display_maker] = count_so_far + 1

            # 実際にcars.jsonへ追加できた記事のみ、このタイミングでseen扱いにする（取得元フィードのキーで記録）。
            # 上限超過でスキップした記事はseenに入れず、次回実行時に再度候補になるようにする。
            state[source_maker] = list(set(state.get(source_maker, [])) | {entry["_uid"]})

        if added:
            save_json(CARS_JSON_PATH, cars)
            print(f"{added}件を cars.json に自動追記しました（自動生成の簡易概要付き）。")
            print("→ 内容を精査したい場合は、cars.json を直接編集してください。")

        for maker, skipped_count in skipped_over_cap.items():
            print(f"[WARNING] {maker}: 1回の実行あたりの上限（{MAX_NEW_PER_MAKER_PER_RUN}件）を超えたため、{skipped_count}件は自動追加しませんでした。")
            print(f"  -> sources.json の該当ソースが想定外に大量の記事を含んでいる可能性があります。手動で確認してください。")

    save_json(STATE_PATH, state)


if __name__ == "__main__":
    main()
