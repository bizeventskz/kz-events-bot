#!/usr/bin/env python3
"""
Kazakhstan Business Events → Telegram Bot
Запуск: python main.py
Cron:   0 8 * * * /usr/bin/python3 /path/to/main.py
"""

import os
import sqlite3
import hashlib
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")   # токен от @BotFather
CHAT_ID   = os.getenv("CHAT_ID")     # твой chat_id (получи у @userinfobot)
DB_PATH   = "sent_events.db"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ─────────────────────────────────────────────
# БАЗА ДАННЫХ — защита от дублей
# ─────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS sent ("
        "  id TEXT PRIMARY KEY,"
        "  sent_at TEXT"
        ")"
    )
    conn.commit()
    return conn

def already_sent(conn, event_id: str) -> bool:
    return conn.execute("SELECT 1 FROM sent WHERE id=?", (event_id,)).fetchone() is not None

def mark_sent(conn, event_id: str):
    conn.execute("INSERT OR IGNORE INTO sent VALUES (?,?)", (event_id, datetime.now().isoformat()))
    conn.commit()

def make_id(title: str, date: str) -> str:
    return hashlib.md5(f"{title}{date}".encode()).hexdigest()

# ─────────────────────────────────────────────
# ФОРМАТИРОВАНИЕ СООБЩЕНИЯ
# ─────────────────────────────────────────────
def format_message(event: dict) -> str:
    return (
        f"📌 *{event['title']}*\n\n"
        f"📅 Дата: {event['date']}\n"
        f"📍 Город / площадка: {event['location']}\n"
        f"📝 {event['description']}\n"
        f"🔗 {event['url']}"
    )

# ─────────────────────────────────────────────
# ОТПРАВКА В TELEGRAM
# ─────────────────────────────────────────────
def send_telegram(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resp = requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }, timeout=15)
    resp.raise_for_status()
    log.info("Отправлено в Telegram ✓")

# ─────────────────────────────────────────────
# ПАРСЕРЫ
# ─────────────────────────────────────────────

def parse_atameken() -> list[dict]:
    """Палата предпринимателей РК — atameken.kz"""
    events = []
    try:
        url = "https://atameken.kz/ru/events"
        r = requests.get(url, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        # Ищем карточки мероприятий (структура может меняться — проверяй)
        for card in soup.select(".event-item, .news-item, article.item")[:10]:
            title_el = card.select_one("h2, h3, .title, .event-title")
            date_el  = card.select_one(".date, time, .event-date")
            link_el  = card.select_one("a[href]")
            desc_el  = card.select_one("p, .description, .excerpt")

            if not title_el or not link_el:
                continue

            href = link_el["href"]
            if not href.startswith("http"):
                href = "https://atameken.kz" + href

            events.append({
                "title":       title_el.get_text(strip=True),
                "date":        date_el.get_text(strip=True) if date_el else "Уточняется",
                "location":    "Казахстан",
                "description": desc_el.get_text(strip=True)[:200] if desc_el else "—",
                "url":         href,
            })
    except Exception as e:
        log.warning(f"atameken.kz: {e}")
    return events


def parse_eventbrite_kz() -> list[dict]:
    """Eventbrite — деловые события в Казахстане"""
    events = []
    try:
        # Используем публичный поиск Eventbrite без API
        url = (
            "https://www.eventbrite.com/d/kazakhstan/business/"
            "?page=1&sort=date"
        )
        r = requests.get(url, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        for card in soup.select("[data-testid='event-card'], .search-event-card")[:10]:
            title_el = card.select_one("h2, h3, [data-testid='event-card-title']")
            date_el  = card.select_one("time, [data-testid='event-card-date']")
            loc_el   = card.select_one("[data-testid='event-card-location'], .location")
            link_el  = card.select_one("a[href*='/e/']")
            desc_el  = card.select_one("[data-testid='event-card-description'], p")

            if not title_el or not link_el:
                continue

            events.append({
                "title":       title_el.get_text(strip=True),
                "date":        date_el.get_text(strip=True) if date_el else "Уточняется",
                "location":    loc_el.get_text(strip=True) if loc_el else "Казахстан",
                "description": desc_el.get_text(strip=True)[:200] if desc_el else "—",
                "url":         link_el["href"].split("?")[0],
            })
    except Exception as e:
        log.warning(f"eventbrite: {e}")
    return events


def parse_forbes_kz() -> list[dict]:
    """Forbes Kazakhstan — события и конференции"""
    events = []
    try:
        url = "https://forbes.kz/events/"
        r = requests.get(url, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        for card in soup.select(".post-card, .event-card, article")[:10]:
            title_el = card.select_one("h2, h3, .entry-title")
            date_el  = card.select_one(".date, time, .post-date")
            link_el  = card.select_one("a[href]")
            desc_el  = card.select_one("p, .excerpt")

            if not title_el or not link_el:
                continue

            href = link_el["href"]
            if not href.startswith("http"):
                href = "https://forbes.kz" + href

            events.append({
                "title":       title_el.get_text(strip=True),
                "date":        date_el.get_text(strip=True) if date_el else "Уточняется",
                "location":    "Казахстан",
                "description": desc_el.get_text(strip=True)[:200] if desc_el else "—",
                "url":         href,
            })
    except Exception as e:
        log.warning(f"forbes.kz: {e}")
    return events


def parse_kazchamber() -> list[dict]:
    """Торгово-промышленная палата РК"""
    events = []
    try:
        url = "https://kazchamber.kz/ru/news/events/"
        r = requests.get(url, headers=HEADERS, timeout=20)
        soup = BeautifulSoup(r.text, "html.parser")

        for card in soup.select(".news-item, .events-item, article")[:10]:
            title_el = card.select_one("h2, h3, .title")
            date_el  = card.select_one(".date, time")
            link_el  = card.select_one("a[href]")
            desc_el  = card.select_one("p, .text")

            if not title_el or not link_el:
                continue

            href = link_el["href"]
            if not href.startswith("http"):
                href = "https://kazchamber.kz" + href

            events.append({
                "title":       title_el.get_text(strip=True),
                "date":        date_el.get_text(strip=True) if date_el else "Уточняется",
                "location":    "Казахстан",
                "description": desc_el.get_text(strip=True)[:200] if desc_el else "—",
                "url":         href,
            })
    except Exception as e:
        log.warning(f"kazchamber.kz: {e}")
    return events


# ─────────────────────────────────────────────
# ГЛАВНАЯ ЛОГИКА
# ─────────────────────────────────────────────
def run():
    log.info("=== Запуск сбора мероприятий ===")

    if not BOT_TOKEN or not CHAT_ID:
        log.error("Не заданы BOT_TOKEN или CHAT_ID в .env")
        return

    conn = init_db()

    # Собираем из всех источников
    all_events = (
        parse_atameken()
        + parse_eventbrite_kz()
        + parse_forbes_kz()
        + parse_kazchamber()
    )

    log.info(f"Найдено мероприятий: {len(all_events)}")

    new_count = 0
    for event in all_events:
        eid = make_id(event["title"], event["date"])
        if already_sent(conn, eid):
            log.info(f"Пропущено (дубль): {event['title'][:50]}")
            continue

        try:
            msg = format_message(event)
            send_telegram(msg)
            mark_sent(conn, eid)
            new_count += 1
        except Exception as e:
            log.error(f"Ошибка отправки: {e}")

    if new_count == 0:
        log.info("Новых мероприятий нет")
    else:
        log.info(f"Отправлено новых: {new_count}")

    conn.close()

if __name__ == "__main__":
    run()
