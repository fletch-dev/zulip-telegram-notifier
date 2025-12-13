import asyncio
import os
import aiohttp
from aiogram import Bot, Dispatcher, Router, types
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from dotenv import load_dotenv
from urllib.parse import quote
import re
from datetime import datetime, time

load_dotenv()

ZULIP_IGNORE_OWN_MESSAGES = os.getenv("ZULIP_IGNORE_OWN_MESSAGES", "true").lower() in ("1", "true", "yes")
ZULIP_EMAIL = os.getenv("ZULIP_EMAIL")
ZULIP_API_KEY = os.getenv("ZULIP_API_KEY")
ZULIP_SITE = os.getenv("ZULIP_SITE").rstrip("/")  # без завершающего слеша
ZULIP_MUTED_STREAMS_POLLING_INTERVAL_SEC = int(os.getenv("ZULIP_MUTED_STREAMS_POLLING_INTERVAL_SEC"))
ZULIP_RATE_LIMIT_DELAY = int(os.getenv("ZULIP_RATE_LIMIT_DELAY"))
ZULIP_RATE_LIMIT_MAX_DELAY = int(os.getenv("ZULIP_RATE_LIMIT_MAX_DELAY"))
TELEGRAM_FORCE_SILENT=os.getenv("TELEGRAM_FORCE_SILENT", "true").lower() in ("1", "true", "yes")
TELEGRAM_FORCE_ALARM=os.getenv("TELEGRAM_FORCE_ALARM", "true").lower() in ("1", "true", "yes")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID"))

# Глобальные mute-настройки
muted_stream_ids = set()
muted_topics = set()   # zulip не передает информации о заглушенных топиках, позже можно будет заглушить топик в настройках бота
MY_USER_ID = None
ZULIP_RATE_LIMITED = False

def parse_time(value: str) -> time:
    hour, minute = map(int, value.split(":"))
    return time(hour, minute)

TELEGRAM_SILENT_FROM = parse_time(os.getenv("TELEGRAM_SILENT_FROM", "22:00"))
TELEGRAM_SILENT_TO   = parse_time(os.getenv("TELEGRAM_SILENT_TO", "08:00"))


router = Router()

PARAMS = [
    "ZULIP_EMAIL",
    "ZULIP_SITE",
    "ZULIP_IGNORE_OWN_MESSAGES",
    "ZULIP_MUTED_STREAMS_POLLING_INTERVAL_SEC",
    "ZULIP_RATE_LIMIT_DELAY",
    "ZULIP_RATE_LIMIT_MAX_DELAY",
    "TELEGRAM_SILENT_FROM",
    "TELEGRAM_SILENT_TO",
    "TELEGRAM_FORCE_SILENT",
    "TELEGRAM_FORCE_ALARM",
]

bot = Bot(
    token=TELEGRAM_BOT_TOKEN,
    default=DefaultBotProperties(
        link_preview_is_disabled=True
    )
)

dp = Dispatcher()
dp.include_router(router)

@router.message(Command("params"))
async def cmd_params(message: types.Message):
    print(message)
    # 🔒 только нужный чат
    if message.chat.id != TELEGRAM_CHAT_ID:
        return

    lines = ["⚙️ <b>Текущие параметры:</b>\n"]

    for key in PARAMS:
        value = os.getenv(key)
        if value is None:
            value = "<i>not set</i>"
        else:
            value = f"<code>{value}</code>"

        lines.append(f"<b>{key}</b>: {value}")

    await message.answer(
        "\n".join(lines),
        parse_mode="HTML",
        disable_notification=True,
    )

async def notify_rate_limited(bot: Bot):
    await bot.send_message(
        TELEGRAM_CHAT_ID,
        "🚨 Zulip API: превышен лимит запросов (HTTP 429).\n"
        "Я временно замедляю запросы и сообщу, когда работа восстановится."
    )

async def notify_rate_limit_recovered(bot: Bot):
    await bot.send_message(
        TELEGRAM_CHAT_ID,
        "✅ Zulip API: работа восстановлена, лимиты больше не превышаются."
    )

async def zulip_api_request(
        session: aiohttp.ClientSession,
        method: str,
        url: str,
        *,
        auth: aiohttp.BasicAuth,
        **kwargs,
):
    global ZULIP_RATE_LIMITED
    delay = ZULIP_RATE_LIMIT_DELAY

    while True:
        async with session.request(
                method,
                url,
                auth=auth,
                **kwargs,
        ) as resp:
            if resp.status != 429:
                if ZULIP_RATE_LIMITED:
                    ZULIP_RATE_LIMITED = False
                    await notify_rate_limit_recovered(bot)

                resp.raise_for_status()
                return await resp.json()

            # 429 — rate limit
            if not ZULIP_RATE_LIMITED:
                ZULIP_RATE_LIMITED = True
                await notify_rate_limited(bot)

            retry_after = resp.headers.get("Retry-After")
            if retry_after is not None:
                sleep_time = float(retry_after)
            else:
                sleep_time = delay
                delay = min(delay * 2, ZULIP_RATE_LIMIT_MAX_DELAY)

            print(f"⚠️ Zulip rate limit hit, sleeping {sleep_time}s")
            await asyncio.sleep(sleep_time)

async def update_muted_streams(session):
    global muted_stream_ids, MY_USER_ID
    # Получить мой user_id
    me = await zulip_api_request(
        session,
        "GET",
        f"{ZULIP_SITE}/api/v1/users/me",
        auth=aiohttp.BasicAuth(ZULIP_EMAIL, ZULIP_API_KEY),
    )
    MY_USER_ID = me["user_id"]

    while True:
        try:
            data = await zulip_api_request(
                session,
                "GET",
                f"{ZULIP_SITE}/api/v1/users/me/subscriptions",
                auth=aiohttp.BasicAuth(ZULIP_EMAIL, ZULIP_API_KEY),
            )
            new_muted = set()
            for sub in data.get("subscriptions", []):
                if sub.get("is_muted"):
                    new_muted.add(sub["stream_id"])
            muted_stream_ids = new_muted
            print("Updated muted streams:", muted_stream_ids)

        except Exception as e:
            print("Error updating muted streams:", e)

        await asyncio.sleep(ZULIP_MUTED_STREAMS_POLLING_INTERVAL_SEC)

def replace_zulip_html_for_telegram(content_html: str) -> str:
    """
    Превращает HTML content из Zulip в HTML для Telegram.
    - <br> → перенос строки
    - <p> → перенос строки
    - <blockquote> → <pre>
    - <a href="#narrow/...">said</a> → кликабельная ссылка
    - <span> → просто текст
    """
    text = content_html

    # <br> → перенос
    text = text.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")

    # <p> → перенос, </p> → перенос
    text = text.replace("<p>", "").replace("</p>", "\n")

    # <blockquote> → <pre>
    text = text.replace("<blockquote>", "<pre>").replace("</blockquote>", "</pre>")

    # Ссылка на 'said'
    def replace_said(match):
        return f"<a href='{ZULIP_SITE}{match.group(1)}'>said</a>"

    text = re.sub(r'<a href="(#narrow/.*?)">said</a>', replace_said, text)

    # span → текст
    text = re.sub(r'<span.*?>(.*?)</span>', r'\1', text)

    # убираем лишние пустые строки
    lines = [line for line in text.splitlines() if line.strip()]
    text = "\n".join(lines)

    return text

def build_zulip_link(msg: dict) -> str:
    """
    Построить ссылку на конкретное сообщение в веб-интерфейсе Zulip.
    Поддерживает stream, pm (single + group).
    """
    msg_id = msg.get("id") or msg.get("message_id")
    if not msg_id:
        return ZULIP_SITE  # fallback

    mtype = msg.get("type")  # 'stream' or 'private'
    # stream
    if mtype == "stream":
        # stream id и имя
        stream_id = msg.get("stream_id") or (msg.get("display_recipient") and msg.get("display_recipient").get("id"))
        # display_recipient часто бывает строкой (stream name) или объектом; для stream обычно строка
        display = msg.get("display_recipient")
        if isinstance(display, str):
            stream_name = display
        elif isinstance(display, dict):
            stream_name = display.get("name") or display.get("stream")
        else:
            stream_name = msg.get("stream_name") or ""
        topic = msg.get("subject") or msg.get("topic") or ""
        # кодируем
        stream_part = f"{stream_id}-{quote(stream_name)}" if stream_id else quote(stream_name)
        topic_part = quote(topic)
        return f"{ZULIP_SITE}/#narrow/stream/{stream_part}/topic/{topic_part}/near/{msg_id}"

    # private messages (pm / huddle)
    if mtype == "private":
        dr = msg.get("display_recipient")
        # display_recipient может быть списком пользователей (group pm) или объект/строкой в редких вариантах
        if isinstance(dr, list):
            # собираем id-список, сортируем по возрастанию (как в Zulip)
            ids = sorted([str(u.get("id")) for u in dr if u.get("id")])
            if len(ids) == 1:
                return f"{ZULIP_SITE}/#narrow/pm-with/{ids[0]}/near/{msg_id}"
            else:
                ids_part = ",".join(ids)
                return f"{ZULIP_SITE}/#narrow/pm/{ids_part}/near/{msg_id}"
        elif isinstance(dr, dict):
            uid = dr.get("id")
            if uid:
                return f"{ZULIP_SITE}/#narrow/pm-with/{uid}/near/{msg_id}"
        # fallback
        return f"{ZULIP_SITE}/#narrow/near/{msg_id}"

    # fallback для неизвестного типа
    return f"{ZULIP_SITE}/#narrow/near/{msg_id}"


def make_plaintext_preview(msg: dict) -> str:
    sender = msg.get("sender_full_name", "Unknown")
    content_html = msg.get("content") or ""
    content = replace_zulip_html_for_telegram(content_html)

    if len(content) > 500:
        content = content[:500] + "…"

    kind = msg.get("type")
    flags = msg.get("flags", [])
    is_mention = "mentioned" in flags or "wildcard_mentioned" in flags

    # Формируем заголовок
    if kind == "stream":
        stream_name = msg.get("display_recipient") if isinstance(msg.get("display_recipient"), str) else "stream"
        topic = msg.get("subject") or msg.get("topic") or ""
        meta = f"<b>[{stream_name}]</b>-<b>[{topic}]</b>"
    else:
        meta = "<b>ЛС</b>"

    mention_text = "⚠️ You were mentioned!" if is_mention else ""
    current_msg_link = build_zulip_link(msg)

    # Собираем текст для Telegram
    parts = [
        f"📩 {meta} — <b>{sender}</b>\n",
    ]
    if mention_text:
        parts.append(mention_text)
    if content.strip():
        parts.append(content)
    parts.append(f"\n<b>Смотреть в Zulip:</b> {current_msg_link}")

    return "\n".join(parts)

def is_silent_hours() -> bool:
    now = datetime.now().time()

    # Silent interval DOES NOT cross midnight
    if TELEGRAM_SILENT_FROM < TELEGRAM_SILENT_TO:
        return TELEGRAM_SILENT_FROM <= now < TELEGRAM_SILENT_TO

    # Silent interval CROSSES midnight (22:00–08:00)
    return now >= TELEGRAM_SILENT_FROM or now < TELEGRAM_SILENT_TO

async def forward_to_telegram(msg):
    # Если нужно игнорировать свои сообщения
    if ZULIP_IGNORE_OWN_MESSAGES and msg.get("sender_email") == ZULIP_EMAIL:
        return  # ничего не делаем

    if not is_zulip_notify(msg):
        return  # не отправляем в Telegram

    text_preview = make_plaintext_preview(msg)
    print(text_preview)

    silent = False
    # если тихий час → отправляем без уведомления
    if (is_silent_hours() or TELEGRAM_FORCE_SILENT) and not TELEGRAM_FORCE_ALARM:
        silent = True

    await bot.send_message(TELEGRAM_CHAT_ID, text = text_preview, parse_mode='html', disable_notification=silent)

def is_zulip_notify(msg):
    """
    Решает, нужно ли отправлять сообщение в Telegram:
    - если стрим/топик заглушены → НЕ отправлять
    - если есть упоминание меня → ОТПРАВЛЯТЬ всегда
    """
    stream_id = msg.get("stream_id")
    topic = msg.get("topic", "").lower()

    # Если есть упоминания меня — всегда отправляем
    flags = set(msg.get("flags", []))
    is_mentioned = "mentioned" in flags
    is_strong_wildcard = "wildcard_mentioned" in flags
    is_weak_wildcard = "stream_wildcard_mentioned" in flags

    if is_mentioned or is_strong_wildcard or is_weak_wildcard:
        return True

    # Если это PM (private) — всегда отправляем
    if msg.get("type") == "private":
        return True

    # Если стрим заглушен
    if stream_id in muted_stream_ids:
        return False

    # Если топик заглушен
    if (stream_id, topic) in muted_topics:
        return False

    return True


# --- Zulip interaction (register/events) ---
async def zulip_register(session):
    data = await zulip_api_request(
        session,
        "POST",
        f"{ZULIP_SITE}/api/v1/register",
        auth=aiohttp.BasicAuth(ZULIP_EMAIL, ZULIP_API_KEY),
        data={"event_types": '["message"]', "apply_markdown": "true"}
    )
    print("Registered:", data)
    return data["queue_id"], data["last_event_id"]


async def zulip_events(session, queue_id, last_event_id, timeout=90):
    url = f"{ZULIP_SITE}/api/v1/events"
    params = {
        "queue_id": queue_id,
        "last_event_id": last_event_id,
        "dont_block": "false",
        "timeout": str(timeout)  # seconds
    }
    async with session.get(
            url,
            params=params,
            auth=aiohttp.BasicAuth(ZULIP_EMAIL, ZULIP_API_KEY),
            timeout=timeout + 10
    ) as resp:
        data = await resp.json()
        return data


async def main():
    async with aiohttp.ClientSession() as session:
        # запускаем таск для обновления мьютов
        asyncio.create_task(update_muted_streams(session))

        # регистрируем очередь Zulip
        queue_id, last_event_id = await zulip_register(session)

        await bot.set_my_commands([
            types.BotCommand(command="params", description="Показать текущие параметры")
        ])
        # запускаем Telegram polling параллельно
        polling_task = asyncio.create_task(dp.start_polling(bot))

        while True:
            try:
                events = await zulip_events(session, queue_id, last_event_id)

                if "events" in events:
                    for event in events["events"]:
                        last_event_id = event["id"]
                        etype = event["type"]
                        print("event:")
                        print(event)
                        if etype == "message":
                            # print(event["message"])
                            await forward_to_telegram(event["message"])

            except Exception as e:
                print("Error:", e)
                await asyncio.sleep(3)  # retry


if __name__ == "__main__":
    asyncio.run(main())
