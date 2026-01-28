import logging
import os
from datetime import datetime, timedelta
from typing import Optional

from telegram import KeyboardButton, ReplyKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

from storage import (
    add_hours,
    add_pickup_log,
    create_client,
    get_client,
    get_user,
    init_db,
    list_pickup_clients,
    list_planning,
    search_clients,
    search_products,
    search_stands,
    sum_hours_by_user,
    update_client_processed,
    update_client_ready_lier,
    update_client_remainder,
    update_user_lang,
    update_user_role,
    upsert_user,
)
from translations import LANGUAGES, t

logging.basicConfig(level=logging.INFO)


ROLE_GUEST = "GUEST"
ROLE_OUTBOUND = "OUTBOUND"
ROLE_WAREHOUSE = "WAREHOUSE"
ROLE_MANAGER = "MANAGER"
ROLE_BOSS = "BOSS"
ROLE_ADMIN = "ADMIN"

OWNER_ID = 1096890291

STATE_AWAIT_NAME = "await_name"
STATE_LANG = "lang"
STATE_CLIENT_ADD = "client_add"
STATE_CLIENT_ADD_REMAINDER = "client_add_remainder"
STATE_CLIENT_ADD_DATE = "client_add_date"
STATE_CLIENT_ADD_CONFIRM = "client_add_confirm"
STATE_CLIENT_SEARCH = "client_search"
STATE_CLIENT_STATUS_LIER = "client_status_lier"
STATE_CLIENT_STATUS_LIER_DATE = "client_status_lier_date"
STATE_CLIENT_STATUS_PROCESSED = "client_status_processed"
STATE_CLIENT_STATUS_PROCESSED_DATE = "client_status_processed_date"
STATE_CLIENT_STATUS_PROCESSED_TIME = "client_status_processed_time"
STATE_PICKUP_QUERY = "pickup_query"
STATE_PICKUP_ID = "pickup_id"
STATE_PICKUP_ACTION = "pickup_action"
STATE_PICKUP_REMAINDER = "pickup_remainder"
STATE_PICKUP_DATE = "pickup_date"
STATE_PLANNING_TYPE = "planning_type"
STATE_PLANNING_PERIOD = "planning_period"
STATE_PLANNING_DATE = "planning_date"
STATE_HOURS_DATE = "hours_date"
STATE_HOURS_START = "hours_start"
STATE_HOURS_END = "hours_end"
STATE_HOURS_BREAK = "hours_break"
STATE_ADMIN_ROLE_USER = "admin_role_user"
STATE_ADMIN_ROLE_SET = "admin_role_set"
STATE_ADMIN_PERF_USER = "admin_perf_user"
STATE_ADMIN_PERF_PERIOD = "admin_perf_period"
STATE_ADMIN_PERF_DATE = "admin_perf_date"
STATE_PRODUCTS_SEARCH = "products_search"
STATE_STANDS_SEARCH = "stands_search"


def main_menu(role: str, lang: str) -> ReplyKeyboardMarkup:
    rows = []
    if role in {ROLE_GUEST, ROLE_OUTBOUND, ROLE_WAREHOUSE, ROLE_MANAGER, ROLE_BOSS, ROLE_ADMIN}:
        rows.append([t(lang, "menu_products"), t(lang, "menu_stands")])
    if role in {ROLE_OUTBOUND, ROLE_WAREHOUSE, ROLE_MANAGER, ROLE_BOSS, ROLE_ADMIN}:
        rows.append([t(lang, "menu_clients")])
    if role in {ROLE_OUTBOUND, ROLE_BOSS, ROLE_ADMIN}:
        rows.append([t(lang, "menu_pickup")])
    if role in {ROLE_OUTBOUND, ROLE_WAREHOUSE, ROLE_MANAGER, ROLE_BOSS, ROLE_ADMIN}:
        rows.append([t(lang, "menu_planning"), t(lang, "menu_hours")])
    if role in {ROLE_BOSS, ROLE_ADMIN}:
        rows.append([t(lang, "menu_admin")])
    rows.append([t(lang, "menu_language")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def clients_menu(role: str, lang: str) -> ReplyKeyboardMarkup:
    rows = []
    if role in {ROLE_OUTBOUND, ROLE_BOSS, ROLE_ADMIN}:
        rows.append([t(lang, "clients_menu_add")])
    rows.append([t(lang, "clients_menu_search")])
    if role in {ROLE_OUTBOUND, ROLE_WAREHOUSE, ROLE_BOSS, ROLE_ADMIN}:
        rows.append([t(lang, "clients_menu_ready_lier")])
    if role in {ROLE_OUTBOUND, ROLE_MANAGER, ROLE_BOSS, ROLE_ADMIN}:
        rows.append([t(lang, "clients_menu_processed")])
    if role in {ROLE_OUTBOUND, ROLE_BOSS, ROLE_ADMIN}:
        rows.append([t(lang, "clients_menu_list_pickup")])
    rows.append([t(lang, "menu_back")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def planning_menu(lang: str) -> ReplyKeyboardMarkup:
    rows = [
        [t(lang, "planning_outbound"), t(lang, "planning_warehouse")],
        [t(lang, "menu_back")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def period_menu(lang: str) -> ReplyKeyboardMarkup:
    rows = [
        [t(lang, "period_today"), t(lang, "period_tomorrow")],
        [t(lang, "period_week"), t(lang, "period_month")],
        [t(lang, "period_date")],
        [t(lang, "menu_back")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def break_menu(lang: str) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[t(lang, "hours_break_yes"), t(lang, "hours_break_no")], [t(lang, "menu_back")]],
        resize_keyboard=True,
    )


def admin_menu(lang: str) -> ReplyKeyboardMarkup:
    rows = [
        [t(lang, "admin_roles")],
        [t(lang, "admin_performance")],
        [t(lang, "menu_back")],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def lang_menu() -> ReplyKeyboardMarkup:
    rows = [[KeyboardButton(code.upper())] for code in LANGUAGES]
    rows.append([KeyboardButton(t("ru", "menu_back"))])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def parse_date(text: str) -> Optional[str]:
    try:
        return datetime.strptime(text, "%d.%m.%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def parse_time(text: str) -> Optional[str]:
    try:
        return datetime.strptime(text, "%H:%M").strftime("%H:%M")
    except ValueError:
        return None


def format_client_row(row) -> str:
    return f"{row['id']} | {row['name']} | {row['city']} | {row['remainder'] or '-'}"


def start_text(lang: str, user_id: int) -> str:
    return "\n".join(
        [
            t(lang, "greeting"),
            t(lang, "welcome_id").format(user_id=user_id),
            t("en", "welcome_staff"),
        ]
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    user = get_user(user_id)
    lang = user["lang"] if user else "ru"
    await update.message.reply_text(start_text(lang, user_id))
    if not user:
        context.user_data["state"] = STATE_AWAIT_NAME
        await update.message.reply_text(t(lang, "ask_name"))
        return
    await update.message.reply_text(
        t(lang, "name_saved").format(name=user["name"]),
        reply_markup=main_menu(user["role"], lang),
    )


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    init_db()
    text = update.message.text.strip()
    user_id = update.effective_user.id
    user = get_user(user_id)
    lang = user["lang"] if user else "ru"
    state = context.user_data.get("state")

    if user_id == OWNER_ID:
        owner_name = update.effective_user.full_name or update.effective_user.first_name or "Owner"
        if not user:
            upsert_user(user_id, owner_name, ROLE_ADMIN, "ru")
        elif user["role"] != ROLE_ADMIN:
            update_user_role(user_id, ROLE_ADMIN)
        user = get_user(user_id)
        lang = user["lang"]

    if user_id == OWNER_ID and text.lower() == "debug":
        await update.message.reply_text(f"user_id={user_id}, role={user['role']}")
        return

    if text == t(lang, "menu_back"):
        context.user_data.clear()
        if user:
            await update.message.reply_text(
                t(lang, "saved"),
                reply_markup=main_menu(user["role"], lang),
            )
        return

    if state == STATE_AWAIT_NAME:
        name = text
        upsert_user(user_id, name, ROLE_GUEST, "ru")
        await update.message.reply_text(
            t("ru", "name_saved").format(name=name),
            reply_markup=main_menu(ROLE_GUEST, "ru"),
        )
        context.user_data.clear()
        return

    if state == STATE_LANG:
        lang_choice = text.lower()
        if lang_choice in LANGUAGES:
            update_user_lang(user_id, lang_choice)
            user = get_user(user_id)
            await update.message.reply_text(
                t(lang_choice, "lang_saved"),
                reply_markup=main_menu(user["role"], lang_choice),
            )
            context.user_data.clear()
            return
        await update.message.reply_text(t(lang, "lang_prompt"), reply_markup=lang_menu())
        return

    if not user:
        await update.message.reply_text(t(lang, "ask_name"))
        context.user_data["state"] = STATE_AWAIT_NAME
        return

    if state == STATE_CLIENT_ADD:
        context.user_data["client_name"] = text
        context.user_data["state"] = "client_city"
        await update.message.reply_text(t(lang, "clients_enter_city"))
        return
    if state == "client_city":
        context.user_data["client_city"] = text
        context.user_data["state"] = "client_product"
        await update.message.reply_text(t(lang, "clients_enter_product"))
        return
    if state == "client_product":
        context.user_data["client_product"] = text
        context.user_data["state"] = "client_remainder_choice"
        await update.message.reply_text(
            t(lang, "clients_remainder_prompt"),
            reply_markup=ReplyKeyboardMarkup(
                [[t(lang, "clients_remainder_none"), t(lang, "clients_remainder_enter")]],
                resize_keyboard=True,
            ),
        )
        return
    if state == "client_remainder_choice":
        if text == t(lang, "clients_remainder_none"):
            context.user_data["client_remainder"] = ""
            context.user_data["state"] = STATE_CLIENT_ADD_DATE
            await update.message.reply_text(t(lang, "clients_enter_date"))
            return
        if text == t(lang, "clients_remainder_enter"):
            context.user_data["state"] = STATE_CLIENT_ADD_REMAINDER
            await update.message.reply_text(t(lang, "clients_enter_remainder"))
            return
        await update.message.reply_text(t(lang, "clients_remainder_prompt"))
        return
    if state == STATE_CLIENT_ADD_REMAINDER:
        context.user_data["client_remainder"] = text
        context.user_data["state"] = STATE_CLIENT_ADD_DATE
        await update.message.reply_text(t(lang, "clients_enter_date"))
        return
    if state == STATE_CLIENT_ADD_DATE:
        parsed = parse_date(text)
        if not parsed:
            await update.message.reply_text(t(lang, "clients_enter_date"))
            return
        context.user_data["client_date"] = parsed
        summary = "\n".join(
            [
                f"{t(lang, 'clients_enter_name')} {context.user_data['client_name']}",
                f"{t(lang, 'clients_enter_city')} {context.user_data['client_city']}",
                f"{t(lang, 'clients_enter_product')} {context.user_data['client_product']}",
                f"{t(lang, 'clients_remainder_prompt')} {context.user_data.get('client_remainder') or '-'}",
                f"{t(lang, 'clients_enter_date')} {text}",
            ]
        )
        context.user_data["state"] = STATE_CLIENT_ADD_CONFIRM
        await update.message.reply_text(
            t(lang, "clients_confirm").format(summary=summary),
            reply_markup=ReplyKeyboardMarkup(
                [[t(lang, "confirm_save"), t(lang, "confirm_edit"), t(lang, "confirm_cancel")]],
                resize_keyboard=True,
            ),
        )
        return
    if state == STATE_CLIENT_ADD_CONFIRM:
        if text == t(lang, "confirm_save"):
            data = {
                "name": context.user_data["client_name"],
                "city": context.user_data["client_city"],
                "missing_product": context.user_data["client_product"],
                "remainder": context.user_data.get("client_remainder"),
                "date": context.user_data["client_date"],
                "responsible": user["name"],
            }
            create_client(data)
            await update.message.reply_text(
                t(lang, "saved"),
                reply_markup=clients_menu(user["role"], lang),
            )
            context.user_data.clear()
            return
        if text == t(lang, "confirm_edit"):
            context.user_data.clear()
            context.user_data["state"] = STATE_CLIENT_ADD
            await update.message.reply_text(t(lang, "clients_enter_name"))
            return
        if text == t(lang, "confirm_cancel"):
            await update.message.reply_text(
                t(lang, "cancelled"),
                reply_markup=clients_menu(user["role"], lang),
            )
            context.user_data.clear()
            return
        await update.message.reply_text(t(lang, "clients_confirm"))
        return

    if state == STATE_CLIENT_SEARCH:
        rows = search_clients(text)
        if not rows:
            await update.message.reply_text(t(lang, "clients_search_none"))
            context.user_data.clear()
            return
        results = "\n".join(format_client_row(row) for row in rows)
        await update.message.reply_text(t(lang, "clients_search_results").format(results=results))
        context.user_data.clear()
        return

    if state == STATE_CLIENT_STATUS_LIER:
        rows = search_clients(text)
        if not rows:
            await update.message.reply_text(t(lang, "clients_search_none"))
            context.user_data.clear()
            return
        results = "\n".join(format_client_row(row) for row in rows)
        context.user_data["client_candidates"] = {row["id"] for row in rows}
        context.user_data["state"] = STATE_CLIENT_STATUS_LIER_DATE
        await update.message.reply_text(t(lang, "clients_search_results").format(results=results))
        return

    if state == STATE_CLIENT_STATUS_LIER_DATE:
        if "client_id" not in context.user_data:
            try:
                client_id = int(text)
            except ValueError:
                await update.message.reply_text(t(lang, "clients_search_results").format(results=""))
                return
            context.user_data["client_id"] = client_id
            await update.message.reply_text(t(lang, "clients_ready_date"))
            return
        parsed = parse_date(text)
        if not parsed:
            await update.message.reply_text(t(lang, "clients_ready_date"))
            return
        update_client_ready_lier(context.user_data["client_id"], parsed, user["name"])
        await update.message.reply_text(
            t(lang, "saved"),
            reply_markup=clients_menu(user["role"], lang),
        )
        context.user_data.clear()
        return

    if state == STATE_CLIENT_STATUS_PROCESSED:
        rows = search_clients(text)
        if not rows:
            await update.message.reply_text(t(lang, "clients_search_none"))
            context.user_data.clear()
            return
        results = "\n".join(format_client_row(row) for row in rows)
        context.user_data["state"] = STATE_CLIENT_STATUS_PROCESSED_DATE
        await update.message.reply_text(t(lang, "clients_search_results").format(results=results))
        return

    if state == STATE_CLIENT_STATUS_PROCESSED_DATE:
        if "client_id" not in context.user_data:
            try:
                context.user_data["client_id"] = int(text)
            except ValueError:
                await update.message.reply_text(t(lang, "clients_search_results").format(results=""))
                return
            await update.message.reply_text(t(lang, "clients_processed_date"))
            return
        parsed = parse_date(text)
        if not parsed:
            await update.message.reply_text(t(lang, "clients_processed_date"))
            return
        context.user_data["processed_date"] = parsed
        context.user_data["state"] = STATE_CLIENT_STATUS_PROCESSED_TIME
        await update.message.reply_text(t(lang, "clients_processed_time"))
        return

    if state == STATE_CLIENT_STATUS_PROCESSED_TIME:
        parsed = parse_time(text)
        if not parsed:
            await update.message.reply_text(t(lang, "clients_processed_time"))
            return
        dt = f"{context.user_data['processed_date']} {parsed}"
        update_client_processed(context.user_data["client_id"], dt, user["name"])
        await update.message.reply_text(
            t(lang, "saved"),
            reply_markup=clients_menu(user["role"], lang),
        )
        context.user_data.clear()
        return

    if state == STATE_PICKUP_QUERY:
        rows = search_clients(text)
        if not rows:
            await update.message.reply_text(t(lang, "clients_search_none"))
            context.user_data.clear()
            return
        results = "\n".join(format_client_row(row) for row in rows)
        context.user_data["state"] = STATE_PICKUP_ID
        await update.message.reply_text(t(lang, "clients_search_results").format(results=results))
        return

    if state == STATE_PICKUP_ID:
        try:
            client_id = int(text)
        except ValueError:
            await update.message.reply_text(t(lang, "pickup_choose"))
            return
        context.user_data["client_id"] = client_id
        context.user_data["state"] = STATE_PICKUP_ACTION
        await update.message.reply_text(
            t(lang, "pickup_choose"),
            reply_markup=ReplyKeyboardMarkup(
                [[t(lang, "pickup_all"), t(lang, "pickup_left")]],
                resize_keyboard=True,
            ),
        )
        return

    if state == STATE_PICKUP_ACTION:
        if text == t(lang, "pickup_all"):
            context.user_data["pickup_action"] = "all"
            context.user_data["pickup_remainder"] = ""
            context.user_data["state"] = STATE_PICKUP_DATE
            await update.message.reply_text(t(lang, "pickup_date"))
            return
        if text == t(lang, "pickup_left"):
            context.user_data["pickup_action"] = "left"
            context.user_data["state"] = STATE_PICKUP_REMAINDER
            await update.message.reply_text(t(lang, "pickup_left_prompt"))
            return
        await update.message.reply_text(t(lang, "pickup_choose"))
        return

    if state == STATE_PICKUP_REMAINDER:
        context.user_data["pickup_remainder"] = text
        context.user_data["state"] = STATE_PICKUP_DATE
        await update.message.reply_text(t(lang, "pickup_date"))
        return

    if state == STATE_PICKUP_DATE:
        parsed = parse_date(text)
        if not parsed:
            await update.message.reply_text(t(lang, "pickup_date"))
            return
        remainder = context.user_data.get("pickup_remainder")
        client_id = context.user_data["client_id"]
        if context.user_data.get("pickup_action") == "all":
            update_client_remainder(client_id, "")
        else:
            update_client_remainder(client_id, remainder)
        add_pickup_log(client_id, parsed, context.user_data.get("pickup_action", ""), remainder, user["name"])
        await update.message.reply_text(
            t(lang, "saved"),
            reply_markup=main_menu(user["role"], lang),
        )
        context.user_data.clear()
        return

    if state == STATE_PLANNING_TYPE:
        if text in {t(lang, "planning_outbound"), t(lang, "planning_warehouse")}:
            context.user_data["planning_type"] = (
                "planning_outbound" if text == t(lang, "planning_outbound") else "planning_warehouse"
            )
            context.user_data["state"] = STATE_PLANNING_PERIOD
            await update.message.reply_text(t(lang, "planning_period_prompt"), reply_markup=period_menu(lang))
            return
        await update.message.reply_text(t(lang, "planning_type_prompt"), reply_markup=planning_menu(lang))
        return

    if state == STATE_PLANNING_PERIOD:
        period = None
        today = datetime.now().date()
        if text == t(lang, "period_today"):
            period = (today, today)
        elif text == t(lang, "period_tomorrow"):
            tomorrow = today + timedelta(days=1)
            period = (tomorrow, tomorrow)
        elif text == t(lang, "period_week"):
            start = today - timedelta(days=today.weekday())
            end = start + timedelta(days=6)
            period = (start, end)
        elif text == t(lang, "period_month"):
            start = today.replace(day=1)
            next_month = (start + timedelta(days=32)).replace(day=1)
            end = next_month - timedelta(days=1)
            period = (start, end)
        elif text == t(lang, "period_date"):
            context.user_data["state"] = STATE_PLANNING_DATE
            await update.message.reply_text(t(lang, "planning_date_prompt"))
            return
        if period:
            table = context.user_data.get("planning_type", "planning_outbound")
            rows = list_planning(table, period[0].isoformat(), period[1].isoformat())
            if not rows:
                await update.message.reply_text(t(lang, "planning_empty"))
            else:
                results = "\n".join(
                    f"{row['date']} | {row.get('client', row.get('shift_names'))} | {row['plan_text']}"
                    for row in rows
                )
                await update.message.reply_text(results)
            context.user_data.clear()
            return
        await update.message.reply_text(t(lang, "planning_period_prompt"), reply_markup=period_menu(lang))
        return

    if state == STATE_PLANNING_DATE:
        parsed = parse_date(text)
        if not parsed:
            await update.message.reply_text(t(lang, "planning_date_prompt"))
            return
        table = context.user_data.get("planning_type", "planning_outbound")
        rows = list_planning(table, parsed, parsed)
        if not rows:
            await update.message.reply_text(t(lang, "planning_empty"))
        else:
            results = "\n".join(
                f"{row['date']} | {row.get('client', row.get('shift_names'))} | {row['plan_text']}"
                for row in rows
            )
            await update.message.reply_text(results)
        context.user_data.clear()
        return

    if state == STATE_HOURS_DATE:
        parsed = parse_date(text)
        if not parsed:
            await update.message.reply_text(t(lang, "hours_date"))
            return
        context.user_data["hours_date"] = parsed
        context.user_data["state"] = STATE_HOURS_START
        await update.message.reply_text(t(lang, "hours_start"))
        return

    if state == STATE_HOURS_START:
        parsed = parse_time(text)
        if not parsed:
            await update.message.reply_text(t(lang, "hours_start"))
            return
        context.user_data["hours_start"] = parsed
        context.user_data["state"] = STATE_HOURS_END
        await update.message.reply_text(t(lang, "hours_end"))
        return

    if state == STATE_HOURS_END:
        parsed = parse_time(text)
        if not parsed:
            await update.message.reply_text(t(lang, "hours_end"))
            return
        context.user_data["hours_end"] = parsed
        context.user_data["state"] = STATE_HOURS_BREAK
        await update.message.reply_text(t(lang, "hours_break"), reply_markup=break_menu(lang))
        return

    if state == STATE_HOURS_BREAK:
        if text not in {t(lang, "hours_break_yes"), t(lang, "hours_break_no")}:
            await update.message.reply_text(t(lang, "hours_break"), reply_markup=break_menu(lang))
            return
        break_minutes = 30 if text == t(lang, "hours_break_yes") else 0
        start_dt = datetime.strptime(context.user_data["hours_start"], "%H:%M")
        end_dt = datetime.strptime(context.user_data["hours_end"], "%H:%M")
        hours = (end_dt - start_dt).total_seconds() / 3600 - (break_minutes / 60)
        add_hours(user_id, context.user_data["hours_date"], context.user_data["hours_start"], context.user_data["hours_end"], break_minutes, hours)
        await update.message.reply_text(
            t(lang, "hours_saved").format(hours=hours),
            reply_markup=main_menu(user["role"], lang),
        )
        context.user_data.clear()
        return

    if state == STATE_ADMIN_ROLE_USER:
        try:
            context.user_data["target_user_id"] = int(text)
        except ValueError:
            await update.message.reply_text(t(lang, "admin_role_user"))
            return
        context.user_data["state"] = STATE_ADMIN_ROLE_SET
        await update.message.reply_text(t(lang, "admin_role_set"))
        return

    if state == STATE_ADMIN_ROLE_SET:
        role = text.strip().upper()
        if role not in {ROLE_GUEST, ROLE_OUTBOUND, ROLE_WAREHOUSE, ROLE_MANAGER, ROLE_BOSS, ROLE_ADMIN}:
            await update.message.reply_text(t(lang, "admin_role_set"))
            return
        update_user_role(context.user_data["target_user_id"], role)
        await update.message.reply_text(t(lang, "admin_role_done"), reply_markup=admin_menu(lang))
        context.user_data.clear()
        return

    if state == STATE_ADMIN_PERF_USER:
        context.user_data["perf_user"] = text.strip()
        context.user_data["state"] = STATE_ADMIN_PERF_PERIOD
        await update.message.reply_text(t(lang, "admin_performance_period"), reply_markup=period_menu(lang))
        return

    if state == STATE_ADMIN_PERF_PERIOD:
        today = datetime.now().date()
        if text == t(lang, "period_today"):
            start = end = today
        elif text == t(lang, "period_tomorrow"):
            start = end = today + timedelta(days=1)
        elif text == t(lang, "period_week"):
            start = today - timedelta(days=today.weekday())
            end = start + timedelta(days=6)
        elif text == t(lang, "period_month"):
            start = today.replace(day=1)
            next_month = (start + timedelta(days=32)).replace(day=1)
            end = next_month - timedelta(days=1)
        elif text == t(lang, "period_date"):
            context.user_data["state"] = STATE_ADMIN_PERF_DATE
            await update.message.reply_text(t(lang, "admin_performance_date"))
            return
        else:
            await update.message.reply_text(t(lang, "admin_performance_period"))
            return
        total = sum_hours_by_user(context.user_data["perf_user"], start.isoformat(), end.isoformat())
        await update.message.reply_text(
            t(lang, "admin_performance_result").format(hours=total),
            reply_markup=admin_menu(lang),
        )
        context.user_data.clear()
        return

    if state == STATE_ADMIN_PERF_DATE:
        parsed = parse_date(text)
        if not parsed:
            await update.message.reply_text(t(lang, "admin_performance_date"))
            return
        total = sum_hours_by_user(context.user_data["perf_user"], parsed, parsed)
        await update.message.reply_text(
            t(lang, "admin_performance_result").format(hours=total),
            reply_markup=admin_menu(lang),
        )
        context.user_data.clear()
        return

    if state == STATE_PRODUCTS_SEARCH:
        rows = search_products(text)
        if not rows:
            await update.message.reply_text(t(lang, "clients_search_none"))
        else:
            results = "\n".join(f"{row['id']} | {row['sort']} | {row['name']} | {row['article']}" for row in rows)
            await update.message.reply_text(t(lang, "search_results").format(results=results))
        context.user_data.clear()
        return

    if state == STATE_STANDS_SEARCH:
        rows = search_stands(text)
        if not rows:
            await update.message.reply_text(t(lang, "clients_search_none"))
        else:
            results = "\n".join(
                f"{row['id']} | {row['stand_name']} | {row['size']} | {row['article']} | {row['tiles_text']}"
                for row in rows
            )
            await update.message.reply_text(t(lang, "search_results").format(results=results))
        context.user_data.clear()
        return

    if text == t(lang, "menu_language"):
        context.user_data["state"] = STATE_LANG
        await update.message.reply_text(t(lang, "lang_prompt"), reply_markup=lang_menu())
        return

    if text == t(lang, "menu_clients"):
        await update.message.reply_text(t(lang, "menu_clients"), reply_markup=clients_menu(user["role"], lang))
        return

    if text == t(lang, "clients_menu_add"):
        context.user_data["state"] = STATE_CLIENT_ADD
        await update.message.reply_text(t(lang, "clients_enter_name"))
        return

    if text == t(lang, "clients_menu_search"):
        context.user_data["state"] = STATE_CLIENT_SEARCH
        await update.message.reply_text(t(lang, "clients_search_prompt"))
        return

    if text == t(lang, "clients_menu_ready_lier"):
        context.user_data["state"] = STATE_CLIENT_STATUS_LIER
        await update.message.reply_text(t(lang, "clients_search_prompt"))
        return

    if text == t(lang, "clients_menu_processed"):
        context.user_data["state"] = STATE_CLIENT_STATUS_PROCESSED
        await update.message.reply_text(t(lang, "clients_search_prompt"))
        return

    if text == t(lang, "clients_menu_list_pickup"):
        rows = list_pickup_clients()
        if not rows:
            await update.message.reply_text(t(lang, "pickup_list_empty"))
        else:
            results = "\n".join(format_client_row(row) for row in rows)
            await update.message.reply_text(results)
        return

    if text == t(lang, "menu_pickup"):
        context.user_data["state"] = STATE_PICKUP_QUERY
        await update.message.reply_text(t(lang, "pickup_query"))
        return

    if text == t(lang, "menu_planning"):
        context.user_data["state"] = STATE_PLANNING_TYPE
        await update.message.reply_text(t(lang, "planning_type_prompt"), reply_markup=planning_menu(lang))
        return

    if text == t(lang, "menu_hours"):
        context.user_data["state"] = STATE_HOURS_DATE
        await update.message.reply_text(t(lang, "hours_date"))
        return

    if text == t(lang, "menu_admin") and user["role"] in {ROLE_BOSS, ROLE_ADMIN}:
        await update.message.reply_text(t(lang, "menu_admin"), reply_markup=admin_menu(lang))
        return

    if text == t(lang, "admin_roles"):
        context.user_data["state"] = STATE_ADMIN_ROLE_USER
        await update.message.reply_text(t(lang, "admin_role_user"))
        return

    if text == t(lang, "admin_performance"):
        context.user_data["state"] = STATE_ADMIN_PERF_USER
        await update.message.reply_text(t(lang, "admin_performance_user"))
        return

    if text == t(lang, "menu_products"):
        context.user_data["state"] = STATE_PRODUCTS_SEARCH
        await update.message.reply_text(t(lang, "products_search"))
        return

    if text == t(lang, "menu_stands"):
        context.user_data["state"] = STATE_STANDS_SEARCH
        await update.message.reply_text(t(lang, "stands_search"))
        return

    await update.message.reply_text(t(lang, "unknown"))


def run() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is required")
    init_db()
    app = Application.builder().token(token).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.run_polling()


if __name__ == "__main__":
    run()
