import sqlite3
from pathlib import Path
from typing import Iterable, Optional

DB_PATH = Path(__file__).resolve().parent / "data" / "bot.db"


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                role TEXT NOT NULL,
                lang TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                city TEXT NOT NULL,
                missing_product TEXT NOT NULL,
                remainder TEXT,
                date TEXT NOT NULL,
                responsible TEXT NOT NULL,
                ready_lier_date TEXT,
                ready_lier_by TEXT,
                processed_datetime TEXT,
                processed_by TEXT
            );
            CREATE TABLE IF NOT EXISTS pickup_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                action TEXT NOT NULL,
                remainder TEXT,
                responsible TEXT NOT NULL,
                FOREIGN KEY (client_id) REFERENCES clients(id)
            );
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sort TEXT NOT NULL,
                name TEXT NOT NULL,
                article TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS stands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stand_name TEXT NOT NULL,
                size TEXT NOT NULL,
                article TEXT NOT NULL,
                tiles_text TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS planning_outbound (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                client TEXT NOT NULL,
                city_index TEXT NOT NULL,
                plan_text TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS planning_warehouse (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                shift_names TEXT NOT NULL,
                plan_text TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS hours (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                break_minutes INTEGER NOT NULL,
                hours REAL NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            """
        )


def get_user(user_id: int) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM users WHERE user_id = ?",
            (user_id,),
        ).fetchone()


def upsert_user(user_id: int, name: str, role: str, lang: str) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO users (user_id, name, role, lang)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                name = excluded.name,
                role = excluded.role,
                lang = excluded.lang
            """,
            (user_id, name, role, lang),
        )


def update_user_role(user_id: int, role: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET role = ? WHERE user_id = ?",
            (role, user_id),
        )


def update_user_lang(user_id: int, lang: str) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE users SET lang = ? WHERE user_id = ?",
            (lang, user_id),
        )


def create_client(data: dict) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO clients (name, city, missing_product, remainder, date, responsible)
            VALUES (:name, :city, :missing_product, :remainder, :date, :responsible)
            """,
            data,
        )
        return cur.lastrowid


def search_clients(query: str) -> Iterable[sqlite3.Row]:
    like = f"%{query.lower()}%"
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT * FROM clients
            WHERE lower(name) LIKE ? OR lower(city) LIKE ?
            ORDER BY id DESC
            """,
            (like, like),
        ).fetchall()


def get_client(client_id: int) -> Optional[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM clients WHERE id = ?",
            (client_id,),
        ).fetchone()


def update_client_ready_lier(client_id: int, date: str, responsible: str) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE clients
            SET ready_lier_date = ?, ready_lier_by = ?
            WHERE id = ?
            """,
            (date, responsible, client_id),
        )


def update_client_processed(client_id: int, dt: str, responsible: str) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE clients
            SET processed_datetime = ?, processed_by = ?
            WHERE id = ?
            """,
            (dt, responsible, client_id),
        )


def update_client_remainder(client_id: int, remainder: Optional[str]) -> None:
    with get_conn() as conn:
        conn.execute(
            "UPDATE clients SET remainder = ? WHERE id = ?",
            (remainder, client_id),
        )


def add_pickup_log(client_id: int, date: str, action: str, remainder: Optional[str], responsible: str) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO pickup_logs (client_id, date, action, remainder, responsible)
            VALUES (?, ?, ?, ?, ?)
            """,
            (client_id, date, action, remainder, responsible),
        )


def list_pickup_clients() -> Iterable[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT * FROM clients
            WHERE remainder IS NOT NULL AND trim(remainder) != ''
            ORDER BY id DESC
            """
        ).fetchall()


def search_products(query: str) -> Iterable[sqlite3.Row]:
    like = f"%{query.lower()}%"
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT * FROM products
            WHERE lower(sort) LIKE ? OR lower(name) LIKE ? OR lower(article) LIKE ?
            ORDER BY id DESC
            """,
            (like, like, like),
        ).fetchall()


def search_stands(query: str) -> Iterable[sqlite3.Row]:
    like = f"%{query.lower()}%"
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT * FROM stands
            WHERE lower(stand_name) LIKE ? OR lower(size) LIKE ? OR lower(article) LIKE ? OR lower(tiles_text) LIKE ?
            ORDER BY id DESC
            """,
            (like, like, like, like),
        ).fetchall()


def list_planning(table: str, start: str, end: str) -> Iterable[sqlite3.Row]:
    if table not in {"planning_outbound", "planning_warehouse"}:
        raise ValueError("Invalid planning table")
    with get_conn() as conn:
        return conn.execute(
            f"""
            SELECT * FROM {table}
            WHERE date BETWEEN ? AND ?
            ORDER BY date ASC
            """,
            (start, end),
        ).fetchall()


def add_hours(user_id: int, date: str, start: str, end: str, break_minutes: int, hours: float) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO hours (user_id, date, start_time, end_time, break_minutes, hours)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, date, start, end, break_minutes, hours),
        )


def sum_hours_by_user(name: str, start: str, end: str) -> float:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT SUM(hours) as total
            FROM hours
            JOIN users ON users.user_id = hours.user_id
            WHERE users.name = ? AND date BETWEEN ? AND ?
            """,
            (name, start, end),
        ).fetchone()
    return row["total"] or 0.0
