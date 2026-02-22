import os
import json
import re
import sqlite3
import textwrap
from datetime import date, datetime
from typing import Optional

import requests
from difflib import SequenceMatcher

try:
    import gspread
    from google.oauth2 import service_account
    from gspread.exceptions import APIError
    SHEETS_AVAILABLE = True
except ImportError:
    gspread = None
    service_account = None
    APIError = Exception
    SHEETS_AVAILABLE = False
import streamlit as st

BDL_BASE = "https://api.balldontlie.io/pga/v1"

USERS = ["Carl", "Jacob", "Vossy", "AJ", "Jordan", "Cade"]

SEED_GOLFERS = [
    ("Si Woo Kim", 5, 496),
    ("Maverick McNealy", 17, 165),
    ("Cameron Young", 67, 37),
    ("Sahith Theegala", 12, 225),
    ("Scottie Scheffler", 2, 625),
    ("Justin Rose", 4, 500),
    ("Hideki Matsuyama", 6, 413),
    ("Jake Knapp", 9, 258),
    ("Matt McCarty", 10, 235),
    ("Patrick Rodgers", 11, 234),
    ("Jacob Bridgeman", 13, 222),
    ("Jason Day", 14, 200),
    ("Andrew Putnam", 15, 184),
    ("Michael Thorbjornsen", 16, 171),
    ("Nicolai Hojgaard", 18, 162),
    ("Sam Stevens", 19, 158),
    ("Daniel Berger", 20, 143),
    ("Haotong Li", 21, 139),
    ("Robert MacIntyre", 22, 138),
    ("Stephan Jaeger", 23, 132),
    ("Akshay Bhatia", 24, 125),
    ("Harry Hall", 25, 122),
    ("Russell Henley", 26, 118),
    ("Kevin Roy", 27, 112),
    ("S.H. Kim", 28, 108),
    ("Nick Taylor", 29, 106),
    ("Ben Griffin", 30, 105),
    ("Joel Dahmen", 31, 101),
    ("Davis Riley", 32, 91),
    ("Tom Hoge", 33, 90),
    ("Harris English", 34, 89),
    ("Rickie Fowler", 35, 88),
    ("Lee Hodges", 36, 86),
    ("Taylor Pendrith", 37, 86),
    ("Andrew Novak", 38, 85),
    ("Seamus Power", 39, 84),
    ("Matt Fitzpatrick", 40, 84),
    ("Keith Mitchell", 41, 83),
    ("Jordan Smith", 42, 81),
    ("John Parry", 43, 78),
    ("Wyndham Clark", 44, 77),
    ("Austin Smotherman", 45, 75),
    ("Zecheng Dou", 46, 73),
    ("Adam Scott", 47, 71),
    ("Zach Bauchou", 48, 71),
    ("Viktor Hovland", 49, 70),
    ("Rasmus Hojgaard", 50, 67),
]

SEED_TOURNAMENTS = [
    ("WM Phoenix Open", "2026-02-05", "2026-02-08", 0, 0, 2026, 9_600_000),
    ("AT&T Pebble Beach Pro-Am", "2026-02-12", "2026-02-15", 0, 1, 2026, 20_000_000),
    ("The Genesis Invitational", "2026-02-19", "2026-02-22", 0, 1, 2026, 20_000_000),
    ("Cognizant Classic", "2026-02-26", "2026-03-01", 0, 0, 2026, 9_600_000),
    ("Arnold Palmer Invitational pres. by Mastercard", "2026-03-05", "2026-03-08", 0, 1, 2026, 20_000_000),
    ("THE PLAYERS Championship", "2026-03-12", "2026-03-15", 0, 0, 2026, 25_000_000),
    ("Valspar Championship", "2026-03-19", "2026-03-22", 0, 0, 2026, 9_100_000),
    ("Texas Children's Houston Open", "2026-03-26", "2026-03-29", 0, 0, 2026, 9_900_000),
    ("Valero Texas Open", "2026-04-02", "2026-04-05", 0, 0, 2026, 9_800_000),
    ("Masters Tournament", "2026-04-09", "2026-04-12", 1, 0, 2026, None),
    ("RBC Heritage", "2026-04-16", "2026-04-19", 0, 1, 2026, 20_000_000),
    ("Zurich Classic of New Orleans", "2026-04-23", "2026-04-26", 0, 0, 2026, 9_500_000),
    ("Cadillac Championship", "2026-04-30", "2026-05-03", 0, 1, 2026, None),
    ("Truist Championship", "2026-05-07", "2026-05-10", 0, 1, 2026, 20_000_000),
    ("PGA Championship", "2026-05-14", "2026-05-17", 1, 0, 2026, None),
    ("THE CJ CUP Byron Nelson", "2026-05-21", "2026-05-24", 0, 0, 2026, 10_300_000),
    ("Charles Schwab Challenge", "2026-05-28", "2026-05-31", 0, 0, 2026, 9_900_000),
    ("the Memorial Tournament pres. by Workday", "2026-06-04", "2026-06-07", 0, 1, 2026, 20_000_000),
    ("RBC Canadian Open", "2026-06-11", "2026-06-14", 0, 0, 2026, 9_800_000),
    ("U.S. Open", "2026-06-18", "2026-06-21", 1, 0, 2026, None),
    ("Travelers Championship", "2026-06-25", "2026-06-28", 0, 1, 2026, 20_000_000),
    ("John Deere Classic", "2026-07-02", "2026-07-05", 0, 0, 2026, 8_800_000),
    ("Genesis Scottish Open", "2026-07-09", "2026-07-12", 0, 0, 2026, 9_000_000),
    ("The Open", "2026-07-16", "2026-07-19", 1, 0, 2026, None),
    ("3M Open", "2026-07-23", "2026-07-26", 0, 0, 2026, 8_800_000),
    ("Rocket Classic", "2026-07-30", "2026-08-02", 0, 0, 2026, 10_000_000),
    ("Wyndham Championship", "2026-08-06", "2026-08-09", 0, 0, 2026, 8_500_000),
    ("FedEx St. Jude Championship", "2026-08-13", "2026-08-16", 0, 0, 2026, 20_000_000),
    ("BMW Championship", "2026-08-20", "2026-08-23", 0, 0, 2026, 20_000_000),
]

INITIAL_PICKS = [
    ("Jacob", "Si Woo Kim", 439_680, 3),
    ("Carl", "Maverick McNealy", 188_000, None),
    ("Cade", "Maverick McNealy", 188_000, None),
    ("AJ", "Cameron Young", 34_080, None),
    ("Jordan", "Sahith Theegala", 122_720, None),
    ("Vossy", "Cameron Young", 34_080, None),
]


def get_db_path() -> str:
    return os.getenv("GOLF_DB_PATH", os.path.join("data", "golf.db"))

def get_picks_backup_path() -> str:
    return os.getenv("GOLF_PICKS_BACKUP", os.path.join("data", "picks_backup.json"))

def get_sheets_id() -> str:
    return os.getenv("GOOGLE_SHEETS_ID", "").strip()

def get_sheets_scopes() -> list:
    return ["https://www.googleapis.com/auth/spreadsheets"]

def get_sheets_client():
    if not SHEETS_AVAILABLE:
        return None
    sheet_id = get_sheets_id()
    if not sheet_id:
        return None
    info_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    info_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
    info = None
    if info_json:
        try:
            info = json.loads(info_json)
        except json.JSONDecodeError:
            return None
    elif info_file and os.path.exists(info_file):
        with open(info_file, "r", encoding="utf-8") as handle:
            info = json.load(handle)
    if not info:
        return None
    creds = service_account.Credentials.from_service_account_info(info, scopes=get_sheets_scopes())
    return gspread.authorize(creds)

def get_picks_worksheet():
    client = get_sheets_client()
    sheet_id = get_sheets_id()
    if not client or not sheet_id:
        return None
    try:
        sheet = client.open_by_key(sheet_id)
        try:
            worksheet = sheet.worksheet("picks")
        except gspread.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title="picks", rows=200, cols=4)
            worksheet.update("A1:D1", [["user", "tournament", "golfer", "created_at"]])
        return worksheet
    except APIError:
        return None


def get_results_worksheet():
    client = get_sheets_client()
    sheet_id = get_sheets_id()
    if not client or not sheet_id:
        return None
    try:
        sheet = client.open_by_key(sheet_id)
        try:
            worksheet = sheet.worksheet("results")
        except gspread.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title="results", rows=500, cols=5)
            worksheet.update("A1:E1", [["tournament", "golfer", "purse", "position", "updated_at"]])
        return worksheet
    except APIError:
        return None

def load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def get_conn() -> sqlite3.Connection:
    db_path = get_db_path()
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA foreign_keys=ON;

        CREATE TABLE IF NOT EXISTS users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL UNIQUE,
          double_pick_used INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS golfers (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL UNIQUE,
          fedex_rank INTEGER,
          fedex_points INTEGER,
          active INTEGER NOT NULL DEFAULT 1,
          bdl_id INTEGER
        );

        CREATE TABLE IF NOT EXISTS tournaments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          start_date TEXT NOT NULL,
          end_date TEXT NOT NULL,
          is_major INTEGER NOT NULL DEFAULT 0,
          is_signature INTEGER NOT NULL DEFAULT 0,
          season INTEGER NOT NULL DEFAULT 2026,
          bdl_id INTEGER,
          purse INTEGER,
          rapid_tourn_id TEXT
        );

        CREATE TABLE IF NOT EXISTS picks (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id INTEGER NOT NULL,
          tournament_id INTEGER NOT NULL,
          golfer_id INTEGER NOT NULL,
          created_at TEXT NOT NULL,
          FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
          FOREIGN KEY(tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE,
          FOREIGN KEY(golfer_id) REFERENCES golfers(id) ON DELETE CASCADE,
          UNIQUE(user_id, golfer_id)
        );

        CREATE TABLE IF NOT EXISTS results (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          tournament_id INTEGER NOT NULL,
          golfer_id INTEGER NOT NULL,
          purse INTEGER NOT NULL,
          position INTEGER,
          FOREIGN KEY(tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE,
          FOREIGN KEY(golfer_id) REFERENCES golfers(id) ON DELETE CASCADE,
          UNIQUE(tournament_id, golfer_id)
        );
        """
    )

    # Add new columns if database already exists
    columns = [row[1] for row in conn.execute("PRAGMA table_info(tournaments)").fetchall()]
    if "purse" not in columns:
        conn.execute("ALTER TABLE tournaments ADD COLUMN purse INTEGER")
    if "rapid_tourn_id" not in columns:
        conn.execute("ALTER TABLE tournaments ADD COLUMN rapid_tourn_id TEXT")
    user_columns = [row[1] for row in conn.execute("PRAGMA table_info(users)").fetchall()]
    if "double_pick_used" not in user_columns:
        conn.execute("ALTER TABLE users ADD COLUMN double_pick_used INTEGER NOT NULL DEFAULT 0")
    conn.commit()

    # Migrate picks table if it still has the UNIQUE(user_id, tournament_id) constraint.
    index_list = conn.execute("PRAGMA index_list(picks)").fetchall()
    has_unique_user_tourn = False
    for idx in index_list:
        if not idx[2]:
            continue
        index_name = idx[1]
        cols = [row[2] for row in conn.execute(f"PRAGMA index_info({index_name})").fetchall()]
        if cols == ["user_id", "tournament_id"]:
            has_unique_user_tourn = True
            break

    if has_unique_user_tourn:
        conn.executescript(
            """
            BEGIN TRANSACTION;
            CREATE TABLE picks_new (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
              tournament_id INTEGER NOT NULL,
              golfer_id INTEGER NOT NULL,
              created_at TEXT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
              FOREIGN KEY(tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE,
              FOREIGN KEY(golfer_id) REFERENCES golfers(id) ON DELETE CASCADE,
              UNIQUE(user_id, golfer_id)
            );
            INSERT INTO picks_new (id, user_id, tournament_id, golfer_id, created_at)
            SELECT id, user_id, tournament_id, golfer_id, created_at FROM picks;
            DROP TABLE picks;
            ALTER TABLE picks_new RENAME TO picks;
            COMMIT;
            """
        )


def seed_if_needed(conn: sqlite3.Connection) -> None:
    if conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
        conn.executemany("INSERT INTO users (name) VALUES (?)", [(u,) for u in USERS])

    # Remove tournaments we don't want in the schedule
    conn.execute("DELETE FROM tournaments WHERE name = ?", ("Puerto Rico Open",))
    conn.execute("DELETE FROM tournaments WHERE name = ?", ("ONEflight Myrtle Beach Classic",))
    conn.execute("DELETE FROM tournaments WHERE name = ?", ("ISCO Championship",))
    conn.execute("DELETE FROM tournaments WHERE name = ?", ("Corales Puntacana Championship",))

    if conn.execute("SELECT COUNT(*) FROM golfers").fetchone()[0] == 0:
        conn.executemany(
            "INSERT INTO golfers (name, fedex_rank, fedex_points) VALUES (?, ?, ?)",
            SEED_GOLFERS,
        )

    existing_tournaments = {
        row["name"]: row
        for row in conn.execute("SELECT id, name FROM tournaments").fetchall()
    }
    for tournament in SEED_TOURNAMENTS:
        name, start, end, is_major, is_signature, season, purse = tournament
        if name in existing_tournaments:
            conn.execute(
                "UPDATE tournaments SET start_date = ?, end_date = ?, is_major = ?, is_signature = ?, season = ?, purse = ? WHERE id = ?",
                (start, end, is_major, is_signature, season, purse, existing_tournaments[name]["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO tournaments (name, start_date, end_date, is_major, is_signature, season, purse) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (name, start, end, is_major, is_signature, season, purse),
            )

    if conn.execute("SELECT COUNT(*) FROM picks").fetchone()[0] == 0:
        tournament_id = conn.execute(
            "SELECT id FROM tournaments WHERE name = ?", ("WM Phoenix Open",)
        ).fetchone()[0]
        for user_name, golfer_name, purse, position in INITIAL_PICKS:
            user_id = conn.execute("SELECT id FROM users WHERE name = ?", (user_name,)).fetchone()[0]
            golfer_id = conn.execute("SELECT id FROM golfers WHERE name = ?", (golfer_name,)).fetchone()[0]
            conn.execute(
                "INSERT OR IGNORE INTO picks (user_id, tournament_id, golfer_id, created_at) VALUES (?, ?, ?, ?)",
                (user_id, tournament_id, golfer_id, datetime.utcnow().isoformat()),
            )
            conn.execute(
                "INSERT OR IGNORE INTO results (tournament_id, golfer_id, purse, position) VALUES (?, ?, ?, ?)",
                (tournament_id, golfer_id, purse, position),
            )

    conn.commit()


def normalize_name(value: str) -> str:
    return (
        value.lower()
        .replace("presented by", "")
        .replace("sponsored by", "")
        .replace("the ", "")
        .replace("tournament", "")
        .replace("championship", "")
        .replace("invitational", "")
        .replace("open", "")
        .replace("classic", "")
        .replace("  ", " ")
        .strip()
    )


def save_picks_snapshot(conn: sqlite3.Connection) -> None:
    picks = conn.execute(
        """
        SELECT users.name as user,
               tournaments.name as tournament,
               golfers.name as golfer,
               picks.created_at as created_at
        FROM picks
        JOIN users ON users.id = picks.user_id
        JOIN tournaments ON tournaments.id = picks.tournament_id
        JOIN golfers ON golfers.id = picks.golfer_id
        ORDER BY picks.created_at
        """
    ).fetchall()
    payload = [
        {
            "user": row["user"],
            "tournament": row["tournament"],
            "golfer": row["golfer"],
            "created_at": row["created_at"],
        }
        for row in picks
    ]
    path = get_picks_backup_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)

def sync_picks_from_sheet(conn: sqlite3.Connection) -> bool:
    worksheet = get_picks_worksheet()
    if not worksheet:
        return False
    try:
        rows = worksheet.get_all_records()
    except APIError:
        return False
    if not rows:
        return False
    conn.execute("DELETE FROM picks")
    restored = 0
    for row in rows:
        user = conn.execute("SELECT id FROM users WHERE name = ?", (row.get("user"),)).fetchone()
        tournament = conn.execute("SELECT id FROM tournaments WHERE name = ?", (row.get("tournament"),)).fetchone()
        golfer = conn.execute("SELECT id FROM golfers WHERE name = ?", (row.get("golfer"),)).fetchone()
        if not user or not tournament or not golfer:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO picks (user_id, tournament_id, golfer_id, created_at) VALUES (?, ?, ?, ?)",
            (user["id"], tournament["id"], golfer["id"], row.get("created_at") or datetime.utcnow().isoformat()),
        )
        restored += 1
    conn.commit()
    return restored > 0


def sync_results_from_sheet(conn: sqlite3.Connection) -> bool:
    worksheet = get_results_worksheet()
    if not worksheet:
        return False
    try:
        rows = worksheet.get_all_records()
    except APIError:
        return False
    if not rows:
        return False
    conn.execute("DELETE FROM results")
    restored = 0
    for row in rows:
        tournament = conn.execute("SELECT id FROM tournaments WHERE name = ?", (row.get("tournament"),)).fetchone()
        golfer = conn.execute("SELECT id FROM golfers WHERE name = ?", (row.get("golfer"),)).fetchone()
        if not tournament or not golfer:
            continue
        purse = row.get("purse") or 0
        position = row.get("position")
        conn.execute(
            "INSERT OR IGNORE INTO results (tournament_id, golfer_id, purse, position) VALUES (?, ?, ?, ?)",
            (tournament["id"], golfer["id"], int(purse), int(position) if position else None),
        )
        restored += 1
    conn.commit()
    return restored > 0


def sync_results_to_sheet(conn: sqlite3.Connection) -> None:
    worksheet = get_results_worksheet()
    if not worksheet:
        return
    rows = conn.execute(
        """
        SELECT tournaments.name as tournament,
               golfers.name as golfer,
               results.purse as purse,
               results.position as position
        FROM results
        JOIN tournaments ON tournaments.id = results.tournament_id
        JOIN golfers ON golfers.id = results.golfer_id
        ORDER BY tournaments.start_date, golfers.name
        """
    ).fetchall()
    values = [["tournament", "golfer", "purse", "position", "updated_at"]]
    now = datetime.utcnow().isoformat()
    for row in rows:
        values.append(
            [row["tournament"], row["golfer"], row["purse"] or 0, row["position"] or "", now]
        )
    try:
        worksheet.clear()
        worksheet.update("A1", values)
    except APIError:
        return
def sync_picks_to_sheet(conn: sqlite3.Connection) -> None:
    worksheet = get_picks_worksheet()
    if not worksheet:
        return
    picks = conn.execute(
        """
        SELECT users.name as user,
               tournaments.name as tournament,
               golfers.name as golfer,
               picks.created_at as created_at
        FROM picks
        JOIN users ON users.id = picks.user_id
        JOIN tournaments ON tournaments.id = picks.tournament_id
        JOIN golfers ON golfers.id = picks.golfer_id
        ORDER BY picks.created_at
        """
    ).fetchall()
    values = [["user", "tournament", "golfer", "created_at"]]
    for row in picks:
        values.append([row["user"], row["tournament"], row["golfer"], row["created_at"]])
    try:
        worksheet.clear()
        worksheet.update("A1", values)
    except APIError:
        return

def persist_picks(conn: sqlite3.Connection) -> None:
    worksheet = get_picks_worksheet()
    if worksheet:
        sync_picks_to_sheet(conn)
    else:
        save_picks_snapshot(conn)


def persist_results(conn: sqlite3.Connection) -> None:
    if get_results_worksheet():
        sync_results_to_sheet(conn)

def hydrate_picks(conn: sqlite3.Connection) -> None:
    worksheet = get_picks_worksheet()
    if worksheet:
        if sync_picks_from_sheet(conn):
            return
    if conn.execute("SELECT COUNT(*) FROM picks").fetchone()[0] == 0:
        restore_picks_snapshot(conn)


def hydrate_results(conn: sqlite3.Connection) -> None:
    if get_results_worksheet():
        if sync_results_from_sheet(conn):
            return


def restore_picks_snapshot(conn: sqlite3.Connection) -> bool:
    path = get_picks_backup_path()
    if not os.path.exists(path):
        return False
    with open(path, "r", encoding="utf-8") as handle:
        try:
            payload = json.load(handle)
        except json.JSONDecodeError:
            return False
    if not payload:
        return False
    restored = 0
    for row in payload:
        user = conn.execute("SELECT id FROM users WHERE name = ?", (row["user"],)).fetchone()
        tournament = conn.execute("SELECT id FROM tournaments WHERE name = ?", (row["tournament"],)).fetchone()
        golfer = conn.execute("SELECT id FROM golfers WHERE name = ?", (row["golfer"],)).fetchone()
        if not user or not tournament or not golfer:
            continue
        conn.execute(
            "INSERT OR IGNORE INTO picks (user_id, tournament_id, golfer_id, created_at) VALUES (?, ?, ?, ?)",
            (user["id"], tournament["id"], golfer["id"], row.get("created_at") or datetime.utcnow().isoformat()),
        )
        restored += 1
    conn.commit()
    return restored > 0


def bdl_get(path: str, params: Optional[dict] = None) -> dict:
    api_key = os.getenv("BDL_API_KEY")
    if not api_key:
        raise RuntimeError("Missing BDL_API_KEY")
    headers = {"Authorization": api_key}
    resp = requests.get(f"{BDL_BASE}{path}", headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def bdl_fetch_all(path: str, params: Optional[dict] = None) -> list:
    params = params or {}
    params = {**params, "per_page": 100}
    results = []
    cursor = None
    while True:
        if cursor:
            params["cursor"] = cursor
        data = bdl_get(path, params)
        results.extend(data.get("data", []))
        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break
    return results


def rapidapi_get(path: str, params: Optional[dict] = None) -> dict:
    api_key = os.getenv("RAPIDAPI_KEY")
    host = os.getenv("RAPIDAPI_HOST", "live-golf-data.p.rapidapi.com")
    if not api_key:
        raise RuntimeError("Missing RAPIDAPI_KEY")
    headers = {
        "x-rapidapi-host": host,
        "x-rapidapi-key": api_key,
    }
    resp = requests.get(f"https://{host}{path}", headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def rapidapi_get_with_fallback(
    paths: list[str], params_list: list[dict]
) -> dict:
    last_error = None
    for path, params in zip(paths, params_list):
        try:
            return rapidapi_get(path, params)
        except requests.HTTPError as exc:
            last_error = exc
            status = getattr(exc.response, "status_code", None)
            if status not in (400, 404):
                break
            continue
    if last_error:
        raise last_error
    raise RuntimeError("RapidAPI request failed.")


def rapidapi_fetch_schedule(year: int) -> dict:
    return rapidapi_get_with_fallback(
        ["/schedules", "/schedule"],
        [{"orgId": 1, "year": year}, {"orgId": 1, "year": year}],
    )


def rapidapi_fetch_leaderboard(tourn_id: str, year: int) -> dict:
    return rapidapi_get_with_fallback(
        ["/leaderboards", "/leaderboard"],
        [
            {"orgId": 1, "tournId": tourn_id, "year": year},
            {"orgId": 1, "tournId": tourn_id, "year": year},
        ],
    )


def rapidapi_fetch_earnings(tourn_id: str, year: int) -> dict:
    return rapidapi_get_with_fallback(
        ["/earnings", "/earning"],
        [
            {"orgId": 1, "tournId": tourn_id, "year": year},
            {"orgId": 1, "tournId": tourn_id, "year": year},
        ],
    )


def normalize_person_name(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9 ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_tournament_name(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9 ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    for token in [
        "presented by",
        "pres by",
        "pres.",
        "sponsored by",
        "the ",
        "tournament",
        "championship",
        "invitational",
        "open",
        "classic",
        "pro am",
        "pro-am",
        "by",
    ]:
        value = value.replace(token, " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def resolve_tourn_id_for_event(
    conn: sqlite3.Connection, tournament_id: int, year: int
) -> tuple[Optional[str], Optional[int]]:
    row = conn.execute(
        "SELECT name, start_date FROM tournaments WHERE id = ?",
        (tournament_id,),
    ).fetchone()
    if not row:
        return None, None
    target_name = normalize_tournament_name(row["name"])
    target_start = row["start_date"]

    for test_year in (year, year - 1):
        try:
            schedule = rapidapi_fetch_schedule(test_year)
        except Exception:
            continue
        items = schedule.get("schedule") or schedule.get("tournaments") or schedule.get("data") or []
        best = None
        best_score = 0.0
        for item in items:
            name = (
                item.get("tournament")
                or item.get("name")
                or item.get("tournName")
                or item.get("eventName")
                or ""
            )
            if not name:
                continue
            norm = normalize_tournament_name(name)
            score = SequenceMatcher(None, target_name, norm).ratio()
            start = extract_date(
                item.get("startDate")
                or item.get("start_date")
                or item.get("start")
                or item.get("startDateUtc")
            )
            if start and target_start and start == target_start:
                score += 0.25
            if score > best_score:
                best_score = score
                best = item
        if best and best_score >= 0.6:
            tourn_id = best.get("tournId") or best.get("tournamentId") or best.get("id")
            if tourn_id is not None:
                return str(tourn_id), test_year
    return None, None


def sync_results_from_rapidapi(
    conn: sqlite3.Connection, tourn_id: str, year: int, tournament_id: int
) -> tuple[int, int]:
    safe_tourn_id = str(tourn_id).strip()
    leaderboard = rapidapi_fetch_leaderboard(safe_tourn_id, year)
    try:
        earnings = rapidapi_fetch_earnings(safe_tourn_id, year)
    except requests.HTTPError:
        earnings = {"leaderboard": []}

    leaderboard_rows = leaderboard.get("leaderboard", [])
    earnings_rows = earnings.get("leaderboard", [])

    positions = {}
    for row in leaderboard_rows:
        first = row.get("firstName") or ""
        last = row.get("lastName") or ""
        name = f"{first} {last}".strip()
        pos_raw = row.get("position")
        pos = None
        if isinstance(pos_raw, str) and pos_raw.upper().startswith("T"):
            pos_raw = pos_raw[1:]
        if isinstance(pos_raw, str) and pos_raw.isdigit():
            pos = int(pos_raw)
        elif isinstance(pos_raw, int):
            pos = pos_raw
        if name:
            positions[normalize_person_name(name)] = pos

    earnings_map = {}
    for row in earnings_rows:
        first = row.get("firstName") or ""
        last = row.get("lastName") or ""
        name = f"{first} {last}".strip()
        earn = row.get("earnings")
        if name and isinstance(earn, (int, float)):
            earnings_map[normalize_person_name(name)] = int(round(earn))

    golfers = conn.execute("SELECT id, name FROM golfers WHERE active = 1").fetchall()
    golfer_lookup = {normalize_person_name(row["name"]): row["id"] for row in golfers}

    upsert = conn.execute
    updated = 0
    skipped = 0
    for norm_name, earnings_value in earnings_map.items():
        golfer_id = golfer_lookup.get(norm_name)
        if not golfer_id:
            skipped += 1
            continue
        position = positions.get(norm_name)
        upsert(
            "INSERT INTO results (tournament_id, golfer_id, purse, position) VALUES (?, ?, ?, ?)\n"
            "ON CONFLICT(tournament_id, golfer_id) DO UPDATE SET purse = excluded.purse, position = excluded.position",
            (tournament_id, golfer_id, earnings_value, position),
        )
        updated += 1

    # If earnings are missing, still store positions with zero purse so top5/top10 update.
    if not earnings_map and positions:
        for norm_name, position in positions.items():
            golfer_id = golfer_lookup.get(norm_name)
            if not golfer_id:
                skipped += 1
                continue
            upsert(
                "INSERT INTO results (tournament_id, golfer_id, purse, position) VALUES (?, ?, ?, ?)\n"
                "ON CONFLICT(tournament_id, golfer_id) DO UPDATE SET position = excluded.position",
                (tournament_id, golfer_id, 0, position),
            )
            updated += 1

    conn.commit()
    return updated, skipped


def sync_results_with_autoresolve(
    conn: sqlite3.Connection, tourn_id: str, year: int, tournament_id: int
) -> tuple[int, int]:
    attempts = []
    if tourn_id:
        attempts.append((tourn_id, year))
        attempts.append((tourn_id, year - 1))
    resolved_id, resolved_year = resolve_tourn_id_for_event(conn, tournament_id, year)
    if resolved_id:
        attempts.append((resolved_id, resolved_year or year))
        attempts.append((resolved_id, (resolved_year or year) - 1))

    last_error = None
    for attempt_id, attempt_year in attempts:
        if not attempt_id:
            continue
        try:
            updated, skipped = sync_results_from_rapidapi(
                conn, str(attempt_id), int(attempt_year), tournament_id
            )
            if updated > 0:
                return updated, skipped
        except requests.HTTPError as exc:
            last_error = exc
            continue
    if last_error:
        raise last_error
    return 0, 0


def extract_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return str(value)[:10]


def sync_tourn_ids_from_rapidapi(conn: sqlite3.Connection, year: int) -> tuple[int, int]:
    schedule = rapidapi_fetch_schedule(year)
    items = schedule.get("schedule") or schedule.get("tournaments") or schedule.get("data") or []

    db_tournaments = conn.execute(
        "SELECT id, name, start_date FROM tournaments ORDER BY start_date"
    ).fetchall()
    by_name_date = {
        (normalize_person_name(row["name"]), row["start_date"]): row["id"]
        for row in db_tournaments
    }
    by_name_only = {normalize_person_name(row["name"]): row["id"] for row in db_tournaments}

    updated = 0
    skipped = 0
    for item in items:
        name = item.get("tournament") or item.get("name") or item.get("tournName") or item.get("eventName")
        if not name:
            skipped += 1
            continue
        tourn_id = item.get("tournId") or item.get("tournamentId") or item.get("id")
        if tourn_id is None:
            skipped += 1
            continue
        start = extract_date(
            item.get("startDate")
            or item.get("start_date")
            or item.get("start")
            or item.get("startDateUtc")
        )
        norm = normalize_person_name(name)
        db_id = None
        if start:
            db_id = by_name_date.get((norm, start))
        if not db_id:
            db_id = by_name_only.get(norm)
        if not db_id:
            skipped += 1
            continue
        conn.execute(
            "UPDATE tournaments SET rapid_tourn_id = ? WHERE id = ?",
            (str(tourn_id), db_id),
        )
        updated += 1

    conn.commit()
    return updated, skipped


def sync_tournaments(conn: sqlite3.Connection, season: int = 2026) -> int:
    tournaments = bdl_fetch_all("/tournaments", {"season": season})
    existing = {
        normalize_name(row["name"]): row
        for row in conn.execute("SELECT id, name FROM tournaments WHERE season = ?", (season,))
    }
    inserted = 0
    for t in tournaments:
        name = t["name"]
        norm = normalize_name(name)
        start = t["start_date"][:10]
        end = t["end_date"][:10]
        if norm in existing:
            conn.execute(
                "UPDATE tournaments SET start_date = ?, end_date = ?, bdl_id = ? WHERE id = ?",
                (start, end, t["id"], existing[norm]["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO tournaments (name, start_date, end_date, season, bdl_id) VALUES (?, ?, ?, ?, ?)",
                (name, start, end, season, t["id"]),
            )
            inserted += 1
    conn.commit()
    return inserted


def sync_results(conn: sqlite3.Connection, season: int = 2026) -> int:
    tournaments = conn.execute(
        "SELECT id, name, bdl_id FROM tournaments WHERE season = ? AND bdl_id IS NOT NULL",
        (season,),
    ).fetchall()

    golfers = conn.execute("SELECT id, name, bdl_id FROM golfers").fetchall()
    golfer_by_norm = {normalize_name(row["name"]): row for row in golfers}

    updated = 0
    for t in tournaments:
        results = bdl_fetch_all("/tournament_results", {"tournament_ids": t["bdl_id"]})
        for r in results:
            player = r.get("player") or {}
            display = player.get("display_name")
            if not display:
                continue
            norm = normalize_name(display)
            golfer = golfer_by_norm.get(norm)
            if not golfer:
                conn.execute(
                    "INSERT OR IGNORE INTO golfers (name, active, bdl_id) VALUES (?, 1, ?)",
                    (display, player.get("id")),
                )
                golfer = conn.execute("SELECT id, name, bdl_id FROM golfers WHERE name = ?", (display,)).fetchone()
                golfer_by_norm[norm] = golfer
            elif golfer["bdl_id"] is None:
                conn.execute("UPDATE golfers SET bdl_id = ? WHERE id = ?", (player.get("id"), golfer["id"]))

            earnings = r.get("earnings") or 0
            position = r.get("position_numeric")
            conn.execute(
                "INSERT INTO results (tournament_id, golfer_id, purse, position) VALUES (?, ?, ?, ?)"
                " ON CONFLICT(tournament_id, golfer_id) DO UPDATE SET purse = excluded.purse, position = excluded.position",
                (t["id"], golfer["id"], int(round(earnings)), position),
            )
            updated += 1
    conn.commit()
    return updated


def format_money(value: int) -> str:
    return f"${value:,.0f}"

def format_short_date(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%b %-d")
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%d").strftime("%b %d")
        except ValueError:
            return value

def parse_clipboard_results(raw: str):
    rows = []
    errors = []
    line_re = re.compile(
        r"^(?:P)?(?P<pos>\d+)\s+(?P<name>.+?)\s+\$?(?P<purse>[\d,]+(?:\.\d{2})?)\s*$",
        re.IGNORECASE,
    )
    for idx, line in enumerate(raw.splitlines(), start=1):
        text = line.strip()
        if not text:
            continue

        match = line_re.match(text)
        if match:
            pos = match.group("pos")
            name = match.group("name").strip()
            purse = f"${match.group('purse')}"
            rows.append((name, pos, purse))
            continue

        # Tab-delimited: Position<TAB>Name<TAB>$Purse
        if "\t" in text:
            parts = [p.strip() for p in text.split("\t") if p.strip()]
            if len(parts) >= 3:
                pos_token = parts[0].upper()
                pos = (
                    pos_token[1:]
                    if pos_token.startswith("P") and pos_token[1:].isdigit()
                    else pos_token
                    if pos_token.isdigit()
                    else None
                )
                name = parts[1]
                purse = next((p for p in parts[2:] if "$" in p), None)
                if name and purse and pos:
                    rows.append((name, pos, purse))
                    continue

        # CSV style: Name, Position, Purse OR Name, Purse, Position
        if "," in text:
            parts = [p.strip() for p in text.split(",") if p.strip()]
            if len(parts) >= 3:
                name = parts[0]
                pos = None
                purse = None
                for part in parts[1:]:
                    if "$" in part or part.replace(",", "").replace(".", "").isdigit():
                        purse = part if "$" in part else f"${part}"
                    elif part.isdigit():
                        pos = part
                if name and purse and pos:
                    rows.append((name, pos, purse))
                    continue

        errors.append((idx, text))
    return rows, errors


def build_leaderboard(conn: sqlite3.Connection):
    return conn.execute(
        """
        SELECT users.name,
               COALESCE(SUM(results.purse), 0) as total,
               SUM(CASE WHEN results.position = 1 THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN results.position IS NOT NULL AND results.position <= 5 THEN 1 ELSE 0 END) as top5,
               SUM(CASE WHEN results.position IS NOT NULL AND results.position <= 10 THEN 1 ELSE 0 END) as top10
        FROM users
        LEFT JOIN picks ON picks.user_id = users.id
        LEFT JOIN results ON results.tournament_id = picks.tournament_id AND results.golfer_id = picks.golfer_id
        GROUP BY users.id
        ORDER BY total DESC, users.name ASC
        """
    ).fetchall()


def get_today_tournament(conn: sqlite3.Connection):
    today = date.today().isoformat()
    return conn.execute(
        "SELECT * FROM tournaments WHERE start_date <= ? AND end_date >= ? ORDER BY start_date LIMIT 1",
        (today, today),
    ).fetchone()


def get_next_tournament(conn: sqlite3.Connection):
    today = date.today().isoformat()
    return conn.execute(
        "SELECT * FROM tournaments WHERE start_date > ? ORDER BY start_date LIMIT 1",
        (today,),
    ).fetchone()


def get_next_tournament_index(tournaments) -> int:
    today = date.today().isoformat()
    for idx, t in enumerate(tournaments):
        if t["start_date"] >= today:
            return idx
    return 0


def get_current_or_next_tournament_index(tournaments) -> int:
    today = date.today().isoformat()
    for idx, t in enumerate(tournaments):
        if t["start_date"] <= today <= t["end_date"]:
            return idx
    return get_next_tournament_index(tournaments)


def is_admin() -> bool:
    return True


def admin_gate():
    return


def maybe_run_scheduled_sync(conn: sqlite3.Connection):
    params = st.query_params
    token = os.getenv("SYNC_TOKEN")
    if params.get("sync") == "1" and token and params.get("token") == token:
        tourn_id = params.get("tournId")
        year = params.get("year")
        if not tourn_id or not year:
            st.error("Missing tournId or year for scheduled sync.")
            return
        tournament = conn.execute(
            "SELECT id FROM tournaments WHERE rapid_tourn_id = ?",
            (tourn_id,),
        ).fetchone()
        if not tournament:
            st.error("Scheduled sync failed: tournament not found for tournId.")
            return
        st.info("Running scheduled sync...")
        try:
            updated, skipped = sync_results_from_rapidapi(
                conn, str(tourn_id), int(year), tournament["id"]
            )
            st.success(f"Synced {updated} results. Skipped {skipped} names.")
        except Exception as exc:
            st.error(f"Sync failed: {exc}")


def main():
    load_env_file()
    st.set_page_config(page_title="Golf One & Done Pool", layout="wide")
    admin_gate()

    conn = get_conn()
    init_db(conn)
    seed_if_needed(conn)
    hydrate_picks(conn)
    if conn.execute("SELECT COUNT(*) FROM results").fetchone()[0] == 0:
        hydrate_results(conn)

    st.markdown(
        textwrap.dedent(
            """
            <style>
              :root {
                --midnight-base: #0d0f14;
                --midnight-slate: #1c222b;
                --midnight-olive: #2a322c;
                --midnight-gold: #c9a646;
              }
              html, body {
                background: radial-gradient(1200px 600px at 10% 0%, var(--midnight-slate) 0%, var(--midnight-base) 60%) !important;
                background-attachment: fixed !important;
              }
              .stApp,
              .st-emotion-cache-13k62yr {
                background: radial-gradient(1200px 600px at 10% 0%, var(--midnight-slate) 0%, var(--midnight-base) 60%) !important;
              }
              [data-testid="stAppViewContainer"],
              [data-testid="stAppViewContainer"] > .main,
              [data-testid="stAppViewContainer"] > .main > div,
              [data-testid="stAppViewContainer"] [data-testid="stAppViewBlockContainer"],
              [data-testid="stAppViewContainer"] [data-testid="stAppViewBlockContainer"] > div,
              section.main,
              section.main > div {
                background: transparent !important;
              }
              [class^="st-emotion-cache-"],
              [class^="st-emotion-cache-"] > div {
                background: transparent !important;
              }
              .st-emotion-cache-13k62yr > div {
                background: transparent !important;
              }
              [data-testid="stHeader"], [data-testid="stToolbar"] {
                background: transparent !important;
              }
              .masters-board {
                border: 2px solid #0c4b2b;
                border-radius: 12px;
                overflow: hidden;
                background: #0c4b2b;
              }
              .masters-board table {
                width: 100%;
                border-collapse: collapse;
                font-family: "Georgia", "Times New Roman", serif;
                background: #f7f3e7;
              }
              .masters-board th {
                background: #0c4b2b;
                color: #f7f3e7;
                text-align: left;
                padding: 10px 12px;
                font-size: 0.9rem;
                letter-spacing: 0.04em;
                text-transform: uppercase;
              }
              .masters-board td {
                padding: 10px 12px;
                border-bottom: 1px solid #e2dbc7;
                color: #1f1f1b;
              }
              .masters-board tr:nth-child(even) td {
                background: #fbf8ef;
              }
              .masters-board .player {
                font-weight: 700;
              }
              .masters-board .money {
                font-variant-numeric: tabular-nums;
              }
              .masters-board .badge {
                display: inline-block;
                padding: 2px 8px;
                border-radius: 999px;
                background: #f0c84b;
                color: #1f1f1b;
                font-size: 0.75rem;
                font-weight: 700;
              }
              button[kind="secondary"] {
                background: transparent !important;
                border: none !important;
                padding: 0 !important;
                min-height: 0 !important;
                height: auto !important;
                box-shadow: none !important;
              }
              button[kind="secondary"] p {
                font-size: 20px !important;
                line-height: 1 !important;
                margin: 0 !important;
                letter-spacing: 2px;
              }
              button[kind="secondary"]:hover {
                color: #f0c84b !important;
                background: transparent !important;
              }
              button[kind="secondary"]:focus {
                outline: none !important;
                box-shadow: none !important;
              }
              .menu-inline {
                display: flex;
                gap: 8px;
                align-items: center;
              }
              .schedule-list {
                display: flex;
                flex-direction: column;
                gap: 14px;
              }
              .schedule-card {
                border: 2px solid #0c4b2b;
                border-radius: 12px;
                padding: 12px 16px;
                background: #f7f3e7;
              }
              .schedule-name {
                font-weight: 700;
                color: #0c4b2b;
                letter-spacing: 0.02em;
              }
              .schedule-meta {
                color: #0c4b2b;
                margin-top: 4px;
              }
              .schedule-tag {
                display: inline-block;
                margin-left: 8px;
                font-weight: 700;
                color: #f0c84b;
              }
              .picks-row {
                border: 2px solid #0c4b2b;
                border-radius: 12px;
                padding: 8px 12px;
                margin-bottom: 10px;
                background: #f7f3e7;
              }
              .picks-user {
                color: #0c4b2b;
                font-weight: 700;
              }
              .picks-golfer {
                color: #f0c84b;
                font-weight: 700;
              }
            </style>
            """
        ),
        unsafe_allow_html=True,
    )

    st.title("Golf One & Done Pool")
    # Subtitle removed per user preference.

    maybe_run_scheduled_sync(conn)

    tab_dashboard, tab_picks, tab_tournaments, tab_players, tab_admin = st.tabs(
        ["Dashboard", "Picks", "Schedule", "Players", "Admin"]
    )

    with tab_dashboard:
        st.subheader("Leaderboard")
        leaderboard = build_leaderboard(conn)
        rows = []
        for row in leaderboard:
            rows.append(
                f"<tr>"
                f"<td class=\"player\">{row['name']}</td>"
                f"<td class=\"money\">{format_money(row['total'])}</td>"
                f"<td><span class=\"badge\">{row['wins']}</span></td>"
                f"<td>{row['top5']}</td>"
                f"<td>{row['top10']}</td>"
                f"</tr>"
            )

        table_html = textwrap.dedent(
            f"""
            <div class="masters-board">
              <table>
                <thead>
                  <tr>
                    <th>Player</th>
                    <th>Total</th>
                    <th>Wins</th>
                    <th>Top 5</th>
                    <th>Top 10</th>
                  </tr>
                </thead>
                <tbody>
                  {''.join(rows)}
                </tbody>
              </table>
            </div>
            """
        )
        st.markdown(table_html, unsafe_allow_html=True)

        def tournament_badge(row):
            if row["is_major"]:
                return " **MAJOR**"
            if row["is_signature"]:
                return " **SIGNATURE**"
            return ""

        current = get_today_tournament(conn)
        col_tournaments, col_rules = st.columns([2, 1])
        with col_tournaments:
            st.subheader("Current Tournament")
            if current:
                st.write(
                    f"{current['name']} ({format_short_date(current['start_date'])} to {format_short_date(current['end_date'])}){tournament_badge(current)}"
                )
                next_up = get_next_tournament(conn)
                if next_up:
                    st.subheader("Upcoming Tournament")
                    st.write(
                        f"{next_up['name']} ({format_short_date(next_up['start_date'])} to {format_short_date(next_up['end_date'])}){tournament_badge(next_up)}"
                    )
            else:
                next_up = get_next_tournament(conn)
                if next_up:
                    st.write(
                        f"Next up: {next_up['name']} ({format_short_date(next_up['start_date'])} to {format_short_date(next_up['end_date'])})"
                    )
                else:
                    st.write("No tournament in progress today.")

        with col_rules:
            st.subheader("Rules")
            st.markdown(
                "- One pick per tournament\n"
                "- Majors = two picks\n"
                "- One non-major double pick per season\n"
                "- Picks lock Thursday 7:00 AM ET\n"
                "- Earnings = leaderboard totals"
            )

    with tab_picks:
        st.subheader("Weekly Picks")
        tournament_order = conn.execute(
            "SELECT id, name, start_date, end_date, purse FROM tournaments ORDER BY start_date"
        ).fetchall()
        week_map = {row["name"]: idx + 1 for idx, row in enumerate(tournament_order)}

        col_left, col_right = st.columns([2, 3])

        # pick current tournament if in progress, otherwise next upcoming
        next_index = get_current_or_next_tournament_index(tournament_order)

        with col_left:
            st.markdown("#### Picks By Tournament")
            tournament_options = []
            tournament_label_map = {}
            for row in tournament_order:
                label = (
                    f"Week {week_map.get(row['name'], '—')} — {row['name']} "
                    f"({format_short_date(row['start_date'])}–{format_short_date(row['end_date'])})"
                )
                tournament_options.append(label)
                tournament_label_map[label] = row["name"]
            default_label = tournament_options[next_index] if tournament_options else ""
            if "picks_tournament_select" not in st.session_state:
                st.session_state["picks_tournament_select"] = default_label
            tournament_label = st.selectbox(
                "Tournament",
                tournament_options,
                key="picks_tournament_select",
            )
            selected_name = tournament_label_map.get(
                tournament_label,
                tournament_order[next_index]["name"] if tournament_order else "",
            )
            pending_delete_user = st.session_state.get("delete_user")
            pending_delete_tourn = st.session_state.get("delete_tourn")
            if pending_delete_user and pending_delete_tourn:
                pick_row = conn.execute(
                    "SELECT users.name as user, GROUP_CONCAT(golfers.name, ', ') as golfers\n"
                    "FROM picks\n"
                    "JOIN users ON users.id = picks.user_id\n"
                    "JOIN golfers ON golfers.id = picks.golfer_id\n"
                    "JOIN tournaments ON tournaments.id = picks.tournament_id\n"
                    "WHERE users.name = ? AND tournaments.name = ?\n"
                    "GROUP BY users.name",
                    (pending_delete_user, pending_delete_tourn),
                ).fetchone()
                if pick_row:
                    st.warning(
                        f"Delete picks for {pick_row['user']} → {pick_row['golfers']}?"
                    )
                    col_confirm, col_cancel = st.columns([1, 1])
                    if col_confirm.button("Yes, delete", key="confirm_delete_pick", type="primary"):
                        conn.execute(
                            "DELETE FROM picks WHERE user_id = (SELECT id FROM users WHERE name = ?) "
                            "AND tournament_id = (SELECT id FROM tournaments WHERE name = ?)",
                            (pending_delete_user, pending_delete_tourn),
                        )
                        conn.commit()
                        persist_picks(conn)
                        st.session_state["delete_user"] = None
                        st.session_state["delete_tourn"] = None
                        st.success("Picks deleted.")
                    if col_cancel.button("Cancel", key="cancel_delete_pick", type="primary"):
                        st.session_state["delete_user"] = None
                        st.session_state["delete_tourn"] = None
            tournament_picks = conn.execute(
                """
                SELECT users.name as user,
                       GROUP_CONCAT(golfers.name, '\n') as golfer_list
                FROM picks
                JOIN users ON users.id = picks.user_id
                JOIN golfers ON golfers.id = picks.golfer_id
                JOIN tournaments ON tournaments.id = picks.tournament_id
                WHERE tournaments.name = ?
                GROUP BY users.name
                ORDER BY users.name
                """,
                (selected_name,),
            ).fetchall()
            for row in tournament_picks:
                st.markdown('<div class="picks-row">', unsafe_allow_html=True)
                col_a, col_b, col_c = st.columns([3, 4, 1])
                col_a.markdown(f"<span class='picks-user'>{row['user']}</span>", unsafe_allow_html=True)
                col_b.markdown(
                    f"<span class='picks-golfer'>{(row['golfer_list'] or '').replace(chr(10), '<br/>')}</span>",
                    unsafe_allow_html=True,
                )
                with col_c:
                    menu_clicked = st.button(
                        "...",
                        key=f"menu_pick_{row['user']}",
                        type="secondary",
                    )
                    if menu_clicked:
                        st.session_state["delete_user"] = row["user"]
                        st.session_state["delete_tourn"] = selected_name
                st.markdown("</div>", unsafe_allow_html=True)

        with col_right:
            st.markdown("#### Picks By Player")
            users = conn.execute("SELECT id, name FROM users ORDER BY name").fetchall()
            user_label = st.selectbox("Player", [u["name"] for u in users], key="picks_user_select")
            user_id = next(u["id"] for u in users if u["name"] == user_label)
            user_picks = conn.execute(
                """
                SELECT tournaments.name as tournament, golfers.name as golfer, tournaments.start_date, tournaments.end_date
                FROM picks
                JOIN tournaments ON tournaments.id = picks.tournament_id
                JOIN golfers ON golfers.id = picks.golfer_id
                WHERE picks.user_id = ?
                ORDER BY tournaments.start_date
                """,
                (user_id,),
            ).fetchall()
            st.dataframe(
                [
                    {
                        "Week": week_map.get(row["tournament"], "—"),
                        "Tournament": f"{row['tournament']} ({format_short_date(row['start_date'])}–{format_short_date(row['end_date'])})",
                        "Golfer": row["golfer"],
                    }
                    for row in user_picks
                ],
                use_container_width=True,
                hide_index=True,
            )

        if is_admin():
            st.markdown("#### Add Pick")
            users = conn.execute("SELECT id, name FROM users ORDER BY name").fetchall()
            tournaments = conn.execute(
                "SELECT id, name, start_date, end_date, is_major FROM tournaments ORDER BY start_date"
            ).fetchall()
            golfers = conn.execute(
                "SELECT id, name, fedex_rank FROM golfers WHERE active = 1 ORDER BY fedex_rank IS NULL, fedex_rank, name"
            ).fetchall()
            today_str = date.today().isoformat()
            next_index = 0
            for idx, t in enumerate(tournaments):
                if t["start_date"] >= today_str:
                    next_index = idx
                    break

            user_name = st.selectbox("User", [u["name"] for u in users])
            tournament_name = st.selectbox(
                "Tournament",
                [f"{t['name']} ({format_short_date(t['start_date'])}–{format_short_date(t['end_date'])})" for t in tournaments],
                index=next_index,
                key="admin_pick_tournament",
            )
            golfer_name = st.selectbox("Golfer", [g["name"] for g in golfers])
            selected_tournament = tournaments[
                [f"{t['name']} ({format_short_date(t['start_date'])}–{format_short_date(t['end_date'])})" for t in tournaments].index(tournament_name)
            ]
            is_major = bool(selected_tournament["is_major"])
            st.caption("Major event" if is_major else "Regular event")
            use_double_pick = False
            if not is_major:
                use_double_pick = st.checkbox("Use season double-pick (non-major)")
            second_golfer_name = None
            if is_major or use_double_pick:
                second_golfer_name = st.selectbox(
                    "Second Golfer",
                    [g["name"] for g in golfers],
                    key="second_golfer",
                )

            if st.button("Save Pick", type="primary"):
                user_id = next(u["id"] for u in users if u["name"] == user_name)
                tournament_id = selected_tournament["id"]
                golfer_id = next(g["id"] for g in golfers if g["name"] == golfer_name)
                second_golfer_id = None
                if second_golfer_name:
                    second_golfer_id = next(g["id"] for g in golfers if g["name"] == second_golfer_name)

                existing = conn.execute(
                    "SELECT COUNT(*) FROM picks WHERE user_id = ? AND tournament_id = ?",
                    (user_id, tournament_id),
                ).fetchone()[0]
                used = conn.execute(
                    "SELECT 1 FROM picks WHERE user_id = ? AND golfer_id = ?",
                    (user_id, golfer_id),
                ).fetchone()
                if is_major and existing >= 2:
                    st.error("User already has two picks for this major.")
                elif not is_major and existing >= 1 and not use_double_pick:
                    st.error("User already has a pick for this tournament.")
                elif not is_major and existing >= 2:
                    st.error("User already used the double pick here.")
                elif used:
                    st.error("User already used this golfer.")
                elif second_golfer_id and conn.execute(
                    "SELECT 1 FROM picks WHERE user_id = ? AND golfer_id = ?",
                    (user_id, second_golfer_id),
                ).fetchone():
                    st.error("User already used the second golfer.")
                elif second_golfer_id and second_golfer_id == golfer_id:
                    st.error("Choose two different golfers.")
                elif use_double_pick and conn.execute(
                    "SELECT double_pick_used FROM users WHERE id = ?",
                    (user_id,),
                ).fetchone()[0] == 1:
                    st.error("User already used the season double-pick.")
                else:
                    conn.execute(
                        "INSERT INTO picks (user_id, tournament_id, golfer_id, created_at) VALUES (?, ?, ?, ?)",
                        (user_id, tournament_id, golfer_id, datetime.utcnow().isoformat()),
                    )
                    if second_golfer_id:
                        conn.execute(
                            "INSERT INTO picks (user_id, tournament_id, golfer_id, created_at) VALUES (?, ?, ?, ?)",
                            (user_id, tournament_id, second_golfer_id, datetime.utcnow().isoformat()),
                        )
                        if not is_major and use_double_pick:
                            conn.execute(
                                "UPDATE users SET double_pick_used = 1 WHERE id = ?",
                                (user_id,),
                            )
                    conn.commit()
                    persist_picks(conn)
                    st.success("Pick saved.")
                    st.rerun()

    with tab_tournaments:
        today_str = date.today().isoformat()
        tournaments = conn.execute(
            "SELECT name, start_date, end_date, is_major, is_signature, purse "
            "FROM tournaments WHERE end_date >= ? ORDER BY start_date",
            (today_str,),
        ).fetchall()
        cards = []
        for row in tournaments:
            tag = ""
            if row["is_major"]:
                tag = "M"
            elif row["is_signature"]:
                tag = "S"
            tag_html = f'<span class="schedule-tag">({tag})</span>' if tag else ""
            cards.append(
                f"<div class=\"schedule-card\">"
                f"<div class=\"schedule-name\">{row['name']}{tag_html}</div>"
                f"<div class=\"schedule-meta\">{format_short_date(row['start_date'])} to {format_short_date(row['end_date'])}</div>"
                f"<div class=\"schedule-meta\">{format_money(row['purse']) if row['purse'] else '—'}</div>"
                f"</div>"
            )
        st.markdown(
            f"<div class=\"schedule-list\">{''.join(cards)}</div>",
            unsafe_allow_html=True,
        )

    with tab_players:
        st.subheader("Player Roster")
        golfers = conn.execute(
            "SELECT name FROM golfers WHERE active = 1 ORDER BY name"
        ).fetchall()
        golfer_names = [row["name"] for row in golfers]
        st.selectbox("Search golfers", golfer_names)

        if is_admin():
            st.markdown("#### Add Golfer")
            gname = st.text_input("Golfer name")
            if st.button("Add Golfer", type="primary"):
                if not gname.strip():
                    st.error("Golfer name is required.")
                else:
                    conn.execute(
                        "INSERT OR IGNORE INTO golfers (name) VALUES (?)",
                        (gname.strip(),),
                    )
                    conn.commit()
                    st.success("Golfer added.")

            st.markdown("#### Bulk Import Golfers")
            bulk_golfers = st.text_area("Paste golfers", height=160)
            st.caption("Format: Name only OR Name, Rank, Points (Rank/Points optional)")
            if st.button("Import Golfers", type="primary"):
                count = 0
                for line in bulk_golfers.splitlines():
                    parts = [p.strip() for p in line.split(",") if p.strip()]
                    if not parts:
                        continue
                    name = parts[0]
                    rank = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
                    points = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
                    conn.execute(
                        "INSERT OR IGNORE INTO golfers (name, fedex_rank, fedex_points) VALUES (?, ?, ?)",
                        (name, rank, points),
                    )
                    count += 1
                conn.commit()
                st.success(f"Imported {count} golfer lines.")

            st.markdown("#### Replace Roster (Name Only)")
            roster_text = st.text_area("Paste full roster (one golfer per line)", height=200, key="roster_replace")
            st.caption("This will set all current golfers to inactive, then activate the pasted names.")
            if st.button("Replace Roster", type="primary"):
                names = [line.strip() for line in roster_text.splitlines() if line.strip()]
                if not names:
                    st.error("Paste at least one golfer.")
                else:
                    conn.execute("UPDATE golfers SET active = 0")
                    for name in names:
                        conn.execute(
                            "INSERT INTO golfers (name, active) VALUES (?, 1)\n"
                            "ON CONFLICT(name) DO UPDATE SET active = 1, fedex_rank = NULL, fedex_points = NULL",
                            (name,),
                        )
                    conn.commit()
                    st.success(f"Roster replaced with {len(names)} golfers.")

    with tab_admin:
        st.subheader("Admin Tools")
        st.write("Admin owner: Carl")

        if not is_admin():
            st.warning("Admin access required.")
        else:
            st.markdown("#### Storage Status")
            if get_sheets_id():
                if get_picks_worksheet():
                    st.success("Google Sheets storage: connected.")
                    sheet_url = f"https://docs.google.com/spreadsheets/d/{get_sheets_id()}/edit"
                    st.markdown(f"[Open Picks Sheet]({sheet_url})")
                else:
                    st.warning("Google Sheets storage: configured but not connected.")
            else:
                st.info("Google Sheets storage: not configured. Using local backup.")

            st.markdown("#### RapidAPI Key Check")
            key_value = os.getenv("RAPIDAPI_KEY", "").strip()
            host_value = os.getenv("RAPIDAPI_HOST", "live-golf-data.p.rapidapi.com").strip()
            if key_value:
                st.write(
                    f"Key loaded: yes (length {len(key_value)}, last 4: {key_value[-4:]})"
                )
                st.write(f"Host: {host_value}")
            else:
                st.warning("Key loaded: no. Check Streamlit secrets for RAPIDAPI_KEY.")
            if st.button("Test RapidAPI Connection", type="primary"):
                try:
                    _ = rapidapi_fetch_schedule(date.today().year)
                    st.success("RapidAPI connection OK.")
                except Exception as exc:
                    st.error(f"RapidAPI test failed: {exc}")

            st.markdown("#### Results Entry")
            tournaments = conn.execute("SELECT id, name, start_date, end_date FROM tournaments ORDER BY start_date").fetchall()
            golfers = conn.execute("SELECT id, name FROM golfers WHERE active = 1 ORDER BY name").fetchall()
            admin_default_idx = get_current_or_next_tournament_index(tournaments)

            col_a, col_b = st.columns([2, 3])
            with col_a:
                st.markdown("**Single Result**")
                t_name = st.selectbox(
                    "Tournament",
                    [t["name"] for t in tournaments],
                    index=admin_default_idx,
                    key="admin_res_t",
                )
                g_name = st.selectbox("Golfer", [g["name"] for g in golfers], key="admin_res_g")
                purse = st.number_input("Purse (USD)", min_value=0, step=1000, key="admin_res_purse")
                position = st.number_input("Finish position", min_value=1, step=1, key="admin_res_pos")
                if st.button("Save Result", key="admin_res_save", type="primary"):
                    t_id = next(t["id"] for t in tournaments if t["name"] == t_name)
                    g_id = next(g["id"] for g in golfers if g["name"] == g_name)
                    conn.execute(
                        "INSERT INTO results (tournament_id, golfer_id, purse, position) VALUES (?, ?, ?, ?)"
                        " ON CONFLICT(tournament_id, golfer_id) DO UPDATE SET purse = excluded.purse, position = excluded.position",
                        (t_id, g_id, int(purse), int(position)),
                    )
                    conn.commit()
                    persist_results(conn)
                    st.success("Result saved.")

            with col_b:
                st.markdown("**Paste From Clipboard**")
                st.caption("Paste results: \"3 Jake Knapp $400,987\" (position, name, purse). One per line.")
                tournament_for_clip = st.selectbox(
                    "Tournament (for clipboard import)",
                    [t["name"] for t in tournaments],
                    index=admin_default_idx,
                    key="admin_clip_tournament",
                )
                clipboard_text = st.text_area("Paste results text", height=180, key="admin_clip_text")
                if st.button("Preview Clipboard Parse", key="admin_clip_preview", type="primary"):
                    rows, errors = parse_clipboard_results(clipboard_text)
                    st.session_state["admin_clip_rows"] = rows
                    st.session_state["admin_clip_errors"] = errors

                rows = st.session_state.get("admin_clip_rows", [])
                errors = st.session_state.get("admin_clip_errors", [])
                if rows:
                    st.markdown("**Parsed rows preview**")
                    st.dataframe(
                        [
                            {"Golfer": r[0], "Position": r[1] or "—", "Purse": r[2]}
                            for r in rows
                        ],
                        use_container_width=True,
                    )
                if errors:
                    st.warning(f"Skipped {len(errors)} lines (could not parse).")

                if st.button("Import Clipboard Results", key="admin_clip_import", type="primary"):
                    if not rows:
                        st.error("No parsed rows. Click Preview Clipboard Parse first.")
                    else:
                        t = conn.execute(
                            "SELECT id FROM tournaments WHERE name = ?",
                            (tournament_for_clip,),
                        ).fetchone()
                        if not t:
                            st.error("Tournament not found.")
                        else:
                            imported = 0
                            for name, pos, purse_value in rows:
                                golfer = conn.execute(
                                    "SELECT id FROM golfers WHERE name = ?",
                                    (name,),
                                ).fetchone()
                                if not golfer:
                                    continue
                                purse_clean = int(float(str(purse_value).replace("$", "").replace(",", "")))
                                position_value = int(pos) if pos and str(pos).isdigit() else None
                                conn.execute(
                                    "INSERT INTO results (tournament_id, golfer_id, purse, position) VALUES (?, ?, ?, ?)"
                                    " ON CONFLICT(tournament_id, golfer_id) DO UPDATE SET purse = excluded.purse, position = excluded.position",
                                    (t["id"], golfer["id"], purse_clean, position_value),
                                )
                                imported += 1
                            conn.commit()
                            persist_results(conn)
                            st.success(f"Imported {imported} results from clipboard.")

            st.markdown("#### RapidAPI Sync (Live Golf Data)")
            st.caption("Uses RapidAPI live-golf-data: /leaderboard (positions) + /earnings (purse).")
            tourn_rows = conn.execute(
                "SELECT id, name, rapid_tourn_id, start_date, end_date FROM tournaments ORDER BY start_date"
            ).fetchall()
            sync_default_idx = get_current_or_next_tournament_index(tourn_rows)
            missing = [row["name"] for row in tourn_rows if not row["rapid_tourn_id"]]
            if missing:
                with st.expander(f"Missing tournIds ({len(missing)})"):
                    for name in missing:
                        st.write(f"- {name}")
            sync_year = st.number_input("Schedule year", min_value=2000, max_value=2100, value=2026, step=1)
            if st.button("Sync tournIds from schedule", type="primary"):
                try:
                    updated, skipped = sync_tourn_ids_from_rapidapi(conn, int(sync_year))
                    st.success(f"Updated {updated} tournament IDs. Skipped {skipped}.")
                except Exception as exc:
                    st.error(f"Schedule sync failed: {exc}")

            with st.expander("Find tournId by name"):
                search_text = st.text_input("Search schedule (e.g., Phoenix, Pebble, Genesis)")
                if st.button("Search schedule", type="primary"):
                    try:
                        schedule = rapidapi_fetch_schedule(int(sync_year))
                        items = schedule.get("schedule") or schedule.get("tournaments") or schedule.get("data") or []
                        matches = []
                        for item in items:
                            name = item.get("tournament") or item.get("name") or item.get("tournName") or item.get("eventName")
                            if not name:
                                continue
                            if search_text.strip().lower() in name.lower():
                                matches.append(
                                    {
                                        "name": name,
                                        "tournId": item.get("tournId") or item.get("tournamentId") or item.get("id"),
                                        "start": extract_date(
                                            item.get("startDate")
                                            or item.get("start_date")
                                            or item.get("start")
                                            or item.get("startDateUtc")
                                        ),
                                    }
                                )
                        st.session_state["schedule_matches"] = matches
                    except Exception as exc:
                        st.error(f"Schedule search failed: {exc}")

                matches = st.session_state.get("schedule_matches", [])
                if matches:
                    option_labels = [
                        f"{m['name']} (start: {m['start'] or 'n/a'}, tournId: {m['tournId']})" for m in matches
                    ]
                    selected_match = st.selectbox("Matches", option_labels)
                    match_index = option_labels.index(selected_match)
                    match = matches[match_index]
                    assign_tournament = st.selectbox(
                        "Assign to tournament",
                        [row["name"] for row in tourn_rows],
                        key="assign_tournament",
                    )
                    if st.button("Save tournId from match", type="primary"):
                        clean_id = str(match["tournId"]).strip()
                        target = next(row for row in tourn_rows if row["name"] == assign_tournament)
                        conn.execute(
                            "UPDATE tournaments SET rapid_tourn_id = ? WHERE id = ?",
                            (clean_id or None, target["id"]),
                        )
                        conn.commit()
                        st.success(f"Saved tournId {clean_id} for {assign_tournament}.")

            tourn_label = st.selectbox(
                "Tournament to sync",
                [f"{row['name']} (tournId: {row['rapid_tourn_id'] or 'unset'})" for row in tourn_rows],
                index=sync_default_idx,
                key="rapid_tourn_select",
            )
            selected_name = tourn_label.split(" (tournId:", 1)[0]
            selected = next(row for row in tourn_rows if row["name"] == selected_name)
            tourn_id = st.text_input("tournId", value=selected["rapid_tourn_id"] or "")
            year = st.number_input("Earnings year", min_value=2000, max_value=2100, value=2026, step=1)

            col_sync_a, col_sync_b = st.columns([1, 2])
            with col_sync_a:
                if st.button("Save tournId", type="primary"):
                    clean_id = tourn_id.strip()
                    conn.execute(
                        "UPDATE tournaments SET rapid_tourn_id = ? WHERE id = ?",
                        (clean_id or None, selected["id"]),
                    )
                    conn.commit()
                    st.success("tournId saved.")

            with col_sync_b:
                if st.button("Sync Now", type="primary"):
                    attempted_id = tourn_id.strip() or None
                    attempted_year = int(year)
                    try:
                            updated, skipped = sync_results_with_autoresolve(
                                conn,
                                attempted_id,
                                int(attempted_year or year),
                                selected["id"],
                            )
                            if updated > 0:
                                persist_results(conn)
                            st.success(f"Synced {updated} results. Skipped {skipped} names not in roster.")
                    except requests.HTTPError as exc:
                        detail = ""
                        if exc.response is not None:
                            detail = exc.response.text
                        st.error(f"RapidAPI sync failed: {exc} {detail}".strip())
                    except Exception as exc:
                        st.error(f"RapidAPI sync failed: {exc}")

            st.caption("RapidAPI requires your key in `.env` for local use, or Streamlit secrets when deployed.")


if __name__ == "__main__":
    main()
