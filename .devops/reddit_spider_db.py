from pathlib import Path
from omegaconf import OmegaConf as Conf
import sqlite3

from reddit_spider_ds import *
from reddit_spider_util import *

# =================================================================================================

TABLE_NAME_ASSET = "asset"
TABLE_NAME_USER = "user"
TABLE_NAME_SPACE = "space"
TABLE_NAME_POST = "post"

# =================================================================================================


def connect_database(db_file, rebuild=False):
    db_file = Path(db_file)
    if rebuild:
        db_file.unlink(missing_ok=True)
    db_con = sqlite3.connect(db_file)
    db_cur = db_con.cursor()
    ensure_database(db_cur)
    db_con.commit()
    db_cur.close()
    return db_con


def ensure_database(db_cur):
    ensure_table_asset(db_cur)
    ensure_table_user(db_cur)
    ensure_table_space(db_cur)
    ensure_table_post(db_cur)


# =================================================================================================
# ASSET


def ensure_table_asset(db_cur):
    db_cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME_ASSET} (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            utc_created TEXT,
            utc_updated TEXT,
            media_type  TEXT,
            media_url   TEXT,
            media_data  BLOB,
        )
    """
    )


def insert_asset(
    db_cur,
    record: AssetRecord,
):
    record.utc_created = get_timestamp_utc()
    record.utc_updated = record.utc_created
    db_cur.execute(
        f"""
        INSERT OR IGNORE INTO {TABLE_NAME_ASSET} (
            utc_created, utc_updated,
            media_type, media_url, media_data
        )
        VALUES (?, ?, ?, ?, ?)
    """,
        (
            record.utc_created,
            record.utc_updated,
            record.media_type,
            record.media_url,
            record.media_data,
        ),
    )


# =================================================================================================
# USER


def ensure_table_user(db_cur):
    db_cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME_USER} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            utc_created TEXT,
            utc_updated TEXT,
            src_slug    TEXT,
            src_url     TEXT,
            src_data    TEXT,
        )
    """
    )


def insert_user(db_cur, record: UserRecord):
    record.utc_created = get_timestamp_utc()
    record.utc_updated = record.utc_created
    db_cur.execute(
        f"""
        INSERT OR IGNORE INTO {TABLE_NAME_USER} (
            utc_created, utc_updated,
            src_slug, src_url, src_data
        )
        VALUES (?, ?, ?, ?, ?)
    """,
        (
            record.utc_created,
            record.utc_updated,
            record.src_slug,
            record.src_url,
            record.src_data,
        ),
    )


# =================================================================================================
# SPACE


def ensure_table_space(db_cur):
    db_cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME_SPACE} (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            utc_created TEXT,
            utc_updated TEXT,
            src_slug    TEXT,
            src_url     TEXT,
            src_data    TEXT,
        )
    """
    )


def insert_space(db_cur, record: SpaceRecord):
    record.utc_created = get_timestamp_utc()
    record.utc_updated = record.utc_created
    db_cur.execute(
        f"""
        INSERT OR IGNORE INTO {TABLE_NAME_SPACE} (
            utc_created, utc_updated,
            src_slug, src_url, src_data
        )
        VALUES (?, ?, ?, ?)
    """,
        (
            record.utc_created,
            record.utc_updated,
            record.src_slug,
            record.src_url,
            record.src_data,
        ),
    )


# =================================================================================================
# POST


def ensure_table_post(db_cur):
    db_cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME_POST} (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            utc_created TEXT,
            utc_updated TEXT,
            src_slug    TEXT,
            src_url     TEXT,
            src_data    TEXT,
            space       TEXT,
            title       TEXT,
            author      TEXT,
            type        TEXT,
            text        TEXT,
            score       INTEGER,
            parent      TEXT,
        )
    """
    )


def insert_post(db_cur, record: PostRecord):
    record.utc_created = get_timestamp_utc()
    record.utc_updated = record.utc_created
    db_cur.execute(
        f"""
        INSERT OR IGNORE INTO {TABLE_NAME_POST} (
            utc_created, utc_updated,
            src_slug, src_url, src_data, space, title, author, type, text, score, parent
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        (
            record.utc_created,
            record.utc_updated,
            record.src_slug,
            record.src_url,
            record.src_data,
            record.space,
            record.title,
            record.author,
            record.type,
            record.text,
            record.score,
            record.parent,
        ),
    )
