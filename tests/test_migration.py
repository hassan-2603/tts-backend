"""Test SQLite to PostgreSQL migration utility."""
import os
import sqlite3
import tempfile
import pytest
from app.database import set_engine_url
from app.migration import run_sqlite_migration

@pytest.mark.asyncio
async def test_sqlite_migration():
    # Configure test database to use SQLite
    test_target_db = tempfile.mktemp(suffix=".db")
    set_engine_url(f"sqlite+aiosqlite:///{test_target_db}")

    # Create temporary SQLite DB with legacy document structure
    temp_sqlite = tempfile.mktemp(suffix=".db")
    con = sqlite3.connect(temp_sqlite)
    cur = con.cursor()

    cur.execute("CREATE TABLE users (id TEXT PRIMARY KEY, doc TEXT)")
    cur.execute("CREATE TABLE traders (id TEXT PRIMARY KEY, doc TEXT)")
    cur.execute("CREATE TABLE accounts (id TEXT PRIMARY KEY, doc TEXT)")
    cur.execute("CREATE TABLE trades (id TEXT PRIMARY KEY, doc TEXT)")

    cur.execute("INSERT INTO users VALUES (?, ?)", ("admin", '{"user": "admin", "role": "admin", "pw": "salt$hash"}'))
    cur.execute("INSERT INTO traders VALUES (?, ?)", ("TR-101", '{"name": "Trader Test", "status": "Testing", "testCapital": 500000}'))
    cur.execute("INSERT INTO accounts VALUES (?, ?)", ("AC-101", '{"name": "Acc Test", "broker": "Zebu", "trader": "TR-101", "zebu_uid": "U101"}'))
    cur.execute("INSERT INTO trades VALUES (?, ?)", ("T-1", '{"id": "T-1", "origin": "zebu", "account": "AC-101", "trader": "TR-101", "date": "2026-11-10", "time": "10:15", "symbol": "BANKNIFTY", "strike": 51500, "opt": "CE", "side": "BUY", "lots": 2, "lotSize": 30, "qty": 60, "entry": 300, "exit": 350, "open": false, "charges": 55.0}'))

    con.commit()
    con.close()

    # Run migration
    report = await run_sqlite_migration(temp_sqlite)

    assert report.status == "SUCCESS"
    assert report.migrated_counts["users"] == 1
    assert report.migrated_counts["traders"] == 1
    assert report.migrated_counts["accounts"] == 1
    assert report.migrated_counts["trades"] == 1
    assert os.path.exists(report.backup_path)

    # Clean up temp files
    try:
        os.remove(temp_sqlite)
        os.remove(report.backup_path)
    except Exception:
        pass
