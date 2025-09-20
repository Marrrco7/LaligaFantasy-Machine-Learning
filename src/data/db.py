from __future__ import annotations

import os
import duckdb
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def get_con(db_url: str | None = None) -> duckdb.DuckDBPyConnection:
    db_url = db_url or os.getenv("DB_URL_DUCKDB", "duckdb:///data/laliga.duckdb")
    if db_url.startswith("duckdb:///"):
        file_path = db_url.replace("duckdb:///", "")
    elif db_url == "duckdb:///:memory:":
        file_path = ":memory:"
    else:
        file_path = db_url

    if file_path != ":memory:":
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    return duckdb.connect(file_path)
