from pathlib import Path
from src.data.db import get_con

SCHEMA_PATH = Path(__file__).parents[2] / "configs" / "schema.sql"

def init_db():
    con = get_con()

    sql = SCHEMA_PATH.read_text(encoding="utf-8")

if __name__ == "__main__":
    init_db()
