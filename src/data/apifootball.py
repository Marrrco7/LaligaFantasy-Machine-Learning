from __future__ import annotations
from dotenv import load_dotenv, find_dotenv
import os, re, time, requests
from typing import Iterable
from dotenv import load_dotenv
from dateutil import parser as dateparser
from src.data.db import get_con

load_dotenv(find_dotenv(usecwd=True), override=True)

API_KEY = os.getenv("APIFOOTBALL_API_KEY")
if not API_KEY:
    raise RuntimeError("APIFOOTBALL_API_KEY not set. Put it in your .env at project root.")

load_dotenv()
API_KEY = os.getenv("APIFOOTBALL_API_KEY")
BASE = "https://v3.football.api-sports.io"

LALIGA_LEAGUE_ID = 140

HEADERS = {"x-apisports-key": API_KEY}

def _get(url: str, params: dict) -> dict:
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_fixtures(season: int) -> Iterable[dict]:
    page = 1
    while True:
        data = _get(f"{BASE}/fixtures", {
            "league": LALIGA_LEAGUE_ID,
            "season": season,
            "page": page
        })
        for item in data.get("response", []):
            yield item
        paging = data.get("paging", {}) or {}
        if page >= int(paging.get("total", 1)):
            break
        page += 1
        time.sleep(0.2)

def _parse_round_to_jornada(round_str: str | None) -> int | None:
    if not round_str:
        return None
    m = re.search(r"(\d+)", round_str)
    return int(m.group(1)) if m else None

def upsert_teams_from_fixtures(fixtures: list[dict]) -> None:
    con = get_con()
    rows = []
    for fx in fixtures:
        h = fx["teams"]["home"]; a = fx["teams"]["away"]
        rows.append((h["id"], h["name"], h.get("name", "")[:3].upper()))
        rows.append((a["id"], a["name"], a.get("name", "")[:3].upper()))
    rows = list({r[0]: r for r in rows}.values())
    con.execute("CREATE TABLE IF NOT EXISTS teams(team_id INTEGER PRIMARY KEY, name TEXT, short_name TEXT)")
    con.executemany("INSERT OR REPLACE INTO teams(team_id,name,short_name) VALUES (?,?,?)", rows)

def write_matches(fixtures: list[dict], season: int) -> None:
    con = get_con()
    con.execute("""
    CREATE TABLE IF NOT EXISTS matches(
      match_id BIGINT PRIMARY KEY,
      season INTEGER,
      jornada INTEGER,
      date_utc TIMESTAMP,
      home_team_id INTEGER,
      away_team_id INTEGER,
      home_goals INTEGER,
      away_goals INTEGER,
      status TEXT,
      venue TEXT
    );
    """)
    rows = []
    for fx in fixtures:
        fid = fx["fixture"]["id"]
        dt = dateparser.parse(fx["fixture"]["date"]).replace(tzinfo=None)  # store naive UTC
        venue = (fx["fixture"].get("venue") or {}).get("name")
        status = fx["fixture"]["status"]["short"]  # e.g. "NS","1H","FT"
        jornada = _parse_round_to_jornada(fx["league"].get("round"))
        home_id = fx["teams"]["home"]["id"]
        away_id = fx["teams"]["away"]["id"]
        goals = fx.get("goals") or {}
        hg = goals.get("home")
        ag = goals.get("away")
        rows.append((fid, season, jornada, dt, home_id, away_id, hg, ag, status, venue))
    con.executemany("""
      INSERT OR REPLACE INTO matches
      (match_id,season,jornada,date_utc,home_team_id,away_team_id,home_goals,away_goals,status,venue)
      VALUES (?,?,?,?,?,?,?,?,?,?)
    """, rows)


def _get(url: str, params: dict) -> dict:
    if not API_KEY:
        raise RuntimeError("APIFOOTBALL_API_KEY not set. Put it in your .env")
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    try:
        data = r.json()
    except Exception:
        r.raise_for_status()
        raise
    if r.status_code != 200:
        raise RuntimeError(f"API error {r.status_code}: {data}")
    if data.get("errors"):
        raise RuntimeError(f"API payload errors: {data['errors']} params={params}")
    return data

def fetch_fixtures(season: int):
    data = _get(f"{BASE}/fixtures", {"league": LALIGA_LEAGUE_ID, "season": season})
    resp = data.get("response", []) or []
    paging = data.get("paging", {}) or {}
    total_pages = int(paging.get("total", 1) or 1)
    print(f"[fixtures] season={season} page=1 results={len(resp)} total_pages={total_pages}")
    for item in resp:
        yield item

    for page in range(2, total_pages + 1):
        data = _get(f"{BASE}/fixtures", {
            "league": LALIGA_LEAGUE_ID,
            "season": season,
            "page": page
        })
        resp = data.get("response", []) or []
        print(f"[fixtures] season={season} page={page} results={len(resp)}")
        for item in resp:
            yield item
        time.sleep(0.2)

def fetch_fixture_player_stats(fixture_id: int) -> list[dict]:
    out = []
    data = _get(f"{BASE}/fixtures/players", {"fixture": fixture_id})
    out.extend(data.get("response", []) or [])
    paging = data.get("paging", {}) or {}
    total_pages = int(paging.get("total", 1) or 1)

    for page in range(2, total_pages + 1):
        time.sleep(6.5)
        data = _get(f"{BASE}/fixtures/players", {"fixture": fixture_id, "page": page})
        out.extend(data.get("response", []) or [])
    return out


def write_lineups_from_players_response(fixture_id: int, payload: list[dict]) -> None:
    upsert_players_from_players_response(payload)
    con = get_con()
    con.execute("""
    CREATE TABLE IF NOT EXISTS lineups(
      match_id BIGINT,
      player_id INTEGER,
      started BOOLEAN,
      minutes INTEGER,
      sub_in_min INTEGER,
      sub_out_min INTEGER,
      PRIMARY KEY (match_id, player_id)
    );
    """)
    rows = []
    for team_block in payload:
        for p in team_block.get("players", []):
            pid = (p.get("player") or {}).get("id")
            stats_list = p.get("statistics") or []
            if not pid or not stats_list:
                continue
            st = stats_list[0].get("games") or {}
            mins = st.get("minutes")
            substitute = st.get("substitute", None)
            started = None
            if mins is not None:
                if substitute is True:
                    started = False
                elif substitute is False:
                    started = True
                else:
                    started = mins >= 60
            rows.append((fixture_id, pid, started, mins, None, None))
    if rows:
        con.executemany("""
          INSERT OR REPLACE INTO lineups(match_id, player_id, started, minutes, sub_in_min, sub_out_min)
          VALUES (?,?,?,?,?,?)
        """, rows)

def get_fixture_ids_for_seasons(seasons: list[int]) -> list[int]:
    con = get_con()
    q = f"""
      SELECT match_id FROM matches
      WHERE season IN ({",".join(str(s) for s in seasons)})
      ORDER BY match_id
    """
    return [r[0] for r in con.execute(q).fetchall()]


def upsert_players_from_players_response(payload: list[dict]) -> None:
    con = get_con()
    con.execute("""
    CREATE TABLE IF NOT EXISTS players(
      player_id INTEGER PRIMARY KEY,
      team_id INTEGER,
      name TEXT,
      position TEXT,
      foot TEXT,
      birthdate DATE
    );
    """)
    rows = []
    for team_block in payload:
        team_id = (team_block.get("team") or {}).get("id")
        for p in team_block.get("players", []):
            pd = p.get("player") or {}
            pid = pd.get("id")
            name = pd.get("name")
            stats_list = p.get("statistics") or []
            pos = (stats_list[0].get("games") or {}).get("position") if stats_list else None
            if pid and team_id and name:
                rows.append((pid, team_id, name, pos))
    if rows:
        rows = list({r[0]: r for r in rows}.values())
        con.executemany("""
          INSERT OR REPLACE INTO players(player_id, team_id, name, position, foot, birthdate)
          VALUES (?,?,?,?,NULL,NULL)
        """, rows)
