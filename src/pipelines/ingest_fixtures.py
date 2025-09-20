import sys
from src.data.apifootball import fetch_fixtures, upsert_teams_from_fixtures, write_matches

def main(seasons):
    if isinstance(seasons, int):
        seasons = [seasons]
    for season in seasons:
        print(f"=== Ingesting LaLiga fixtures for season {season} ===")
        fixtures = list(fetch_fixtures(season))
        if not fixtures:
            print(f"No fixtures returned for season {season}. (Check plan/season availability)")
            continue
        upsert_teams_from_fixtures(fixtures)
        write_matches(fixtures, season)
        print(f"Ingested {len(fixtures)} fixtures for season {season}.\n")

if __name__ == "__main__":
    # usage:
    #   python -m src.pipelines.ingest_fixtures 2021 2022 2023
    #   python -m src.pipelines.ingest_fixtures 2023
    seasons = [int(x) for x in sys.argv[1:]] or [2023]
    main(seasons)
