import sys, time
from src.data.apifootball import (
    get_fixture_ids_for_seasons,
    fetch_fixture_player_stats,
    write_lineups_from_players_response,
)

def main(seasons: list[int]):
    fixture_ids = get_fixture_ids_for_seasons(seasons)
    print(f"Found {len(fixture_ids)} fixtures in DB for seasons {seasons}")
    count = 0
    for i, fid in enumerate(fixture_ids, 1):
        try:
            payload = fetch_fixture_player_stats(fid)
            write_lineups_from_players_response(fid, payload)
            count += 1
            if i % 10 == 0:
                print(f"Processed {i}/{len(fixture_ids)} fixtures...")
        except Exception as e:
            print(f"[WARN] fixture {fid}: {e}")
        time.sleep(6.5)
    print(f"Done. Lineups/minutes written for {count} fixtures.")

if __name__ == "__main__":
    seasons = [int(x) for x in sys.argv[1:]] or [2023]
    main(seasons)
