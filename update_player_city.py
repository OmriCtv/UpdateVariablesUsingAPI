import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Iterable, Tuple

import requests


CSV_FILE = Path("_1ec01f0כרטיס מכשיר(DataSheet).csv")
DICT_FILE = Path("dictionaries.json")
API_BASE_URL = "https://m4d-srv.ctv.co.il/media4display-api"


def prompt_site_id() -> str:
    site_id = input("Enter מספר אתר/תאור אתר (site id): ").strip()
    if not site_id:
        raise ValueError("No site id provided.")
    return site_id


def iter_csv_rows(path: Path, encodings: Iterable[str]) -> Iterable[list[str]]:
    for encoding in encodings:
        try:
            with path.open("r", encoding=encoding, newline="") as fh:
                reader = csv.reader(fh)
                header = next(reader, None)
                if header is None:
                    continue
                yield header
                for row in reader:
                    yield row
                return
        except Exception:
            continue
    raise RuntimeError(f"Unable to read CSV file {path} with provided encodings.")


def find_site_city(site_id: str) -> str:
    encodings = ("utf-8-sig", "utf-8", "windows-1255", "cp1255", "iso-8859-8", "latin1")
    rows = iter_csv_rows(CSV_FILE, encodings)
    header = next(rows)

    try:
        site_idx = header.index("מספר אתר/תאור אתר")
        city_idx = header.index("עיר האתר")
    except ValueError as exc:
        raise RuntimeError("Expected columns not found in CSV header.") from exc

    for row in rows:
        if len(row) <= max(site_idx, city_idx):
            continue
        if row[site_idx] == site_id:
            city = row[city_idx].replace("\u00a0", " ").strip()
            if not city:
                raise RuntimeError(f"City column empty for site {site_id}.")
            return city

    raise RuntimeError(f"Site id {site_id} not found in CSV.")


def load_dictionaries() -> dict:
    with DICT_FILE.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def translate_city(city_he: str, dictionaries: dict) -> str:
    cities = dictionaries.get("cities_dictionary", {})
    for key, value in cities.items():
        if key.replace("\u00a0", " ").strip() == city_he:
            return value
    raise RuntimeError(f"City '{city_he}' not found in cities_dictionary.")


def get_api_credentials() -> Tuple[str, str]:
    api_key = os.environ.get("M4D_API_KEY")
    organization = os.environ.get("M4D_ORG")
    if not api_key or not organization:
        raise RuntimeError(
            "API key and organization are required. "
            "Set environment variables M4D_API_KEY and M4D_ORG."
        )
    return api_key, organization


def request_token(api_key: str, organization: str) -> str:
    resp = requests.post(
        f"{API_BASE_URL}/v1/token",
        json={"apiKey": api_key, "organization": organization},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text.strip('"')


def fetch_players(token: str) -> list[dict]:
    resp = requests.get(
        f"{API_BASE_URL}/v1/players",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def filter_players(players: Iterable[dict], site_id: str) -> list[dict]:
    return [
        player
        for player in players
        if site_id in (player.get("identifier", "") or "")
        or site_id in (player.get("name", "") or "")
    ]


def fetch_player(token: str, player_id: int) -> dict:
    resp = requests.get(
        f"{API_BASE_URL}/v1/players/{player_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def patch_player_city(token: str, player_id: int, city_en: str) -> None:
    resp = requests.patch(
        f"{API_BASE_URL}/v1/players/{player_id}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json-patch+json",
        },
        json=[{"op": "replace", "path": "/coordinates/city", "value": city_en}],
        timeout=60,
    )
    resp.raise_for_status()


def main() -> None:
    if not CSV_FILE.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")
    if not DICT_FILE.exists():
        raise FileNotFoundError(f"Dictionaries file not found: {DICT_FILE}")

    site_id = prompt_site_id()

    city_he = find_site_city(site_id)
    dictionaries = load_dictionaries()
    try:
        city_en = translate_city(city_he, dictionaries)
    except RuntimeError as exc:
        print(f"[ERROR] {exc}")
        return 1

    print(f"Site {site_id}: Hebrew city '{city_he}' -> English '{city_en}'")

    api_key, organization = get_api_credentials()

    token = request_token(api_key, organization)
    players = fetch_players(token)
    target_players = filter_players(players, site_id)

    if not target_players:
        print(f"No players found containing '{site_id}'. Nothing to update.")
        return 0

    print(f"Found {len(target_players)} player(s) matching site {site_id}.")

    for player in target_players:
        player_id = player.get("playerId") or player.get("id")
        identifier = player.get("identifier") or player.get("name", "")
        if player_id is None:
            print(f"Skipping player without id: {identifier}")
            continue

        try:
            print(f"\nProcessing player {identifier} (id {player_id})")
            token = request_token(api_key, organization)  # fresh token per player
            current = fetch_player(token, player_id)
            current_city = (current.get("coordinates") or {}).get("city")
            print(f"  Current city: {current_city!r}")

            patch_player_city(token, player_id, city_en)
            token = request_token(api_key, organization)
            updated = fetch_player(token, player_id)
            new_city = (updated.get("coordinates") or {}).get("city")
            print(f"  Updated city: {new_city!r}")
        except requests.HTTPError as exc:
            print(f"  [ERROR] API call failed: {exc}")
        except Exception as exc:
            print(f"  [ERROR] Unexpected error: {exc}")

    return 0


if __name__ == "__main__":
    exit_code = 0
    try:
        exit_code = main()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        exit_code = 1
    finally:
        try:
            print("\nExiting in 6 seconds...")
            time.sleep(6)
        except KeyboardInterrupt:
            pass
    sys.exit(exit_code)

