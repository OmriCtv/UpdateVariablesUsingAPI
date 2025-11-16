import csv
import json
import os
from pathlib import Path
from typing import Iterable, Iterator

import requests


CSV_FILE = Path("_1ec01f0כרטיס מכשיר(DataSheet).csv")
API_BASE_URL = "https://m4d-srv.ctv.co.il/media4display-api"
OUTPUT_FILE = Path("missing_player_attributes.csv")


def iter_csv_rows(path: Path, encodings: Iterable[str]) -> Iterator[list[str]]:
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


def get_api_credentials() -> tuple[str, str]:
    """
    Prompt the user for the API key and organization (same as in update_player_city.py).
    """
    api_key = input("Enter API key: ").strip()
    if not api_key:
        raise RuntimeError("API key is required.")

    organization = os.environ.get("M4D_ORG")
    if not organization:
        organization = input("Enter organization: ").strip()
    if not organization:
        raise RuntimeError("Organization is required.")

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


def fetch_player(token: str, player_id: int) -> dict:
    resp = requests.get(
        f"{API_BASE_URL}/v1/players/{player_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def filter_players_by_site(players: Iterable[dict], site_id: str) -> list[dict]:
    """
    Reuse the same matching logic as update_player_city.py:
    site id appears in identifier or name.
    """
    return [
        player
        for player in players
        if site_id in (player.get("identifier", "") or "")
        or site_id in (player.get("name", "") or "")
    ]


def player_has_missing_attributes(player: dict) -> bool:
    """
    Check if any of the required attributes are missing on a single player:
      - coordinates.city
      - M4DS_Reseller
      - M4DS_ISP
      - Streaming*Muted variables (4 of them)
    """
    coords = player.get("coordinates") or {}
    city = coords.get("city")
    if not city:
        return True

    vars_raw = player.get("variables") or []
    if isinstance(vars_raw, dict):
        vars_list = [vars_raw]
    elif isinstance(vars_raw, list):
        vars_list = vars_raw
    else:
        vars_list = []

    def get_var(name: str) -> str | None:
        for item in vars_list:
            if not isinstance(item, dict):
                continue
            if item.get("name") == name:
                return item.get("value")
        return None

    reseller = get_var("M4DS_Reseller")
    isp = get_var("M4DS_ISP")
    if not reseller or not isp:
        return True

    streaming_names = [
        "M4DS_StreamingHot_Muted",
        "M4DS_StreamingTriple_Muted",
        "M4DS_StreamingVerticalHot_Muted",
        "M4DS_StreamingVerticalTriple_Muted",
    ]
    for name in streaming_names:
        val = get_var(name)
        if val is None or val == "":
            return True

    return False


def audit_site(
    site_id: str,
    players_cache: list[dict],
    api_key: str,
    organization: str,
) -> bool:
    """
    Return True if this site_id has at least one player with missing attributes.
    """
    token = request_token(api_key, organization)
    target_players = filter_players_by_site(players_cache, site_id)

    if not target_players:
        print(f"[INFO] No players found for site {site_id}.")
        return False

    print(f"[INFO] Found {len(target_players)} player(s) for site {site_id}. Checking...")

    for player in target_players:
        player_id = player.get("playerId") or player.get("id")
        identifier = (player.get("identifier") or player.get("name", "")).strip()
        if player_id is None:
            print(f"[WARN] Skipping player without id (site {site_id}, identifier {identifier!r})")
            continue

        try:
            token = request_token(api_key, organization)  # fresh token per player
            full_player = fetch_player(token, int(player_id))
            if player_has_missing_attributes(full_player):
                print(
                    f"[INFO] Site {site_id}: player {identifier} (id {player_id}) "
                    f"has missing attributes."
                )
                return True
        except Exception as exc:
            print(
                f"[WARN] Failed to check player {identifier} (id {player_id}) for site {site_id}: {exc}"
            )

    return False


def main() -> None:
    if not CSV_FILE.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")

    api_key, organization = get_api_credentials()

    # Pre-fetch players once to avoid re-listing for every site.
    token = request_token(api_key, organization)
    players_cache = fetch_players(token)

    encodings = ("utf-8-sig", "utf-8", "windows-1255", "cp1255", "iso-8859-8", "latin1")
    rows = iter_csv_rows(CSV_FILE, encodings)
    header = next(rows)

    try:
        site_idx = header.index("מספר אתר/תאור אתר")
    except ValueError as exc:
        raise RuntimeError("Column 'מספר אתר/תאור אתר' not found in CSV header.") from exc

    seen_sites: set[str] = set()
    sites_with_missing: list[str] = []

    for row in rows:
        if len(row) <= site_idx:
            continue
        site_id = row[site_idx].strip()
        if not site_id or site_id in seen_sites:
            continue
        seen_sites.add(site_id)

        if audit_site(site_id, players_cache, api_key, organization):
            sites_with_missing.append(site_id)

    # Write results CSV: one row per site_id with missing attributes.
    with OUTPUT_FILE.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["מספר אתר/תאור אתר"])
        for site_id in sites_with_missing:
            writer.writerow([site_id])

    print(
        f"\n[INFO] Finished audit. Found {len(sites_with_missing)} site(s) with missing attributes."
    )
    print(f"[INFO] Results written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()


