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
    site_id = input("Enter site number (without computer number or ISP - like 200010 IPS): ").strip()
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
    """
    Translate Hebrew city name to English code using cities_dictionary.
    """
    cities = dictionaries.get("cities_dictionary", {})
    for key, value in cities.items():
        if key.replace("\u00a0", " ").strip() == city_he:
            return value
    raise RuntimeError(f"City '{city_he}' not found in cities_dictionary.")


def translate_reseller(reseller_he: str, dictionaries: dict) -> str:
    """
    Translate Hebrew reseller name to English code using reseller_dictionary.
    """
    resellers = dictionaries.get("reseller_dictionary", {})
    for key, value in resellers.items():
        if key.replace("\u00a0", " ").strip() == reseller_he:
            return value
    raise RuntimeError(f"Reseller '{reseller_he}' not found in reseller_dictionary.")


def translate_isp(isp_he: str, dictionaries: dict) -> str:
    """
    Translate Hebrew ISP name to English code using ISP_dictionary.
    """
    isps = dictionaries.get("ISP_dictionary", {})
    for key, value in isps.items():
        if key.replace("\u00a0", " ").strip() == isp_he:
            return value
    raise RuntimeError(f"ISP '{isp_he}' not found in ISP_dictionary.")


def find_site_reseller(site_id: str) -> str:
    """
    Look up 'תאור משווק' (reseller description) for the given site id in the CSV.
    """
    encodings = ("utf-8-sig", "utf-8", "windows-1255", "cp1255", "iso-8859-8", "latin1")
    rows = iter_csv_rows(CSV_FILE, encodings)
    header = next(rows)

    try:
        site_idx = header.index("מספר אתר/תאור אתר")
        reseller_idx = header.index("תאור משווק")
    except ValueError as exc:
        raise RuntimeError("Expected columns not found in CSV header.") from exc

    for row in rows:
        if len(row) <= max(site_idx, reseller_idx):
            continue
        if row[site_idx] == site_id:
            reseller = row[reseller_idx].replace("\u00a0", " ").strip()
            if not reseller:
                raise RuntimeError(f"Reseller column empty for site {site_id}.")
            return reseller

    raise RuntimeError(f"Site id {site_id} not found in CSV.")


def find_site_isp(site_id: str) -> str:
    """
    Look up 'ספק תקשורת' (ISP) for the given site id in the CSV.
    """
    encodings = ("utf-8-sig", "utf-8", "windows-1255", "cp1255", "iso-8859-8", "latin1")
    rows = iter_csv_rows(CSV_FILE, encodings)
    header = next(rows)

    try:
        site_idx = header.index("מספר אתר/תאור אתר")
        isp_idx = header.index("ספק תקשורת")
    except ValueError as exc:
        raise RuntimeError("Expected columns not found in CSV header.") from exc

    for row in rows:
        if len(row) <= max(site_idx, isp_idx):
            continue
        if row[site_idx] == site_id:
            isp = row[isp_idx].replace("\u00a0", " ").strip()
            if not isp:
                raise RuntimeError(f"ISP column empty for site {site_id}.")
            return isp

    raise RuntimeError(f"Site id {site_id} not found in CSV.")


def get_api_credentials() -> Tuple[str, str]:
    """
    Prompt the user for the API key (instead of using a hardcoded value or env var)
    and obtain the organization (from env if available, otherwise via prompt).
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


def set_player_reseller(token: str, player_id: int, reseller_en: str) -> None:
    """
    Set the M4DS_Reseller variable for a player using:
      POST /v1/players/{id}/variables
    as shown in the Swagger screenshot.
    """
    if not reseller_en:
        return

    payload = [{"name": "M4DS_Reseller", "value": reseller_en}]

    print(f"  Reseller POST body for player {player_id}: {json.dumps(payload, ensure_ascii=False)}")

    resp = requests.post(
        f"{API_BASE_URL}/v1/players/{player_id}/variables",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json-patch+json",
        },
        json=payload,
        timeout=60,
    )
    if resp.status_code >= 400:
        try:
            print(f"  Reseller POST error body: {resp.text}")
        except Exception:
            pass
    resp.raise_for_status()


def set_player_isp(token: str, player_id: int, isp_en: str) -> None:
    """
    Set the M4DS_ISP variable for a player using:
      POST /v1/players/{id}/variables
    """
    if not isp_en:
        return

    payload = [{"name": "M4DS_ISP", "value": isp_en}]

    print(f"  ISP POST body for player {player_id}: {json.dumps(payload, ensure_ascii=False)}")

    resp = requests.post(
        f"{API_BASE_URL}/v1/players/{player_id}/variables",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json-patch+json",
        },
        json=payload,
        timeout=60,
    )
    if resp.status_code >= 400:
        try:
            print(f"  ISP POST error body: {resp.text}")
        except Exception:
            pass
    resp.raise_for_status()


def set_player_streaming_flags(
    token: str,
    player_id: int,
    identifier: str,
    *,
    any_combined: bool,
    any_lh_only: bool,
    any_pv_only: bool,
    is_combined: bool,
    is_lh_only: bool,
    is_pv_only: bool,
) -> None:
    """
    Set streaming-muted variables for the given player according to the rules:

    Case A – LH-only and PV-only players, no combined (PV_LH/LH_PV):
      - LH-only player(s): one non-vertical flag true (by -H/-T), others false.
      - PV-only player(s): all false.

    Case B – combined player(s) AND a single "alone" type (LH-only or PV-only):
      - Alone player(s): all false.
      - Combined player(s):
          * if alone is LH-only  -> vertical flag true (by -H/-T), others false.
          * if alone is PV-only  -> non-vertical flag true (by -H/-T), others false.

    Case C – only combined player(s), no alone LH/PV:
      - Combined player(s): vertical flag true (by -H/-T), others false.
    """
    id_upper = identifier.upper()

    # Debug context
    print(
        f"  Streaming flags decision for '{identifier}': "
        f"any_combined={any_combined}, any_lh_only={any_lh_only}, any_pv_only={any_pv_only}, "
        f"is_combined={is_combined}, is_lh_only={is_lh_only}, is_pv_only={is_pv_only}"
    )

    # Start with all false
    payload: dict[str, str] = {
        "M4DS_StreamingHot_Muted": "false",
        "M4DS_StreamingTriple_Muted": "false",
        "M4DS_StreamingVerticalHot_Muted": "false",
        "M4DS_StreamingVerticalTriple_Muted": "false",
    }

    ends_with_h = id_upper.endswith("-H")
    ends_with_t = id_upper.endswith("-T")

    if any_combined:
        # Cases B and C – there is at least one combined player in this number.
        if is_combined:
            # Determine alone type, if any.
            alone_is_lh = any_lh_only and not any_pv_only
            alone_is_pv = any_pv_only and not any_lh_only

            if alone_is_lh:
                # Combined gets vertical flag true.
                if ends_with_h:
                    payload["M4DS_StreamingVerticalHot_Muted"] = "true"
                elif ends_with_t:
                    payload["M4DS_StreamingVerticalTriple_Muted"] = "true"
            elif alone_is_pv:
                # Combined gets non-vertical flag true.
                if ends_with_h:
                    payload["M4DS_StreamingHot_Muted"] = "true"
                elif ends_with_t:
                    payload["M4DS_StreamingTriple_Muted"] = "true"
            else:
                # Only combined players and no alone LH/PV → treat like vertical true.
                if ends_with_h:
                    payload["M4DS_StreamingVerticalHot_Muted"] = "true"
                elif ends_with_t:
                    payload["M4DS_StreamingVerticalTriple_Muted"] = "true"
        else:
            # Alone players when a combined exists → always all false.
            pass
    else:
        # Case A – no combined players for this number.
        if any_lh_only and any_pv_only and is_lh_only:
            # Only LH-only players get non-vertical true.
            if ends_with_h:
                payload["M4DS_StreamingHot_Muted"] = "true"
            elif ends_with_t:
                payload["M4DS_StreamingTriple_Muted"] = "true"

    body = [{"name": name, "value": value} for name, value in payload.items()]

    print(
        f"  Streaming muted POST body for player {player_id}: "
        f"{json.dumps(body, ensure_ascii=False)}"
    )

    resp = requests.post(
        f"{API_BASE_URL}/v1/players/{player_id}/variables",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json-patch+json",
        },
        json=body,
        timeout=60,
    )
    # Log response body for debugging regardless of status
    try:
        print(f"  Streaming muted POST status: {resp.status_code}, body: {resp.text}")
    except Exception:
        pass
    resp.raise_for_status()


def main() -> None:
    if not CSV_FILE.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")
    if not DICT_FILE.exists():
        raise FileNotFoundError(f"Dictionaries file not found: {DICT_FILE}")

    site_id = prompt_site_id()

    city_he = find_site_city(site_id)
    reseller_he = find_site_reseller(site_id)
    isp_he = find_site_isp(site_id)
    dictionaries = load_dictionaries()
    try:
        city_en = translate_city(city_he, dictionaries)
        reseller_en = translate_reseller(reseller_he, dictionaries)
        isp_en = translate_isp(isp_he, dictionaries)
    except RuntimeError as exc:
        print(f"[ERROR] {exc}")
        return 1

    print(
        f"Site {site_id}: Hebrew city '{city_he}' -> English '{city_en}', "
        f"reseller '{reseller_he}' -> '{reseller_en}', "
        f"ISP '{isp_he}' -> '{isp_en}'"
    )

    api_key, organization = get_api_credentials()

    token = request_token(api_key, organization)
    players = fetch_players(token)
    target_players = filter_players(players, site_id)

    if not target_players:
        print(f"No players found containing '{site_id}'. Nothing to update.")
        return 0

    print(f"Found {len(target_players)} player(s) matching site {site_id}.")

    # Determine, within this site/number, whether there are LH-only, PV-only
    # and/or combined (PV_LH/LH_PV) players.
    any_lh_only = False
    any_pv_only = False
    any_combined = False
    for p in target_players:
        ident_raw = (p.get("identifier") or p.get("name") or "").strip()
        ident = ident_raw.upper()
        has_lh = "LH" in ident
        has_pv = "PV" in ident
        is_combined = "PV_LH" in ident or "LH_PV" in ident
        if is_combined:
            any_combined = True
        elif has_lh and not has_pv:
            any_lh_only = True
        elif has_pv and not has_lh:
            any_pv_only = True

    for player in target_players:
        player_id = player.get("playerId") or player.get("id")
        identifier = (player.get("identifier") or player.get("name", "")).strip()
        if player_id is None:
            print(f"Skipping player without id: {identifier}")
            continue

        try:
            print(f"\nProcessing player {identifier} (id {player_id})")
            token = request_token(api_key, organization)  # fresh token per player
            current = fetch_player(token, player_id)

            # Current city
            current_city = (current.get("coordinates") or {}).get("city")

            # Current reseller from variables list (name == "M4DS_Reseller")
            vars_raw = current.get("variables") or []
            if isinstance(vars_raw, dict):
                vars_list = [vars_raw]
            elif isinstance(vars_raw, list):
                vars_list = vars_raw
            else:
                vars_list = []

            current_reseller = None
            current_isp = None
            current_stream_hot = None
            current_stream_triple = None
            current_stream_vert_hot = None
            current_stream_vert_triple = None
            reseller_index: int | None = None
            for idx, item in enumerate(vars_list):
                if not isinstance(item, dict):
                    continue
                if item.get("name") == "M4DS_Reseller":
                    reseller_index = idx
                    current_reseller = item.get("value")
                if item.get("name") == "M4DS_ISP":
                    current_isp = item.get("value")
                if item.get("name") == "M4DS_StreamingHot_Muted":
                    current_stream_hot = item.get("value")
                if item.get("name") == "M4DS_StreamingTriple_Muted":
                    current_stream_triple = item.get("value")
                if item.get("name") == "M4DS_StreamingVerticalHot_Muted":
                    current_stream_vert_hot = item.get("value")
                if item.get("name") == "M4DS_StreamingVerticalTriple_Muted":
                    current_stream_vert_triple = item.get("value")

            print(f"  Current city: {current_city!r}")
            print(f"  Current reseller: {current_reseller!r}")
            print(f"  Current ISP: {current_isp!r}")
            print(f"  Current StreamingHot_Muted: {current_stream_hot!r}")
            print(f"  Current StreamingTriple_Muted: {current_stream_triple!r}")
            print(f"  Current StreamingVerticalHot_Muted: {current_stream_vert_hot!r}")
            print(f"  Current StreamingVerticalTriple_Muted: {current_stream_vert_triple!r}")

            # Patch city
            patch_player_city(token, player_id, city_en)

            # Set reseller and ISP via POST /v1/players/{id}/variables
            token = request_token(api_key, organization)
            set_player_reseller(token, player_id, reseller_en)
            token = request_token(api_key, organization)
            set_player_isp(token, player_id, isp_en)

            # Set streaming-muted flags based on identifier and group rules
            ident_upper = identifier.upper()
            has_lh = "LH" in ident_upper
            has_pv = "PV" in ident_upper
            is_combined = "PV_LH" in ident_upper or "LH_PV" in ident_upper
            is_lh_only = has_lh and not has_pv and not is_combined
            is_pv_only = has_pv and not has_lh and not is_combined

            token = request_token(api_key, organization)
            set_player_streaming_flags(
                token,
                player_id,
                identifier,
                any_combined=any_combined,
                any_lh_only=any_lh_only,
                any_pv_only=any_pv_only,
                is_combined=is_combined,
                is_lh_only=is_lh_only,
                is_pv_only=is_pv_only,
            )

            # Fetch updated player and show new values
            token = request_token(api_key, organization)
            updated = fetch_player(token, player_id)

            new_city = (updated.get("coordinates") or {}).get("city")

            updated_vars_raw = updated.get("variables") or []
            if isinstance(updated_vars_raw, dict):
                updated_vars_list = [updated_vars_raw]
            elif isinstance(updated_vars_raw, list):
                updated_vars_list = updated_vars_raw
            else:
                updated_vars_list = []

            new_reseller = None
            new_isp = None
            new_stream_hot = None
            new_stream_triple = None
            new_stream_vert_hot = None
            new_stream_vert_triple = None
            for item in updated_vars_list:
                if not isinstance(item, dict):
                    continue
                if item.get("name") == "M4DS_Reseller":
                    new_reseller = item.get("value")
                if item.get("name") == "M4DS_ISP":
                    new_isp = item.get("value")
                if item.get("name") == "M4DS_StreamingHot_Muted":
                    new_stream_hot = item.get("value")
                if item.get("name") == "M4DS_StreamingTriple_Muted":
                    new_stream_triple = item.get("value")
                if item.get("name") == "M4DS_StreamingVerticalHot_Muted":
                    new_stream_vert_hot = item.get("value")
                if item.get("name") == "M4DS_StreamingVerticalTriple_Muted":
                    new_stream_vert_triple = item.get("value")

            print(f"  Updated city: {new_city!r}")
            print(f"  Updated reseller: {new_reseller!r}")
            print(f"  Updated ISP: {new_isp!r}")
            print(f"  Updated StreamingHot_Muted: {new_stream_hot!r}")
            print(f"  Updated StreamingTriple_Muted: {new_stream_triple!r}")
            print(f"  Updated StreamingVerticalHot_Muted: {new_stream_vert_hot!r}")
            print(f"  Updated StreamingVerticalTriple_Muted: {new_stream_vert_triple!r}")
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

