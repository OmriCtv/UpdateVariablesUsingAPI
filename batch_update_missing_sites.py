import csv
from pathlib import Path
from typing import Iterable, List, Tuple

import requests

from update_player_city import (
    CSV_FILE,
    DICT_FILE,
    API_BASE_URL,
    find_site_city,
    find_site_reseller,
    find_site_isp,
    load_dictionaries,
    translate_city,
    translate_reseller,
    translate_isp,
    get_api_credentials,
    request_token,
    fetch_players,
    filter_players,
    fetch_player,
    patch_player_city,
    set_player_reseller,
    set_player_isp,
    set_player_streaming_flags,
)


MISSING_SITES_FILE = Path("missing_player_attributes.csv")


def load_missing_sites(path: Path) -> List[Tuple[str, str]]:
    """
    Load existing missing_player_attributes.csv and return a list of
    (site_id, note). Note may be empty string if no note is present.
    """
    sites: List[Tuple[str, str]] = []
    if not path.exists():
        return sites

    seen: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader, None)
        if not header:
            return sites
        for row in reader:
            if not row:
                continue
            site_id = row[0].strip()
            if not site_id or site_id in seen:
                continue
            seen.add(site_id)
            note = row[1].strip() if len(row) > 1 else ""
            sites.append((site_id, note))
    return sites


def write_missing_sites(path: Path, sites: Iterable[Tuple[str, str]]) -> None:
    """
    Rewrite missing_player_attributes.csv with the remaining site ids and notes.
    """
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["מספר אתר/תאור אתר", "note"])
        for site_id, note in sites:
            writer.writerow([site_id, note])


def process_site(
    site_id: str,
    dictionaries: dict,
    api_key: str,
    organization: str,
    players_cache: list[dict],
) -> Tuple[bool, str]:
    """
    Apply the same logic as update_player_city.main for a single site id.

    Returns (resolved, note):
      - resolved=True  -> site was successfully updated, can be removed from CSV.
      - resolved=False -> keep site in CSV with 'note' explaining why.
    """
    if not CSV_FILE.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")
    if not DICT_FILE.exists():
        raise FileNotFoundError(f"Dictionaries file not found: {DICT_FILE}")

    print(f"\n=== Processing site {site_id} ===")

    try:
        city_he = find_site_city(site_id)
        reseller_he = find_site_reseller(site_id)
        isp_he = find_site_isp(site_id)
    except Exception as exc:
        msg = f"Failed to read CSV data for site {site_id}: {exc}"
        print(f"[ERROR] {msg}")
        return False, msg

    try:
        city_en = translate_city(city_he, dictionaries)
        reseller_en = translate_reseller(reseller_he, dictionaries)
        isp_en = translate_isp(isp_he, dictionaries)
    except RuntimeError as exc:
        # Dictionary lookup failure (city/reseller/ISP not found) –
        # record it in the CSV note and continue with other sites.
        msg = str(exc)
        print(f"[ERROR] {msg}")
        return False, msg

    print(
        f"Site {site_id}: Hebrew city '{city_he}' -> English '{city_en}', "
        f"reseller '{reseller_he}' -> '{reseller_en}', "
        f"ISP '{isp_he}' -> '{isp_en}'"
    )

    token = request_token(api_key, organization)
    players = players_cache or fetch_players(token)
    target_players = filter_players(players, site_id)

    if not target_players:
        msg = f"No players found containing '{site_id}'. Nothing to update."
        print(msg)
        return False, msg

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

    had_error = False

    for player in target_players:
        player_id = player.get("playerId") or player.get("id")
        identifier = (player.get("identifier") or player.get("name", "")).strip()
        if player_id is None:
            print(f"Skipping player without id: {identifier}")
            continue

        try:
            print(f"\nProcessing player {identifier} (id {player_id})")
            token = request_token(api_key, organization)  # fresh token per player
            current = fetch_player(token, int(player_id))

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
            patch_player_city(token, int(player_id), city_en)

            # Set reseller and ISP via POST /v1/players/{id}/variables
            token = request_token(api_key, organization)
            set_player_reseller(token, int(player_id), reseller_en)
            token = request_token(api_key, organization)
            set_player_isp(token, int(player_id), isp_en)

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
                int(player_id),
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
            updated = fetch_player(token, int(player_id))

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
            had_error = True
        except Exception as exc:
            print(f"  [ERROR] Unexpected error: {exc}")
            had_error = True

    if had_error:
        return False, "One or more players failed to update"

    return True, ""


def main() -> None:
    if not MISSING_SITES_FILE.exists():
        raise FileNotFoundError(f"Missing sites file not found: {MISSING_SITES_FILE}")

    original_sites = load_missing_sites(MISSING_SITES_FILE)
    dictionaries = load_dictionaries()
    api_key, organization = get_api_credentials()

    # Pre-fetch players once
    token = request_token(api_key, organization)
    players_cache = fetch_players(token)

    # Process all sites from the CSV
    remaining_sites: List[Tuple[str, str]] = []

    for site_id, existing_note in original_sites:
        resolved, note = process_site(
            site_id, dictionaries, api_key, organization, players_cache
        )
        if resolved:
            # Successfully updated – remove from CSV (do not add to remaining_sites)
            continue

        # Keep site with updated note (dictionary errors, no players, or per-player errors)
        final_note = note or existing_note
        remaining_sites.append((site_id, final_note))

    write_missing_sites(MISSING_SITES_FILE, remaining_sites)

    print(
        f"\n[INFO] Batch update finished. Remaining sites in {MISSING_SITES_FILE}: "
        f"{len(remaining_sites)}"
    )


if __name__ == "__main__":
    main()


