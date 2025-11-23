"""
Validate and correct city and reseller values for all players in the M4D system.
Checks if current values exist in dictionaries, and if not, looks up correct values
from CSV and updates the player.
"""

import csv
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

import requests


CSV_FILE = Path("_1ec01f0כרטיס מכשיר(DataSheet).csv")
DICT_FILE = Path("dictionaries.json")
API_BASE_URL = "https://m4d-srv.ctv.co.il/media4display-api"


def iter_csv_rows(path: Path, encodings: Iterable[str]) -> Iterable[list[str]]:
    """Try to read CSV file with multiple encodings."""
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


def load_dictionaries() -> dict:
    """Load translation dictionaries from JSON file."""
    with DICT_FILE.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def get_all_dictionary_values(dictionaries: dict, dict_key: str) -> set[str]:
    """Get all values from a dictionary (e.g., all values from cities_dictionary)."""
    d = dictionaries.get(dict_key, {})
    return set(value for value in d.values() if value)


def extract_site_number(identifier: str) -> Optional[str]:
    """
    Extract site number from player identifier or name.
    The site number is typically a numeric part at the beginning of the identifier.
    Examples: "200010", "200010-1", "200010_H", etc.
    """
    if not identifier:
        return None
    
    # Try to extract numeric site number (typically 6 digits)
    # Look for pattern like 200010 at the start
    match = re.match(r"(\d{6})", identifier.strip())
    if match:
        return match.group(1)
    
    # Fallback: try to find any 6-digit number
    match = re.search(r"\b(\d{6})\b", identifier)
    if match:
        return match.group(1)
    
    return None


def find_site_city(site_number: str, dictionaries: dict) -> Optional[str]:
    """
    Look up 'עיר האתר' (city) for the given site number in the CSV.
    If multiple rows exist for the site, prioritizes values that exist in cities_dictionary.
    Returns None if not found.
    """
    encodings = ("utf-8-sig", "utf-8", "windows-1255", "cp1255", "iso-8859-8", "latin1")
    rows = iter_csv_rows(CSV_FILE, encodings)
    header = next(rows)

    try:
        site_idx = header.index("מספר אתר/תאור אתר")
        city_idx = header.index("עיר האתר")
    except ValueError:
        return None

    cities_dictionary = dictionaries.get("cities_dictionary", {})
    city_keys = set(key.replace("\u00a0", " ").strip() for key in cities_dictionary.keys())
    
    cities_found = []
    dictionary_match = None
    
    # Check all rows for this site
    for row in rows:
        if len(row) <= max(site_idx, city_idx):
            continue
        if row[site_idx].strip() == site_number:
            city = row[city_idx].replace("\u00a0", " ").strip()
            if city:
                cities_found.append(city)
                # Check if this city is in the dictionary
                if city in city_keys:
                    dictionary_match = city
                    break  # Found a dictionary match, use it
    
    # If we found a dictionary match, return it
    if dictionary_match:
        return dictionary_match
    
    # Otherwise, return the first non-empty city found
    if cities_found:
        return cities_found[0]
    
    return None


def find_site_reseller(site_number: str, dictionaries: dict) -> Optional[str]:
    """
    Look up 'תאור משווק' (reseller description) for the given site number in the CSV.
    If multiple rows exist for the site, prioritizes values that exist in reseller_dictionary.
    Returns None if not found.
    """
    encodings = ("utf-8-sig", "utf-8", "windows-1255", "cp1255", "iso-8859-8", "latin1")
    rows = iter_csv_rows(CSV_FILE, encodings)
    header = next(rows)

    try:
        site_idx = header.index("מספר אתר/תאור אתר")
        reseller_idx = header.index("תאור משווק")
    except ValueError:
        return None

    reseller_dictionary = dictionaries.get("reseller_dictionary", {})
    reseller_keys = set(key.replace("\u00a0", " ").strip() for key in reseller_dictionary.keys())
    
    resellers_found = []
    dictionary_match = None
    
    # Check all rows for this site
    for row in rows:
        if len(row) <= max(site_idx, reseller_idx):
            continue
        if row[site_idx].strip() == site_number:
            reseller = row[reseller_idx].replace("\u00a0", " ").strip()
            if reseller:
                resellers_found.append(reseller)
                # Check if this reseller is in the dictionary
                if reseller in reseller_keys:
                    dictionary_match = reseller
                    break  # Found a dictionary match, use it
    
    # If we found a dictionary match, return it
    if dictionary_match:
        return dictionary_match
    
    # Otherwise, return the first non-empty reseller found
    if resellers_found:
        return resellers_found[0]
    
    return None


def translate_city(city_he: str, dictionaries: dict) -> Optional[str]:
    """Translate Hebrew city name to English code using cities_dictionary."""
    cities = dictionaries.get("cities_dictionary", {})
    for key, value in cities.items():
        if key.replace("\u00a0", " ").strip() == city_he:
            return value
    return None


def translate_reseller(reseller_he: str, dictionaries: dict) -> Optional[str]:
    """Translate Hebrew reseller name to English code using reseller_dictionary."""
    resellers = dictionaries.get("reseller_dictionary", {})
    for key, value in resellers.items():
        if key.replace("\u00a0", " ").strip() == reseller_he:
            return value
    return None


def find_site_sector(site_number: str, dictionaries: dict) -> Optional[str]:
    """
    Look up 'סוג תוכן' (content type/sector) for the given site number in the CSV.
    If multiple rows exist for the site, prioritizes values that exist in sector_dictionary.
    Returns None if not found.
    """
    encodings = ("utf-8-sig", "utf-8", "windows-1255", "cp1255", "iso-8859-8", "latin1")
    rows = iter_csv_rows(CSV_FILE, encodings)
    header = next(rows)

    try:
        site_idx = header.index("מספר אתר/תאור אתר")
        sector_idx = header.index("סוג תוכן")
    except ValueError:
        return None

    sector_dictionary = dictionaries.get("sector_dictionary", {})
    sector_keys = set(key.replace("\u00a0", " ").strip() for key in sector_dictionary.keys())
    
    sectors_found = []
    dictionary_match = None
    
    # Check all rows for this site
    for row in rows:
        if len(row) <= max(site_idx, sector_idx):
            continue
        if row[site_idx].strip() == site_number:
            sector = row[sector_idx].replace("\u00a0", " ").strip()
            if sector:
                sectors_found.append(sector)
                # Check if this sector is in the dictionary
                if sector in sector_keys:
                    dictionary_match = sector
                    break  # Found a dictionary match, use it
    
    # If we found a dictionary match, return it
    if dictionary_match:
        return dictionary_match
    
    # Otherwise, return the first non-empty sector found
    if sectors_found:
        return sectors_found[0]
    
    return None


def translate_sector(sector_he: str, dictionaries: dict) -> str:
    """
    Translate Hebrew sector/content type to English code using sector_dictionary.
    If not found in dictionary, returns "GENERAL".
    """
    sectors = dictionaries.get("sector_dictionary", {})
    for key, value in sectors.items():
        if key.replace("\u00a0", " ").strip() == sector_he:
            return value
    # If not found in dictionary, return "GENERAL"
    return "GENERAL"


def get_api_credentials() -> tuple[str, str]:
    """Prompt the user for the API key and organization."""
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
    """Get authentication token from the API."""
    resp = requests.post(
        f"{API_BASE_URL}/v1/token",
        json={"apiKey": api_key, "organization": organization},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text.strip('"')


def fetch_all_players(token: str) -> list[dict]:
    """Fetch all players from the API."""
    print("[INFO] Fetching all players from API...")
    resp = requests.get(
        f"{API_BASE_URL}/v1/players",
        headers={"Authorization": f"Bearer {token}"},
        timeout=120,
    )
    resp.raise_for_status()
    players = resp.json()
    
    if not isinstance(players, list):
        print(f"[WARN] API returned non-list response. Type: {type(players)}")
        return []
    
    print(f"[INFO] Fetched {len(players)} player(s) from API.")
    return players


def fetch_player(token: str, player_id: int) -> dict:
    """Fetch detailed information for a specific player."""
    resp = requests.get(
        f"{API_BASE_URL}/v1/players/{player_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def patch_player_city(token: str, player_id: int, city_en: str) -> None:
    """Update player city using PATCH. This does NOT delete anything - only updates the city value."""
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
    """Set the M4DS_Reseller variable for a player. This does NOT delete anything - only updates/adds the variable."""
    if not reseller_en:
        return

    payload = [{"name": "M4DS_Reseller", "value": reseller_en}]

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


def set_player_sector(token: str, player_id: int, sector_en: str) -> None:
    """Set the M4DS_Sector variable for a player. This does NOT delete anything - only updates/adds the variable."""
    if not sector_en:
        return

    payload = [{"name": "M4DS_Sector", "value": sector_en}]

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
            print(f"  Sector POST error body: {resp.text}")
        except Exception:
            pass
    resp.raise_for_status()


def validate_and_correct_player(
    player: dict,
    dictionaries: dict,
    valid_cities: set[str],
    valid_resellers: set[str],
    valid_sectors: set[str],
    api_key: str,
    organization: str,
) -> dict:
    """
    Validate and correct a single player.
    Returns dict with validation results.
    """
    player_id = player.get("playerId") or player.get("id")
    identifier = (player.get("identifier") or player.get("name", "")).strip()

    result = {
        "player_id": player_id,
        "identifier": identifier,
        "needs_correction": False,
        "city_invalid": False,
        "reseller_invalid": False,
        "sector_invalid": False,
        "site_number": None,
        "updated": False,
        "error": None,
        # Track original and new values for CSV
        "original_city": None,
        "new_city": None,
        "city_changed": False,
        "original_reseller": None,
        "new_reseller": None,
        "reseller_changed": False,
        "original_sector": None,
        "new_sector": None,
        "sector_changed": False,
    }

    if player_id is None:
        result["error"] = "No player ID"
        return result

    try:
        # Fetch full player details
        token = request_token(api_key, organization)
        player_details = fetch_player(token, player_id)

        # Get current values
        coords = player_details.get("coordinates") or {}
        current_city = coords.get("city")
        result["original_city"] = current_city or ""

        vars_raw = player_details.get("variables") or []
        if isinstance(vars_raw, dict):
            vars_list = [vars_raw]
        elif isinstance(vars_raw, list):
            vars_list = vars_raw
        else:
            vars_list = []

        current_reseller = None
        current_sector = None
        for item in vars_list:
            if not isinstance(item, dict):
                continue
            if item.get("name") == "M4DS_Reseller":
                current_reseller = item.get("value")
            if item.get("name") == "M4DS_Sector":
                current_sector = item.get("value")
        
        result["original_reseller"] = current_reseller or ""
        result["original_sector"] = current_sector or ""

        # Validate city
        city_valid = False
        if current_city and current_city in valid_cities:
            city_valid = True
        else:
            result["city_invalid"] = True
            result["needs_correction"] = True

        # Validate reseller
        reseller_valid = False
        if current_reseller and current_reseller in valid_resellers:
            reseller_valid = True
        else:
            result["reseller_invalid"] = True
            result["needs_correction"] = True

        # Show player status for city and reseller
        city_status = "✓ OK" if city_valid else "✗ INVALID"
        reseller_status = "✓ OK" if reseller_valid else "✗ INVALID"
        overall_status = "✓ OK" if not result["needs_correction"] else "✗ NEEDS CORRECTION"
        
        print(f"\n[{overall_status}] Player: {identifier} (ID: {player_id})")
        print(f"  City: {current_city or '(empty)'} - {city_status}")
        print(f"  Reseller: {current_reseller or '(empty)'} - {reseller_status}")
        print(f"  Sector: {current_sector or '(empty)'}")

        # Extract site number once for all lookups
        site_number = extract_site_number(identifier)
        result["site_number"] = site_number

        # Always check and correct sector (no validation step - always lookup from CSV)
        sector_he = None
        expected_sector_en = None
        if site_number:
            # Look up correct sector from CSV
            sector_he = find_site_sector(site_number, dictionaries)
            if sector_he:
                # Translate Hebrew to English code
                expected_sector_en = translate_sector(sector_he, dictionaries)
            else:
                # No sector found in CSV - use GENERAL as default
                expected_sector_en = "GENERAL"
            
            # Compare current sector with expected sector
            if current_sector != expected_sector_en:
                result["sector_invalid"] = True
                result["needs_correction"] = True
                print(f"    Sector needs update: {current_sector or '(empty)'} → {expected_sector_en}")
            else:
                print(f"    Sector is correct: {expected_sector_en}")

        # If validation fails, try to correct
        if result["needs_correction"]:
            if not site_number:
                result["error"] = "Could not extract site number from identifier"
                return result

            # Look up correct values from CSV
            city_he = find_site_city(site_number, dictionaries) if result["city_invalid"] else None
            reseller_he = find_site_reseller(site_number, dictionaries) if result["reseller_invalid"] else None
            # Sector already looked up above, reuse expected_sector_en

            if not city_he and result["city_invalid"]:
                result["error"] = f"Site number {site_number} not found in CSV for city"
                return result

            if not reseller_he and result["reseller_invalid"]:
                result["error"] = f"Site number {site_number} not found in CSV for reseller"
                return result

            # Translate Hebrew to English
            city_en = None
            if city_he:
                city_en = translate_city(city_he, dictionaries)
                if not city_en:
                    result["error"] = f"City '{city_he}' not found in cities_dictionary"
                    return result

            reseller_en = None
            if reseller_he:
                reseller_en = translate_reseller(reseller_he, dictionaries)
                if not reseller_en:
                    result["error"] = f"Reseller '{reseller_he}' not found in reseller_dictionary"
                    return result

            # Sector already translated above, reuse expected_sector_en
            sector_en = expected_sector_en if result["sector_invalid"] else None

            # Update player
            print(f"  → Correcting values:")
            if result["city_invalid"] and city_en:
                print(f"    City: {current_city or '(empty)'} → {city_en}")
            if result["reseller_invalid"] and reseller_en:
                print(f"    Reseller: {current_reseller or '(empty)'} → {reseller_en}")
            if result["sector_invalid"] and sector_en:
                print(f"    Sector: {current_sector or '(empty)'} → {sector_en}")

            try:
                token = request_token(api_key, organization)
                if result["city_invalid"] and city_en:
                    patch_player_city(token, player_id, city_en)
                    result["city_changed"] = True
                    result["new_city"] = city_en
                    print(f"    ✓ City updated successfully")

                if result["reseller_invalid"] and reseller_en:
                    token = request_token(api_key, organization)
                    set_player_reseller(token, player_id, reseller_en)
                    result["reseller_changed"] = True
                    result["new_reseller"] = reseller_en
                    print(f"    ✓ Reseller updated successfully")

                if result["sector_invalid"] and sector_en:
                    token = request_token(api_key, organization)
                    set_player_sector(token, player_id, sector_en)
                    result["sector_changed"] = True
                    result["new_sector"] = sector_en
                    print(f"    ✓ Sector updated successfully")

                result["updated"] = True
            except requests.HTTPError as exc:
                error_msg = f"API update failed: {exc}"
                if hasattr(exc, 'response') and exc.response is not None:
                    try:
                        error_body = exc.response.text
                        error_msg += f" - {error_body}"
                    except Exception:
                        pass
                result["error"] = error_msg
                print(f"    ✗ {error_msg}")
                return result
            except Exception as exc:
                result["error"] = f"Unexpected error during update: {exc}"
                print(f"    ✗ {result['error']}")
                return result
        else:
            # Player is valid, no correction needed
            print(f"  ✓ All values are correct, no update needed")

    except Exception as exc:
        result["error"] = str(exc)
        print(f"  [ERROR] Failed to process player {identifier} (ID: {player_id}): {exc}")

    return result


def write_results_csv(results: list[dict], output_file: Path, total_players: int, processed_count: int) -> None:
    """
    Write validation results to a CSV file.
    
    IMPORTANT: This CSV only contains players that were actually checked/processed.
    Players that were not processed (due to test mode limits) are NOT included.
    This script does NOT delete any players - it only updates existing player data.
    """
    if not results:
        return
    
    fieldnames = [
        "Player ID",
        "Player Identifier",
        "Site Number",
        "City Status",
        "Reseller Status",
        "Sector Status",
        "Error Message",
    ]
    
    with output_file.open("w", encoding="utf-8-sig", newline="") as fh:
        # Write explanation header for Notepad readability
        fh.write(f"Player Validation Results Report\n")
        fh.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        fh.write(f"Total players in system: {total_players}\n")
        fh.write(f"Players checked in this run: {processed_count}\n")
        fh.write(f"Players in this CSV (with errors or updates): {len(results)}\n")
        fh.write(f"\n")
        fh.write(f"IMPORTANT NOTES:\n")
        fh.write(f"- This CSV contains ONLY players with ERRORS or UPDATES\n")
        fh.write(f"- Players that were OK (no changes needed) are NOT included\n")
        fh.write(f"- Players not checked remain completely untouched in the system\n")
        fh.write(f"- This script does NOT delete any players - it only updates data\n")
        fh.write(f"\n")
        fh.write(f"{'='*100}\n")
        fh.write(f"DATA STARTS BELOW\n")
        fh.write(f"{'='*100}\n")
        fh.write(f"\n")
        
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        
        for r in results:
            # Build city status description
            original_city = r.get("original_city", "") or "(empty)"
            if r.get("city_changed"):
                new_city = r.get("new_city", "") or "(empty)"
                city_status = f"city changed from {original_city} to {new_city}"
            else:
                city_status = f"city is valid ({original_city})"
            
            # Build reseller status description
            original_reseller = r.get("original_reseller", "") or "(empty)"
            if r.get("reseller_changed"):
                new_reseller = r.get("new_reseller", "") or "(empty)"
                reseller_status = f"reseller changed from {original_reseller} to {new_reseller}"
            else:
                reseller_status = f"reseller is valid ({original_reseller})"
            
            # Build sector status description
            original_sector = r.get("original_sector", "") or "(empty)"
            if r.get("sector_changed"):
                new_sector = r.get("new_sector", "") or "(empty)"
                sector_status = f"sector changed from {original_sector} to {new_sector}"
            else:
                sector_status = f"sector is valid ({original_sector})"
            
            row = {
                "Player ID": r.get("player_id", ""),
                "Player Identifier": r.get("identifier", ""),
                "Site Number": r.get("site_number", ""),
                "City Status": city_status,
                "Reseller Status": reseller_status,
                "Sector Status": sector_status,
                "Error Message": r.get("error", ""),
            }
            writer.writerow(row)


def main() -> int:
    """Main function."""
    print("=" * 60)
    print("M4D Player Validation and Correction")
    print("=" * 60)
    print()

    if not CSV_FILE.exists():
        print(f"[ERROR] CSV file not found: {CSV_FILE}")
        return 1
    if not DICT_FILE.exists():
        print(f"[ERROR] Dictionaries file not found: {DICT_FILE}")
        return 1

    # Load dictionaries
    dictionaries = load_dictionaries()
    valid_cities = get_all_dictionary_values(dictionaries, "cities_dictionary")
    valid_resellers = get_all_dictionary_values(dictionaries, "reseller_dictionary")
    valid_sectors = get_all_dictionary_values(dictionaries, "sector_dictionary")
    # Add "GENERAL" to valid sectors
    valid_sectors.add("GENERAL")

    print(f"[INFO] Loaded {len(valid_cities)} valid city values")
    print(f"[INFO] Loaded {len(valid_resellers)} valid reseller values")
    print(f"[INFO] Loaded {len(valid_sectors)} valid sector values (including GENERAL)")
    print()

    # Get API credentials
    api_key, organization = get_api_credentials()

    # Fetch all players
    token = request_token(api_key, organization)
    all_players = fetch_all_players(token)

    if not all_players:
        print("[ERROR] No players found in the system.")
        return 1

    # Process all players
    original_total_count = len(all_players)
    print(f"\n[INFO] Processing all {len(all_players)} player(s)...")
    print("[INFO] This may take a while.\n")

    results = []
    corrected_count = 0
    error_count = 0

    # Process only the limited set of players (e.g., first 100 in test mode)
    # IMPORTANT: Players not in this list are NOT affected - they remain unchanged in the system
    # This script does NOT delete any players, only updates the ones that are processed
    for i, player in enumerate(all_players, 1):
        if i % 10 == 0:
            print(f"[INFO] Processing player {i}/{len(all_players)}...")

        result = validate_and_correct_player(
            player, dictionaries, valid_cities, valid_resellers, valid_sectors, api_key, organization
        )
        results.append(result)

        if result["updated"]:
            corrected_count += 1
        if result["error"]:
            error_count += 1

    # Print summary
    print("\n" + "=" * 60)
    print("Summary:")
    print("=" * 60)
    print(f"Total players processed: {len(all_players)}")
    print(f"Players corrected: {corrected_count}")
    print(f"Players with errors: {error_count}")

    invalid_city_count = sum(1 for r in results if r["city_invalid"])
    invalid_reseller_count = sum(1 for r in results if r["reseller_invalid"])
    invalid_sector_count = sum(1 for r in results if r["sector_invalid"])

    print(f"\nPlayers with invalid city: {invalid_city_count}")
    print(f"Players with invalid reseller: {invalid_reseller_count}")
    print(f"Players with invalid sector: {invalid_sector_count}")

    # Show players with errors
    players_with_errors = [r for r in results if r["error"]]
    if players_with_errors:
        print(f"\nPlayers with errors ({len(players_with_errors)}):")
        for r in players_with_errors:
            print(f"  - {r['identifier']} (ID: {r['player_id']}): {r['error']}")

    # Write results to CSV
    # IMPORTANT: This CSV only includes players that had errors or were updated (changed values)
    # Players that were OK and didn't need changes are NOT included in this CSV
    # Players not checked are NOT included in this CSV and remain completely untouched in the system
    filtered_results = [r for r in results if r.get("error") or r.get("updated")]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_output_file = Path(f"player_validation_results_{timestamp}.csv")
    print(f"\n[INFO] Writing results to CSV: {csv_output_file}")
    print(f"[INFO] Total players checked: {len(results)}")
    print(f"[INFO] Players with errors or updates: {len(filtered_results)}")
    print(f"[INFO] CSV will contain only players with errors or updates (not OK players)")
    try:
        write_results_csv(filtered_results, csv_output_file, original_total_count, len(results))
        print(f"[INFO] CSV file created successfully: {csv_output_file}")
        print(f"[INFO] CSV contains {len(filtered_results)} player(s) with errors or updates")
    except Exception as exc:
        print(f"[ERROR] Failed to write CSV file: {exc}")

    return 0


if __name__ == "__main__":
    exit_code = 0
    try:
        exit_code = main()
    except Exception as exc:
        print(f"[ERROR] {exc}")
        import traceback
        traceback.print_exc()
        exit_code = 1
    finally:
        try:
            print("\nExiting in 15 seconds...")
            time.sleep(15)
        except KeyboardInterrupt:
            pass
    sys.exit(exit_code)

