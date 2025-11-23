"""
Simple script to fetch a specific player from the M4D API.
Usage: py fetch_player.py
"""

import os
import sys
import json
import time
import requests
from pathlib import Path

API_BASE_URL = "https://m4d-srv.ctv.co.il/media4display-api"


def request_token(api_key: str, organization: str) -> str:
    """Get authentication token from the API."""
    print("[INFO] Requesting authentication token...")
    resp = requests.post(
        f"{API_BASE_URL}/v1/token",
        json={"apiKey": api_key, "organization": organization},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.text.strip('"')
    print("[INFO] Token received successfully")
    return token


def fetch_player(token: str, player_id: int) -> dict:
    """Fetch detailed information for a specific player."""
    print(f"[INFO] Fetching player {player_id}...")
    print(f"[INFO] URL: {API_BASE_URL}/v1/players/{player_id}")
    
    resp = requests.get(
        f"{API_BASE_URL}/v1/players/{player_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    
    # Display response details
    print(f"\n[INFO] Response Status Code: {resp.status_code}")
    print(f"[INFO] Response Headers:")
    for key, value in resp.headers.items():
        print(f"  {key}: {value}")
    
    # Display response body (raw text)
    print(f"\n[INFO] Raw Response Body:")
    print(f"{'='*60}")
    print(resp.text)
    print(f"{'='*60}")
    
    # Check for errors first, but don't raise yet - we want to see the response
    if resp.status_code >= 400:
        print(f"\n[WARN] HTTP Error Status Code: {resp.status_code}")
    
    # Try to parse JSON if possible
    try:
        player_data = resp.json()
        print(f"\n[INFO] Successfully parsed JSON response")
        player_data["_status_code"] = resp.status_code  # Include status code in response
        return player_data
    except json.JSONDecodeError as e:
        print(f"\n[WARN] Could not parse response as JSON: {e}")
        print(f"[INFO] Returning raw text instead")
        return {"raw_response": resp.text, "status_code": resp.status_code}
    
    # Now raise for status if there was an error (after we've displayed everything)
    resp.raise_for_status()


def main():
    """Main function."""
    print("=" * 60)
    print("M4D Player Fetcher")
    print("=" * 60)
    print()

    # Get API credentials
    api_key = input("Enter API key: ").strip()
    if not api_key:
        print("[ERROR] API key is required.")
        return 1

    organization = os.environ.get("M4D_ORG")
    if not organization:
        organization = input("Enter organization: ").strip()
    if not organization:
        print("[ERROR] Organization is required.")
        return 1

    # Get player ID
    player_id_input = input("Enter player ID (default: 324): ").strip()
    player_id = int(player_id_input) if player_id_input else 324

    try:
        # Get token
        token = request_token(api_key, organization)

        # Fetch player
        player = fetch_player(token, player_id)

        # Display parsed results
        if isinstance(player, dict) and "raw_response" in player:
            print("\n" + "=" * 60)
            print("Response could not be parsed as JSON")
            print("=" * 60)
            print(f"Status Code: {player.get('status_code', 'N/A')}")
            print(f"Raw Response: {player.get('raw_response', 'N/A')}")
        else:
            print("\n" + "=" * 60)
            print(f"Player {player_id} Data (Parsed JSON):")
            print("=" * 60)
            print(json.dumps(player, indent=2, ensure_ascii=False))
            print("=" * 60)

            # Extract key information
            print("\n" + "=" * 60)
            print("Key Information:")
            print("=" * 60)
            player_id_val = player.get("playerId") or player.get("id")
            identifier = player.get("identifier") or player.get("name", "N/A")
            
            coords = player.get("coordinates") or {}
            city = coords.get("city", "N/A")
            
            vars_list = player.get("variables") or []
            if isinstance(vars_list, dict):
                vars_list = [vars_list]
            
            reseller = None
            sector = None
            for var in vars_list:
                if isinstance(var, dict):
                    name = var.get("name", "")
                    value = var.get("value", "")
                    if name == "M4DS_Reseller":
                        reseller = value
                    elif name == "M4DS_Sector":
                        sector = value
            
            print(f"Player ID: {player_id_val}")
            print(f"Identifier: {identifier}")
            print(f"City: {city}")
            print(f"Reseller: {reseller or 'N/A'}")
            print(f"Sector: {sector or 'N/A'}")
            print("=" * 60)

        return 0

    except requests.HTTPError as e:
        print("\n" + "=" * 60)
        print("HTTP Error Details:")
        print("=" * 60)
        print(f"Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"\nStatus Code: {e.response.status_code}")
            print(f"Response Headers:")
            for key, value in e.response.headers.items():
                print(f"  {key}: {value}")
            print(f"\nResponse Body:")
            print(f"{'='*60}")
            try:
                print(e.response.text)
            except Exception:
                print("Could not read response text")
            print(f"{'='*60}")
            # Try to parse error response as JSON
            try:
                error_json = e.response.json()
                print(f"\nParsed Error JSON:")
                print(json.dumps(error_json, indent=2, ensure_ascii=False))
            except Exception:
                pass
        return 1
    except requests.RequestException as e:
        print("\n" + "=" * 60)
        print("Request Error Details:")
        print("=" * 60)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    except Exception as e:
        print("\n" + "=" * 60)
        print("Unexpected Error Details:")
        print("=" * 60)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
    except KeyboardInterrupt:
        print("\n\n[INFO] Interrupted by user")
        exit_code = 1
    finally:
        try:
            print("\n[INFO] Exiting in 15 seconds...")
            time.sleep(15)
        except KeyboardInterrupt:
            pass
    sys.exit(exit_code)

