import os
import sys
import requests

APP_URL = os.getenv("STREAMLIT_APP_URL")
SYNC_TOKEN = os.getenv("SYNC_TOKEN")
TOURN_ID = os.getenv("TOURN_ID")
YEAR = os.getenv("YEAR", "2026")

if not APP_URL or not SYNC_TOKEN or not TOURN_ID:
    print("Missing STREAMLIT_APP_URL, SYNC_TOKEN, or TOURN_ID")
    sys.exit(1)

params = {"sync": "1", "token": SYNC_TOKEN, "tournId": TOURN_ID, "year": YEAR}

resp = requests.get(APP_URL, params=params, timeout=60)
if resp.status_code >= 400:
    print(f"Sync failed: {resp.status_code} {resp.text}")
    sys.exit(1)

print("Sync triggered successfully")
