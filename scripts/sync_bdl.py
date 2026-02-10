import os
import sys
import requests

APP_URL = os.getenv("STREAMLIT_APP_URL")
SYNC_TOKEN = os.getenv("SYNC_TOKEN")

if not APP_URL or not SYNC_TOKEN:
    print("Missing STREAMLIT_APP_URL or SYNC_TOKEN")
    sys.exit(1)

params = {"sync": "1", "token": SYNC_TOKEN}

resp = requests.get(APP_URL, params=params, timeout=60)
if resp.status_code >= 400:
    print(f"Sync failed: {resp.status_code} {resp.text}")
    sys.exit(1)

print("Sync triggered successfully")
