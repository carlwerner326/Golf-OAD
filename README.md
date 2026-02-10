# Golf One & Done Pool (Streamlit)

Streamlit app for a season-long One & Done pool with manual and RapidAPI sync options.

## Run locally
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## Environment variables (.env)
Create a `.env` file in the project root (copy `.env.example`). Example:
```
RAPIDAPI_KEY=your_rapidapi_key
RAPIDAPI_HOST=live-golf-data.p.rapidapi.com
SYNC_TOKEN=your_secret_token
GOLF_DB_PATH=optional/path/to/golf.db
```

## Admin access
Admin is **Carl**. Enter `Carl` in the sidebar admin name box.

## RapidAPI sync (Live Golf Data)
Admin tab → **RapidAPI Sync**
- Set `tournId` (from RapidAPI)
- Pick year
- Click **Sync Now**

This pulls:
- `/leaderboard` for positions (Wins/Top5/Top10)
- `/earnings` for purse values

## Scheduler (Monday 1AM EST)
GitHub Actions workflow triggers weekly sync. Requires repo secrets:
- `STREAMLIT_APP_URL` (your Streamlit app URL)
- `SYNC_TOKEN`
- `TOURN_ID` (RapidAPI tournId)
- `YEAR` (optional; defaults to 2026)

The workflow calls:
```
https://your-app.streamlit.app/?sync=1&token=...&tournId=...&year=2026
```

## Bulk formats
Golfers:
```
Name
Scottie Scheffler
Rory McIlroy
```

Results:
```
Tournament Name, Golfer Name, Purse, Position
WM Phoenix Open,Si Woo Kim,439680,3
```
