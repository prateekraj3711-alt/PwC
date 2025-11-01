import asyncio
import os
import json
import logging
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from playwright.async_api import async_playwright, Page, Download
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
app = FastAPI(title="PwC Dashboard Export API")

start_time = datetime.now()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
SESSION_STORAGE_PATH = os.getenv("SESSION_STORAGE_PATH", "/tmp/pwc")
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
SNAPSHOTS_DIR = TMP_DIR / "snapshots"
EXPORT_TIMEOUT_MS = 240000
KEY_COLUMN = "Candidate ID"
DASHBOARD_TABS = [
    "Today's allocated",
    "Not started",
    "Draft",
    "Rejected / Insufficient",
    "Submitted",
    "Work in progress",
    "BGV closed"
]
SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
(TMP_DIR / "dashboard_exports").mkdir(parents=True, exist_ok=True)

def get_sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if GOOGLE_CREDENTIALS_JSON:
        creds_info = json.loads(GOOGLE_CREDENTIALS_JSON.replace("\\n", "\n"))
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    else:
        raise FileNotFoundError("GOOGLE_CREDENTIALS_JSON not found")
    return build("sheets", "v4", credentials=creds)

def read_sheet(service, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    try:
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A:ZZ").execute()
        values = result.get("values", [])
        if not values:
            return pd.DataFrame()
        headers = values[0]
        data = values[1:]
        padded = [r + [""] * (len(headers) - len(r)) for r in data]
        return pd.DataFrame(padded, columns=headers)
    except Exception:
        return pd.DataFrame()

def write_sheet(service, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame):
    try:
        if "LastSyncedAt" not in df.columns:
            df.insert(0, "LastSyncedAt", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        else:
            df["LastSyncedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        headers = [df.columns.tolist()]
        values = headers + df.fillna("").values.tolist()
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()
        logger.info(f"Wrote {len(df)} rows to sheet '{sheet_name}'")
    except Exception as e:
        logger.error(f"Error writing sheet: {e}")

def save_snapshot(tab, df):
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    path = SNAPSHOTS_DIR / f"{tab}.json"
    with open(path, "w") as f:
        json.dump({"rows": df.fillna("").to_dict("records")}, f)

def load_snapshot(tab):
    path = SNAPSHOTS_DIR / f"{tab}.json"
    if not path.exists():
        return None
    try:
        data = json.load(open(path))
        return pd.DataFrame(data["rows"])
    except Exception:
        return None

def incremental_sync(tab, excel_path, sheet_id):
    try:
        service = get_sheets_service()
        df_new = pd.read_excel(excel_path)
        if df_new.empty:
            return {"new": 0, "updated": 0, "skipped": 0}
        key = KEY_COLUMN if KEY_COLUMN in df_new.columns else df_new.columns[0]
        df_new[key] = df_new[key].astype(str).str.strip()
        df_existing = read_sheet(service, sheet_id, tab)
        if df_existing.empty:
            df_existing = load_snapshot(tab) or pd.DataFrame()
        new, updated, skipped = 0, 0, 0
        if df_existing.empty:
            df_merged = df_new.copy()
            new = len(df_merged)
        else:
            df_existing[key] = df_existing[key].astype(str).str.strip()
            df_merged = df_existing.copy()
            for _, row in df_new.iterrows():
                k = str(row[key]).strip()
                mask = df_merged[key] == k
                if not mask.any():
                    df_merged = pd.concat([df_merged, row.to_frame().T], ignore_index=True)
                    new += 1
                else:
                    idx = df_merged[mask].index[0]
                    if not row.equals(df_merged.loc[idx]):
                        df_merged.loc[idx] = row
                        updated += 1
                    else:
                        skipped += 1
        write_sheet(service, sheet_id, tab, df_merged)
        save_snapshot(tab, df_merged)
        return {"new": new, "updated": updated, "skipped": skipped}
    except Exception as e:
        logger.error(f"Sync error for {tab}: {e}")
        return {"error": str(e)}

async def export_dashboard_tab(page: Page, tab, path, sheet):
    try:
        logger.info(f"📊 Exporting tab: {tab}")
        await page.wait_for_selector(f"text=\"{tab}\"", timeout=30000)
        await page.click(f"text=\"{tab}\"")
        await asyncio.sleep(25)
        btn = page.locator("button:has-text('Export to excel'), a:has-text('Export to excel')")
        await btn.wait_for(state="visible", timeout=30000)
        download_event = asyncio.Event()
        file = path / f"{tab}.xlsx"
        async def handle_download(d: Download):
            await d.save_as(file)
            download_event.set()
        page.on("download", handle_download)
        await btn.click()
        await asyncio.wait_for(download_event.wait(), timeout=EXPORT_TIMEOUT_MS / 1000)
        await asyncio.sleep(3)
        if not file.exists() or file.stat().st_size == 0:
            return {"tab": tab, "error": "Download failed"}
        res = incremental_sync(tab, file, sheet)
        file.unlink(missing_ok=True)
        return {"tab": tab, **res}
    except Exception as e:
        return {"tab": tab, "error": str(e)}

async def export_dashboard(session_id: str, sheet_id: str, state: Optional[Dict]):
    results = []
    try:
        path = TMP_DIR / "dashboard_exports"
        path.mkdir(parents=True, exist_ok=True)
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(storage_state=state, accept_downloads=True)
            page = await context.new_page()
            await page.goto("https://compliancenominationportal.in.pwc.com/dashboard", wait_until="networkidle")
            for i in range(3):
                try:
                    sel = "button.btn.btn-warning:has-text('Advance search')"
                    await page.wait_for_selector(sel, timeout=10000)
                    await page.click(sel)
                    await asyncio.sleep(3)
                    break
                except Exception:
                    if i == 2:
                        raise Exception("Advance search not clickable after 3 attempts")
                    await asyncio.sleep(2)
            await asyncio.sleep(2)
            for tab in DASHBOARD_TABS:
                res = await export_dashboard_tab(page, tab, path, sheet_id)
                results.append(res)
                await asyncio.sleep(2)
            try:
                logger.info("🔒 Attempting logout")
                arrow = page.locator("button.dropdown-toggle, .dropdown-toggle")
                await arrow.wait_for(state="visible", timeout=15000)
                await arrow.click()
                await asyncio.sleep(2)
                logout = page.locator("a:has-text('Logout'), [href*='Signout']")
                await logout.wait_for(state="visible", timeout=10000)
                await logout.click()
                await page.wait_for_url("**/Login/Signout", timeout=30000)
                logger.info("✅ Logout successful")
            except Exception as e:
                logger.error(f"Logout failed: {e}")
            await browser.close()
        return {"ok": True, "results": results}
    except Exception as e:
        logger.error(f"Export error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class ExportRequest(BaseModel):
    session_id: Optional[str] = "latest"
    spreadsheet_id: Optional[str] = None
    storage_state: Optional[Dict] = None

@app.post("/export-dashboard")
async def export_dashboard_endpoint(req: ExportRequest):
    sid = req.session_id or "latest"
    sheet = req.spreadsheet_id or GOOGLE_SHEET_ID
    if not sheet:
        raise HTTPException(status_code=400, detail="Spreadsheet ID required")
    return await export_dashboard(sid, sheet, req.storage_state)

@app.get("/health")
async def health():
    return {"ok": True, "uptime": int((datetime.now() - start_time).total_seconds())}
