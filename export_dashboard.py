"""
PwC Dashboard Export Automation
FastAPI + Playwright + Google Sheets
→ Clicks 'Advance search'
→ Exports each dashboard tab with confirmation and incremental sync
→ Logs heartbeat during long waits
→ Prints summary table after all tabs
"""

import asyncio, os, json, logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from playwright.async_api import async_playwright, Page, Download
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ===== Logging =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("export_dashboard")

# ===== FastAPI App =====
app = FastAPI(title="PwC Dashboard Export Service")
start_time = datetime.now()

# ===== Config =====
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
TMP_DIR = Path("/tmp")
SNAPSHOTS_DIR = TMP_DIR / "snapshots"
SESSION_STORAGE_PATH = "/tmp/pwc"
EXPORT_TIMEOUT_MS = 240000  # 4 minutes
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

# ===== Google Sheets Utils =====
def get_sheets_service():
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    if GOOGLE_CREDENTIALS_JSON:
        creds_info = json.loads(GOOGLE_CREDENTIALS_JSON.replace('\\n', '\n'))
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    else:
        raise FileNotFoundError("GOOGLE_CREDENTIALS_JSON not found")
    return build("sheets", "v4", credentials=creds)

def read_sheet(service, spreadsheet_id, sheet_name):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A:ZZ"
        ).execute()
        vals = result.get("values", [])
        if not vals:
            return pd.DataFrame()
        headers, rows = vals[0], vals[1:]
        df = pd.DataFrame(rows, columns=headers)
        return df
    except Exception:
        return pd.DataFrame()

def write_sheet(service, spreadsheet_id, sheet_name, df):
    if df.empty:
        return
    df.insert(0, "LastSyncedAt", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    values = [df.columns.tolist()] + df.fillna("").values.tolist()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()

# ===== Snapshot Helpers =====
def save_snapshot(tab, df):
    p = SNAPSHOTS_DIR / f"{tab}.json"
    json.dump({"rows": df.fillna('').to_dict('records')}, open(p, "w"), indent=2)

def load_snapshot(tab):
    p = SNAPSHOTS_DIR / f"{tab}.json"
    if p.exists():
        try:
            d = json.load(open(p))
            return pd.DataFrame(d.get("rows", []))
        except:
            return None
    return None

# ===== Incremental Sync =====
def incremental_sync(tab, excel_path, sheet_id, key_col=KEY_COLUMN):
    try:
        df_new = pd.read_excel(excel_path)
        if df_new.empty:
            return {"new": 0, "updated": 0, "skipped": 0}
        if key_col not in df_new.columns:
            key_col = df_new.columns[0]
        df_new[key_col] = df_new[key_col].astype(str).str.strip()
        service = get_sheets_service()
        df_old = read_sheet(service, sheet_id, tab)
        if df_old.empty:
            df_old = load_snapshot(tab) or pd.DataFrame()
        df_old[key_col] = df_old.get(key_col, "").astype(str).str.strip()
        merged, new, updated, skipped = df_old.copy(), 0, 0, 0
        for _, r in df_new.iterrows():
            key = str(r[key_col]).strip()
            match = merged[merged[key_col] == key]
            if match.empty:
                merged = pd.concat([merged, r.to_frame().T], ignore_index=True)
                new += 1
            else:
                idx = match.index[0]
                changed = any(str(merged.at[idx, c]) != str(r[c]) for c in df_new.columns if c != "LastSyncedAt")
                if changed:
                    for c in df_new.columns:
                        merged.at[idx, c] = r[c]
                    updated += 1
                else:
                    skipped += 1
        write_sheet(service, sheet_id, tab, merged)
        save_snapshot(tab, merged)
        return {"new": new, "updated": updated, "skipped": skipped}
    except Exception as e:
        return {"error": str(e)}

# ===== Tab Export Logic =====
async def export_dashboard_tab(page: Page, tab: str, dl_path: Path, sheet_id: str):
    start = datetime.now()
    try:
        logger.info(f"📊 Starting export for tab: {tab}")
        await page.click(f"text='{tab}'", timeout=30000)
        logger.info(f"  → Waiting for tab '{tab}' data to load...")

        # Wait for table up to 35s
        table_loaded = False
        for s in range(35):
            try:
                if await page.locator("table, [role='table'], .table").first().is_visible():
                    table_loaded = True
                    logger.info(f"  ✅ Data table visible after {s+1}s for tab '{tab}'")
                    break
            except:
                pass
            await asyncio.sleep(1)
        if not table_loaded:
            logger.warning(f"⚠️ No visible table after 35s for tab '{tab}'")

        # Extra 30s buffer + heartbeat
        for s in range(30):
            if s % 5 == 0:
                logger.info(f"    ⏳ Still waiting... {s}/30s for tab '{tab}'")
            await asyncio.sleep(1)
        logger.info(f"  ✅ Tab '{tab}' content ready for export")

        # Confirm Export button (25 s window now)
        export_selectors = [
            'button:has-text("Export to excel")',
            'button:has-text("Export to Excel")',
            'a:has-text("Export to excel")',
            'a:has-text("Export to Excel")'
        ]
        export_btn = None
        for sel in export_selectors:
            try:
                await page.wait_for_selector(sel, timeout=25000)
                loc = page.locator(sel).first()
                if await loc.is_visible() and await loc.is_enabled():
                    export_btn = sel
                    logger.info(f"  ✅ Export button ready: {sel}")
                    break
            except:
                continue
        if not export_btn:
            raise Exception("Export button not visible or enabled")

        # Download
        path = dl_path / f"{tab}.xlsx"
        done = asyncio.Event()

        async def on_dl(d: Download):
            await d.save_as(path)
            done.set()

        page.on("download", on_dl)
        await page.click(export_btn)
        await asyncio.wait_for(done.wait(), timeout=EXPORT_TIMEOUT_MS / 1000)
        if not path.exists() or path.stat().st_size == 0:
            raise Exception("Downloaded file missing or empty")

        sync = incremental_sync(tab, path, sheet_id)
        logger.info(f"  ✅ Tab '{tab}' synced: {sync}")
        path.unlink(missing_ok=True)
        return {"tab": tab, "success": True, **sync, "time": f"{(datetime.now()-start).seconds}s"}
    except Exception as e:
        logger.error(f"❌ Tab '{tab}' failed: {e}")
        return {"tab": tab, "success": False, "error": str(e), "time": f"{(datetime.now()-start).seconds}s"}

# ===== Full Export Orchestration =====
async def export_dashboard(session_id: str, sheet_id: str):
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        await page.goto("https://compliancenominationportal.in.pwc.com/dashboard", wait_until="networkidle", timeout=60000)
        await asyncio.sleep(3)

        # Click Advance Search (3 attempts)
        for i in range(3):
            try:
                await page.wait_for_selector('button:has-text("Advance search")', timeout=10000)
                await page.click('button:has-text("Advance search")')
                logger.info(f"Clicked Advance search (attempt {i+1})")
                await asyncio.sleep(3)
                break
            except Exception:
                logger.warning(f"Advance search not clickable (attempt {i+1})")
                await asyncio.sleep(2)

        dl_path = TMP_DIR / "dashboard_exports"
        dl_path.mkdir(exist_ok=True)

        for tab in DASHBOARD_TABS:
            res = await export_dashboard_tab(page, tab, dl_path, sheet_id)
            results.append(res)
            await asyncio.sleep(5)

        await browser.close()

    # Print summary
    success = [r for r in results if r.get("success")]
    fail = [r for r in results if not r.get("success")]
    logger.info("\n" + "=" * 50)
    logger.info(f"📊 EXPORT SUMMARY ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    for r in results:
        status = "✅" if r.get("success") else "❌"
        msg = f"{r['tab']:<25} {status}  ({r.get('time','?')} / {r.get('new',0)} new / {r.get('updated',0)} upd)"
        logger.info(msg)
    logger.info(f"{len(success)}/{len(results)} tabs successful")
    logger.info("=" * 50)

    return {"ok": True, "results": results, "successful": len(success), "failed": len(fail)}

# ===== API Endpoints =====
class ExportRequest(BaseModel):
    session_id: Optional[str] = "latest"
    spreadsheet_id: Optional[str] = None

@app.get("/health")
async def health():
    return {"ok": True, "uptime": int((datetime.now() - start_time).total_seconds())}

@app.post("/export-dashboard")
async def export_dashboard_endpoint(req: ExportRequest):
    try:
        sheet = req.spreadsheet_id or GOOGLE_SHEET_ID
        if not sheet:
            raise HTTPException(status_code=400, detail="Missing spreadsheet_id")
        result = await export_dashboard(req.session_id, sheet)
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Export endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
