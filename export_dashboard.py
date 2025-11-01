"""
Final export_dashboard.py
- Playwright (headless) + FastAPI orchestration to export PwC dashboard tabs
- Incremental sync to Google Sheets using service account JSON in env var GOOGLE_CREDENTIALS_JSON
- Mandatory logout via top-right profile -> Logout (validates signout page)
- After logout, navigates to fresh login page (BGVAdmin/BGVDashboard)
"""

import asyncio
import os
import json
import logging
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from playwright.async_api import async_playwright, Page, Download
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("export_dashboard")

# ---------- App ----------
app = FastAPI(title="PwC Dashboard Export API")
start_time = datetime.now()

# ---------- Config (env) ----------
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
SESSION_STORAGE_PATH = os.getenv("SESSION_STORAGE_PATH", "/tmp/pwc")
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
EXPORT_TIMEOUT_MS = int(os.getenv("EXPORT_TIMEOUT_MS_MS", 240000))  # ms
KEY_COLUMN = os.getenv("KEY_COLUMN", "Candidate ID")

# Tabs (order matters)
DASHBOARD_TABS = [
    "Today's allocated",
    "Not started",
    "Draft",
    "Rejected / Insufficient",
    "Submitted",
    "Work in progress",
    "BGV closed",
]

TMP_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOTS_DIR = TMP_DIR / "snapshots"
SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)

# URLs (from your message)
SIGNOUT_URL = "https://compliancenominationportal.in.pwc.com/Login/Signout"
LOGIN_PAGE_URL = "https://compliancenominationportal.in.pwc.com/BGVAdmin/BGVDashboard"
DASHBOARD_URL = "https://compliancenominationportal.in.pwc.com/dashboard"

# ---------- Google Sheets helpers ----------
def get_sheets_service():
    if not GOOGLE_CREDENTIALS_JSON:
        raise FileNotFoundError("GOOGLE_CREDENTIALS_JSON environment variable is missing")
    try:
        # Some deployments escape newline chars; normalize them
        creds_info = json.loads(GOOGLE_CREDENTIALS_JSON.replace("\\n", "\n"))
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        service = build("sheets", "v4", credentials=creds)
        logger.info("Google Sheets service initialized from GOOGLE_CREDENTIALS_JSON")
        return service
    except Exception as e:
        logger.error(f"Failed to initialize Google Sheets service: {e}")
        raise

def read_sheet(service, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    try:
        resp = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A:ZZ").execute()
        values = resp.get("values", [])
        if not values:
            return pd.DataFrame()
        headers = values[0]
        rows = values[1:]
        df = pd.DataFrame(rows, columns=headers)
        return df
    except Exception as e:
        logger.warning(f"Could not read sheet '{sheet_name}': {e}")
        return pd.DataFrame()

def write_sheet(service, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame):
    try:
        # ensure LastSyncedAt exists
        if "LastSyncedAt" not in df.columns:
            df.insert(0, "LastSyncedAt", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        else:
            df["LastSyncedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        values = [df.columns.tolist()] + df.fillna("").values.tolist()
        service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1", valueInputOption="RAW", body={"values": values}).execute()
        logger.info(f"Wrote {len(df)} rows to sheet '{sheet_name}'")
    except Exception as e:
        logger.error(f"Failed to write sheet '{sheet_name}': {e}")
        raise

# snapshot helpers
def save_snapshot(tab_name: str, df: pd.DataFrame):
    p = SNAPSHOTS_DIR / f"{tab_name}.json"
    try:
        data = {"timestamp": datetime.now().isoformat(), "rows": df.fillna("").to_dict("records")}
        p.write_text(json.dumps(data, indent=2))
        logger.info(f"Saved snapshot for '{tab_name}'")
    except Exception as e:
        logger.warning(f"Failed to save snapshot for '{tab_name}': {e}")

def load_snapshot(tab_name: str) -> Optional[pd.DataFrame]:
    p = SNAPSHOTS_DIR / f"{tab_name}.json"
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text())
        rows = data.get("rows", [])
        return pd.DataFrame(rows)
    except Exception as e:
        logger.warning(f"Failed to load snapshot for '{tab_name}': {e}")
        return None

# incremental sync
def incremental_sync(tab_name: str, excel_path: Path, spreadsheet_id: str, key_col: str = KEY_COLUMN) -> Dict:
    try:
        service = get_sheets_service()
        df_new = pd.read_excel(excel_path)
        if df_new.empty:
            logger.warning(f"Excel for '{tab_name}' is empty")
            return {"new": 0, "updated": 0, "skipped": 0}
        if key_col not in df_new.columns:
            key_col = df_new.columns[0]
            logger.info(f"Key column not found, using '{key_col}'")
        df_new[key_col] = df_new[key_col].astype(str).str.strip()
        df_existing = read_sheet(service, spreadsheet_id, tab_name)
        if df_existing.empty:
            # try snapshot
            snap = load_snapshot(tab_name)
            df_existing = snap if snap is not None else pd.DataFrame()
        # normalize existing key
        if not df_existing.empty and key_col in df_existing.columns:
            df_existing[key_col] = df_existing[key_col].astype(str).str.strip()
        # merge: keep latest per key (new takes precedence)
        if df_existing.empty:
            merged = df_new.copy()
            new_count = len(merged)
            updated_count = 0
            skipped_count = 0
        else:
            # make index by key
            merged = df_existing.copy()
            new_count = 0
            updated_count = 0
            skipped_count = 0
            for _, row in df_new.iterrows():
                key = str(row[key_col]).strip()
                mask = merged[merged[key_col] == key] if key_col in merged.columns else merged.iloc[0:0]
                if mask.empty:
                    merged = pd.concat([merged, row.to_frame().T], ignore_index=True)
                    new_count += 1
                else:
                    idx = mask.index[0]
                    # detect changes
                    changed = False
                    for c in df_new.columns:
                        old = merged.at[idx, c] if c in merged.columns else ""
                        newv = row[c]
                        if str(old).strip() != str(newv).strip():
                            changed = True
                            break
                    if changed:
                        for c in df_new.columns:
                            merged.at[idx, c] = row[c]
                        updated_count += 1
                    else:
                        skipped_count += 1
        # write back
        write_sheet(service, spreadsheet_id, tab_name, merged)
        save_snapshot(tab_name, merged)
        logger.info(f"Sync '{tab_name}': new={new_count} updated={updated_count} skipped={skipped_count}")
        return {"new": int(new_count), "updated": int(updated_count), "skipped": int(skipped_count)}
    except Exception as e:
        logger.error(f"Incremental sync error for '{tab_name}': {e}")
        return {"new": 0, "updated": 0, "skipped": 0, "error": str(e)}

# ---------- Playwright helpers ----------
async def wait_for_table_with_heartbeat(page: Page, max_wait_s: int = 35, extra_buffer_s: int = 30):
    """Wait up to max_wait_s for a table/grid, then extra_buffer_s with heartbeat logs."""
    table_found = False
    for sec in range(max_wait_s):
        try:
            # common table selectors
            loc = page.locator("table, [role='table'], .table").first()
            if await loc.is_visible():
                logger.info(f"  ✓ Data table detected after {sec+1}s")
                table_found = True
                break
        except Exception:
            pass
        await asyncio.sleep(1)
    if not table_found:
        logger.warning(f"  ⚠️ No visible table after {max_wait_s}s — proceeding with caution")

    logger.info(f"  → Extra {extra_buffer_s}s buffer for data to populate (heartbeat every 5s)...")
    for s in range(extra_buffer_s):
        if s % 5 == 0:
            logger.info(f"    ⏳ Still waiting... {s}/{extra_buffer_s}s")
        await asyncio.sleep(1)

async def find_and_confirm_export_button(page: Page, timeout_ms: int = 25000):
    """Find an Export to Excel button and confirm it's visible/enabled within timeout."""
    export_selectors = [
        'button:has-text("Export to excel")',
        'button:has-text("Export to Excel")',
        'a:has-text("Export to excel")',
        'a:has-text("Export to Excel")',
        '[aria-label*="Export" i]',
        'button[title*="Export" i]'
    ]
    for sel in export_selectors:
        try:
            await page.wait_for_selector(sel, timeout=timeout_ms)
            loc = page.locator(sel).first()
            if await loc.is_visible() and await loc.is_enabled():
                logger.info(f"  ✅ Export button visible & enabled: {sel}")
                return sel
        except Exception:
            continue
    raise Exception("Export button not found or not enabled within timeout")

async def export_dashboard_tab(page: Page, tab_name: str, download_path: Path, spreadsheet_id: str):
    start_ts = datetime.now()
    logger.info(f"📊 Starting export for tab: {tab_name}")
    try:
        # Click the tab text (the blue region is clickable as well)
        tab_selector = f'text="{tab_name}"'
        await page.wait_for_selector(tab_selector, timeout=30000)
        await page.click(tab_selector)
        logger.info(f"  → Clicked tab '{tab_name}'")

        # Wait for the tab to load (table detection + buffer with heartbeat)
        await wait_for_table_with_heartbeat(page, max_wait_s=35, extra_buffer_s=30)

        # Confirm Export button (25s window)
        export_sel = await find_and_confirm_export_button(page, timeout_ms=25000)

        # Prepare download listener and click
        file_path = download_path / f"{tab_name}.xlsx"
        download_event = asyncio.Event()
        async def _on_download(d: Download):
            try:
                await d.save_as(file_path)
                download_event.set()
                logger.info(f"  ✓ Download saved to {file_path}")
            except Exception as e:
                logger.error(f"  ✗ Error saving download: {e}")
                download_event.set()
        page.on("download", _on_download)

        # Click export
        await page.click(export_sel)
        logger.info("  → Clicked Export to Excel, waiting for download...")
        try:
            await asyncio.wait_for(download_event.wait(), timeout=EXPORT_TIMEOUT_MS / 1000)
        except asyncio.TimeoutError:
            raise Exception("Download timeout")

        # verify file
        if not file_path.exists() or file_path.stat().st_size == 0:
            raise Exception("Downloaded file missing or empty")

        # incremental sync
        result = incremental_sync(tab_name, file_path, spreadsheet_id, key_col=KEY_COLUMN)
        logger.info(f"  ✅ Sync result for '{tab_name}': {result}")

        # cleanup
        try:
            file_path.unlink(missing_ok=True)
        except Exception:
            pass

        elapsed = (datetime.now() - start_ts).seconds
        return {"tab": tab_name, "ok": True, "time_s": elapsed, **result}
    except Exception as e:
        elapsed = (datetime.now() - start_ts).seconds
        logger.error(f"❌ Error processing tab '{tab_name}': {e}")
        return {"tab": tab_name, "ok": False, "time_s": elapsed, "error": str(e)}

# ---------- Orchestration ----------
async def export_dashboard(session_id: str, spreadsheet_id: str, storage_state: Optional[Dict] = None):
    """
    session_id: 'latest' or explicit filename (without .json) to find storage_state
    spreadsheet_id: Google Sheets ID
    storage_state: optional pre-loaded Playwright storage state JSON
    """
    # Determine storage_state: prefer provided, else read from SESSION_STORAGE_PATH/latest or file
    if storage_state:
        logger.info("Using storage_state provided in request body")
    else:
        # attempt to read session file
        if session_id and session_id != "latest":
            candidate = Path(SESSION_STORAGE_PATH) / f"{session_id}.json"
            if not candidate.exists():
                raise FileNotFoundError(f"Session file not found: {candidate}")
            storage_state = json.loads(candidate.read_text())
            logger.info(f"Loaded storage_state from {candidate}")
        else:
            # find latest in SESSION_STORAGE_PATH
            base = Path(SESSION_STORAGE_PATH)
            session_file = None
            if base.exists():
                files = sorted(base.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
                if files:
                    session_file = files[0]
            if not session_file:
                raise FileNotFoundError(f"No session storage file found in {SESSION_STORAGE_PATH}")
            storage_state = json.loads(session_file.read_text())
            logger.info(f"Using latest session: {session_file.name}")

    download_path = TMP_DIR / "dashboard_exports"
    download_path.mkdir(parents=True, exist_ok=True)

    tab_results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-setuid-sandbox"])
        context = await browser.new_context(storage_state=storage_state, accept_downloads=True)
        page = await context.new_page()

        # go to dashboard
        await page.goto(DASHBOARD_URL, wait_until="networkidle", timeout=60000)
        await asyncio.sleep(3)

        # Ensure Advance Search expanded
        advance_selectors = [
            'button:has-text("Advance search")',
            'text="Advance search"',
            'button[aria-controls*="advance"]'
        ]
        clicked_advance = False
        for attempt in range(3):
            for sel in advance_selectors:
                try:
                    loc = page.locator(sel).first()
                    if await loc.is_visible():
                        await loc.click()
                        clicked_advance = True
                        logger.info(f"Clicked Advance search using {sel} (attempt {attempt+1})")
                        await asyncio.sleep(2)
                        break
                except Exception:
                    continue
            if clicked_advance:
                break
            await asyncio.sleep(2)
        if not clicked_advance:
            logger.warning("Advance search not clickable after 3 attempts — proceeding but exports may fail")

        # Process tabs one by one
        for tab in DASHBOARD_TABS:
            res = await export_dashboard_tab(page, tab, download_path, spreadsheet_id)
            tab_results.append(res)
            # small breathing room before next tab
            await asyncio.sleep(5)

        # ---------- Mandatory logout sequence ----------
        # Click profile dropdown and logout, confirm signout message, then navigate to fresh login page
        try:
            logger.info("🔒 Attempting mandatory logout via top-right dropdown (Kendo UI menu)")

            # dropdown trigger selectors found in DOM
            dropdown_selectors = [
                'a.k-link.k-menu-link.k-active',
                '.k-link.k-menu-link:has-text("Welcome")',
                'a:has-text("Welcome")',
                '.k-menu-item.k-last a.k-link',
                '.text-end .dropdown-toggle'
            ]
            logout_selectors = [
                '.k-animation-container-shown a:has-text("Logout")',
                'a:has-text("Logout")',
                '.dropdown-menu a:has-text("Logout")',
                'text="Logout"'
            ]

            clicked_dropdown = False
            for attempt in range(3):
                for sel in dropdown_selectors:
                    try:
                        elem = page.locator(sel).first()
                        if await elem.is_visible():
                            await elem.click()
                            clicked_dropdown = True
                            logger.info(f"Clicked profile dropdown using selector: {sel}")
                            await asyncio.sleep(2)
                            break
                    except Exception as e:
                        logger.debug(f"Dropdown selector failed ({sel}): {e}")
                if clicked_dropdown:
                    break
                await asyncio.sleep(2)

            if not clicked_dropdown:
                raise Exception("Could not open profile dropdown for logout")

            logout_clicked = False
            for sel in logout_selectors:
                try:
                    elem = page.locator(sel).first()
                    if await elem.is_visible():
                        await elem.click()
                        logout_clicked = True
                        logger.info(f"Clicked Logout using selector: {sel}")
                        break
                except Exception as e:
                    logger.debug(f"Logout selector failed ({sel}): {e}")

            if not logout_clicked:
                raise Exception("Logout link not found or not clickable inside dropdown")

            # wait for navigation; signout page is known to be /Login/Signout
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            await asyncio.sleep(2)

            # Confirm signout message or signout URL
            signout_detected = False
            try:
                # check for the signout message element
                signout_msg = page.locator('.cust_singout')
                if await signout_msg.count() > 0 and await signout_msg.first().is_visible():
                    txt = (await signout_msg.first().inner_text()).strip().lower()
                    if "logged-out" in txt or "logged out" in txt or "logged-out sucessfully" in txt or "logged-out successfully" in txt:
                        signout_detected = True
                        logger.info("Signout confirmation message detected on page.")
            except Exception:
                pass

            # also accept URL check
            try:
                curr = page.url
                if SIGNOUT_URL.lower() in curr.lower() or "/Login/Signout".lower() in curr.lower():
                    signout_detected = True
                    logger.info(f"Signout URL detected: {curr}")
            except Exception:
                pass

            if not signout_detected:
                raise Exception("Signout confirmation not detected (message or URL)")

            # Navigate to fresh login page for next run
            logger.info("Redirecting to fresh login page for next run...")
            await page.goto(LOGIN_PAGE_URL, wait_until="networkidle", timeout=60000)
            await asyncio.sleep(3)
            # verify basic login element presence
            try:
                if await page.locator('input[name="username"]').first().is_visible():
                    logger.info("Fresh login page loaded successfully.")
                else:
                    logger.warning("Login page loaded but expected username input not visible.")
            except Exception:
                logger.info("Login page navigation attempted; skipping visibility strict check.")

        except Exception as e:
            # mandatory logout failed -> close browser and raise to fail API
            logger.error(f"Mandatory logout failed: {e}")
            await context.close()
            await browser.close()
            raise Exception(f"Logout verification failed: {e}")

        # close browser cleanly
        await context.close()
        await browser.close()

    # prepare summary
    success_count = sum(1 for r in tab_results if r.get("ok"))
    failure_count = len(tab_results) - success_count
    logger.info("\n" + "="*50)
    logger.info(f"EXPORT SUMMARY ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})")
    for r in tab_results:
        status = "✅" if r.get("ok") else "❌"
        logger.info(f"{r.get('tab'):<28} {status}  time={r.get('time_s','?')}s  details={r.get('error', r)}")
    logger.info(f"{success_count}/{len(tab_results)} tabs successful")
    logger.info("="*50 + "\n")

    return {"ok": True, "tabs": tab_results, "successful": success_count, "failed": failure_count}

# ---------- API Endpoints ----------
class ExportRequest(BaseModel):
    session_id: Optional[str] = "latest"
    spreadsheet_id: Optional[str] = None
    storage_state: Optional[Dict] = None

@app.post("/export-dashboard")
async def export_dashboard_endpoint(req: ExportRequest):
    sheet_id = req.spreadsheet_id or GOOGLE_SHEET_ID
    if not sheet_id:
        raise HTTPException(status_code=400, detail="spreadsheet_id or GOOGLE_SHEET_ID env var required")
    try:
        result = await export_dashboard(req.session_id or "latest", sheet_id, req.storage_state)
        return JSONResponse(content=result)
    except FileNotFoundError as e:
        logger.error(f"Session file missing: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Export endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    uptime = int((datetime.now() - start_time).total_seconds())
    return {"ok": True, "uptime": uptime}

# allow direct run
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("export_dashboard:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)), log_level="info")
