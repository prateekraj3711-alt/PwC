import asyncio
import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict

import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from playwright.async_api import async_playwright, Page, Download
from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="PwC Export Dashboard")

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
TMP_DIR = Path("/tmp")
SESSION_PATH = TMP_DIR / "pwc"
SNAPSHOTS_DIR = TMP_DIR / "snapshots"
SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
SESSION_PATH.mkdir(parents=True, exist_ok=True)

EXPORT_TIMEOUT = 240
TABS = [
    "Today's allocated",
    "Not started",
    "Draft",
    "Rejected / Insufficient",
    "Submitted",
    "Work in progress",
    "BGV closed",
]


def get_sheets_service():
    creds_json = GOOGLE_CREDENTIALS_JSON
    creds_info = json.loads(creds_json.replace("\\n", "\n")) if "\\n" in creds_json else json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


async def click_force(page: Page, selector: str, timeout=5000, name="element"):
    for attempt in range(3):
        try:
            await page.wait_for_selector(selector, timeout=timeout)
            el = page.locator(selector).first()
            await el.scroll_into_view_if_needed()
            await asyncio.sleep(1)
            await el.click(force=True)
            logger.info(f"✅ Clicked {name} via {selector}")
            return True
        except Exception as e:
            logger.warning(f"Retrying click for {name} ({attempt+1}/3): {e}")
            await asyncio.sleep(3)
    screenshot_path = f"/tmp/{name}_fail_{datetime.now().strftime('%H%M%S')}.png"
    await page.screenshot(path=screenshot_path)
    raise Exception(f"{name} not clickable after retries (screenshot: {screenshot_path})")


async def wait_full_load(page: Page, seconds=30, name="page"):
    logger.info(f"⏳ Waiting {seconds}s for {name} to load fully...")
    await asyncio.sleep(seconds)
    await page.wait_for_load_state("networkidle")
    await asyncio.sleep(2)


async def click_advance_search(page: Page):
    await wait_full_load(page, 30, "dashboard")
    selectors = [
        'button[data-bs-target="#collapse-advance-serach"]',
        'button:has-text("Advance search")',
        'text="Advance search"',
    ]
    for sel in selectors:
        try:
            await click_force(page, sel, name="Advance_search")
            logger.info("✅ Advance Search clicked successfully")
            return
        except Exception:
            continue
    raise Exception("Advance Search not clickable after retries")


async def export_tab(page: Page, tab_name: str, download_dir: Path):
    logger.info(f"📊 Exporting tab: {tab_name}")
    await click_force(page, f'text="{tab_name}"', name=f"tab_{tab_name}")
    await wait_full_load(page, 25, f"tab {tab_name}")

    export_selectors = [
        'button:has-text("Export to excel")',
        'button:has-text("Export to Excel")',
        'a:has-text("Export to excel")',
    ]
    export_sel = None
    for sel in export_selectors:
        try:
            await page.wait_for_selector(sel, timeout=5000)
            export_sel = sel
            break
        except Exception:
            continue
    if not export_sel:
        raise Exception(f"Export button not visible for {tab_name}")

    download_event = asyncio.Event()
    file_path = download_dir / f"{tab_name}.xlsx"
    downloaded_file = []

    async def handle_download(d: Download):
        await d.save_as(file_path)
        downloaded_file.append(file_path)
        download_event.set()
        logger.info(f"💾 Download saved: {file_path}")

    page.on("download", handle_download)
    await click_force(page, export_sel, name=f"Export_{tab_name}")
    try:
        await asyncio.wait_for(download_event.wait(), timeout=EXPORT_TIMEOUT)
    except asyncio.TimeoutError:
        raise Exception(f"Download timeout for {tab_name}")

    await asyncio.sleep(2)
    if not file_path.exists() or file_path.stat().st_size == 0:
        raise Exception(f"File missing or empty for {tab_name}")

    await asyncio.sleep(1)
    logger.info(f"✅ Export completed for {tab_name} ({file_path.stat().st_size} bytes)")
    return {"tab": tab_name, "status": "done"}


async def perform_logout(page: Page):
    try:
        logger.info("🔒 Attempting logout via top-right dropdown...")
        await wait_full_load(page, 5, "pre-logout")
        await click_force(page, "button.dropdown-toggle", name="Profile_dropdown")
        await asyncio.sleep(2)
        await click_force(page, 'a:has-text("Logout")', name="Logout")
        await asyncio.sleep(5)
        await page.wait_for_selector('text="You are logged-out successfully!!!"', timeout=15000)
        logger.info("✅ Logout confirmed successfully")
    except Exception as e:
        logger.error(f"Logout failed: {e}")
        raise


async def export_dashboard(session_id: str, spreadsheet_id: str, storage_state: Optional[Dict] = None):
    try:
        if not storage_state:
            session_file = SESSION_PATH / f"{session_id}.json"
            try:
                with open(session_file, "r") as f:
                    storage_state = json.load(f)
                    logger.info(f"Loaded session from local file: {session_file}")
            except FileNotFoundError:
                logger.warning(f"Session file missing locally, fetching from login API for {session_id}")
                try:
                    r = requests.get(f"https://pwc-twhw.onrender.com/status/{session_id}", timeout=10)
                    r.raise_for_status()
                    data = r.json()
                    storage_state = data["result"].get("session_state") or data["result"].get("storage_state")
                    if not storage_state:
                        raise Exception("session_state missing in response")
                    logger.info(f"Fetched session from login service for {session_id}")
                except Exception as e:
                    raise FileNotFoundError(f"Could not load session: {e}")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context(storage_state=storage_state, accept_downloads=True)
            page = await context.new_page()
            await page.goto("https://compliancenominationportal.in.pwc.com/dashboard", wait_until="networkidle")
            await click_advance_search(page)

            download_dir = TMP_DIR / "dashboard_exports"
            download_dir.mkdir(parents=True, exist_ok=True)

            results = []
            for tab in TABS:
                try:
                    result = await export_tab(page, tab, download_dir)
                    results.append(result)
                    await asyncio.sleep(25)
                except Exception as e:
                    logger.error(f"❌ Error on {tab}: {e}")
                    await page.screenshot(path=f"/tmp/{tab}_fail_{datetime.now().strftime('%H%M%S')}.png")

            await perform_logout(page)
            await browser.close()
            return {"ok": True, "tabs": results}

    except Exception as e:
        logger.error(f"Export error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class ExportRequest(BaseModel):
    session_id: str
    spreadsheet_id: str


@app.post("/export-dashboard")
async def export_endpoint(req: ExportRequest):
    result = await export_dashboard(req.session_id, req.spreadsheet_id)
    return JSONResponse(content=result)


@app.get("/screenshots/{filename}")
async def get_screenshot(filename: str):
    file_path = Path(f"/tmp/{filename}")
    if file_path.exists():
        return FileResponse(file_path)
    raise HTTPException(status_code=404, detail="Screenshot not found")


@app.get("/test-sheets")
async def test_sheets():
    try:
        if not GOOGLE_SHEET_ID:
            raise HTTPException(status_code=400, detail="GOOGLE_SHEET_ID required")
        service = get_sheets_service()
        test_row = ["✅ Connection test", datetime.now().isoformat()]
        sheet_name = "TestConnection"
        range_name = f"{sheet_name}!A:B"
        service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=range_name,
            valueInputOption="RAW",
            body={"values": [test_row]},
        ).execute()
        logger.info(f"✅ Test row written to {GOOGLE_SHEET_ID} / {sheet_name}")
        return JSONResponse(content={
            "ok": True,
            "message": "Write test successful",
            "spreadsheet_id": GOOGLE_SHEET_ID,
            "sheet": sheet_name
        })
    except Exception as e:
        logger.error(f"Test Sheets error: {e}")
        raise HTTPException(status_code=500, detail=f"Test failed: {e}")


@app.get("/health")
async def health():
    return {"ok": True, "timestamp": datetime.utcnow().isoformat()}
