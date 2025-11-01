import asyncio, os, json, logging
from pathlib import Path
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from playwright.async_api import async_playwright, Page, Download
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("export_dashboard")

app = FastAPI(title="PwC Dashboard Export API")

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
TMP_DIR = Path("/tmp"); TMP_DIR.mkdir(exist_ok=True)
EXPORT_TIMEOUT_MS = 240000
KEY_COLUMN = "Candidate ID"
DASHBOARD_TABS = [
    "Today's allocated", "Not started", "Draft",
    "Rejected / Insufficient", "Submitted",
    "Work in progress", "BGV closed"
]

def get_sheets_service():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GOOGLE_CREDENTIALS_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)

def write_sheet(service, spreadsheet_id, sheet_name, df):
    values = [df.columns.tolist()] + df.fillna("").values.tolist()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        body={"values": values}
    ).execute()
    logger.info(f"Wrote {len(df)} rows to sheet '{sheet_name}'")

def incremental_sync(tab, path, sid):
    try:
        s = get_sheets_service()
        df = pd.read_excel(path)
        if df.empty: return {"new": 0}
        write_sheet(s, sid, tab, df)
        return {"new": len(df)}
    except Exception as e:
        logger.error(e); return {"error": str(e)}

async def wait(seconds, msg=""):
    logger.info(f"⏳ Waiting {seconds}s {msg}...")
    await asyncio.sleep(seconds)

async def safe_click(page: Page, selectors, label, wait_after=5):
    for attempt in range(3):
        for sel in selectors:
            try:
                await page.wait_for_selector(sel, timeout=8000)
                el = page.locator(sel).first
                if await el.is_visible():
                    await el.scroll_into_view_if_needed()
                    await el.click(force=True)
                    logger.info(f"✅ Clicked {label} using {sel}")
                    await wait(wait_after)
                    return True
            except Exception:
                continue
        try:
            txt = page.get_by_text(label, exact=False)
            if await txt.is_visible():
                await txt.scroll_into_view_if_needed()
                await txt.click(force=True)
                logger.info(f"✅ Force-clicked {label} by text reader")
                await wait(wait_after)
                return True
        except Exception:
            pass
        try:
            el = page.locator(f"text=/{label}/i").first
            box = await el.bounding_box()
            if box:
                await page.mouse.click(box["x"] + box["width"]/2, box["y"] + box["height"]/2)
                logger.info(f"🖱️ Bounding-box click for {label}")
                await wait(wait_after)
                return True
        except Exception:
            pass
        logger.warning(f"Retrying click for {label} ({attempt+1}/3)")
        await wait(3)
    ts = datetime.now().strftime("%H%M%S")
    ss_path = TMP_DIR / f"{label.replace(' ','_')}_fail_{ts}.png"
    await page.screenshot(path=str(ss_path))
    raise Exception(f"{label} not clickable after retries (screenshot: {ss_path})")

async def export_tab(page: Page, tab: str, sid: str, download_dir: Path):
    try:
        await safe_click(page, [f"text=\"{tab}\""], tab, wait_after=30)
        await safe_click(page,
                         ["button:has-text('Export to excel')",
                          "a:has-text('Export to excel')",
                          "text=/Export\\s*to\\s*excel/i"],
                         "Export to Excel", wait_after=5)
        download_event = asyncio.Event()
        file_path = download_dir / f"{tab}.xlsx"
        async def on_download(d: Download):
            await d.save_as(file_path)
            download_event.set()
        page.on("download", on_download)
        await asyncio.wait_for(download_event.wait(), timeout=EXPORT_TIMEOUT_MS/1000)
        await wait(2)
        res = incremental_sync(tab, file_path, sid)
        file_path.unlink(missing_ok=True)
        logger.info(f"✅ Exported {tab}: {res}")
        return res
    except Exception as e:
        logger.error(f"❌ Failed {tab}: {e}")
        return {"error": str(e)}

async def click_advance_search(page: Page):
    await wait(30, "for dashboard to load fully after login")
    await safe_click(page,
        ["button[data-bs-target='#collapse-advance-serach']",
         "button.btn.btn-warning",
         "text=/Advance\\s*search/i"],
        "Advance search", wait_after=30)
    logger.info("✅ Advance Search expanded successfully")

async def perform_logout(page: Page):
    logger.info("🔒 Starting logout...")
    await wait(10)
    await safe_click(page,
        ["span.k-menu-expand-arrow", "a.k-menu-link.k-active", "text=/Welcome/i"],
        "Profile dropdown", wait_after=2)
    await safe_click(page, ["text=/Logout/i"], "Logout", wait_after=5)
    try:
        await page.wait_for_selector("text='You are logged-out successfully!!!'", timeout=10000)
        logger.info("✅ Logout confirmed")
    except Exception:
        logger.warning("⚠️ Logout confirmation not found")

async def export_dashboard(session_id: str, spreadsheet_id: str, storage_state=None):
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        ctx = await browser.new_context(storage_state=storage_state, accept_downloads=True)
        page = await ctx.new_page()
        await page.goto("https://compliancenominationportal.in.pwc.com/dashboard", wait_until="networkidle")
        logger.info("🌐 Dashboard loaded, waiting before clicking Advance Search...")
        await wait(30, "for full dashboard load")
        try:
            await click_advance_search(page)
        except Exception as e:
            logger.error(f"🚫 Critical failure: Advance Search not clickable, stopping export. Reason: {e}")
            await browser.close()
            return {
                "ok": False,
                "error": "Advance Search not clickable — export aborted",
                "details": str(e)
            }
        logger.info("✅ Proceeding to tab exports after successful Advance Search...")
        ddir = TMP_DIR / "exports"; ddir.mkdir(exist_ok=True)
        for tab in DASHBOARD_TABS:
            results.append(await export_tab(page, tab, spreadsheet_id, ddir))
        await perform_logout(page)
        await browser.close()
    return {
        "ok": True,
        "tabs": results,
        "successful": [r for r in results if "error" not in r],
        "failed": [r for r in results if "error" in r]
    }

class ExportRequest(BaseModel):
    session_id: str = "latest"
    spreadsheet_id: str | None = None
    storage_state: dict | None = None

@app.post("/export-dashboard")
async def run_export(req: ExportRequest):
    sid = req.session_id or "latest"
    sheet = req.spreadsheet_id or GOOGLE_SHEET_ID
    if not sheet:
        raise HTTPException(400, "spreadsheet_id required")
    result = await export_dashboard(sid, sheet, req.storage_state)
    return JSONResponse(content=result)

@app.get("/health")
async def health():
    return {"ok": True, "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
