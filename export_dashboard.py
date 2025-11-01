import asyncio, os, json, logging, glob
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from playwright.async_api import async_playwright, Page, Download
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI(title="PwC Dashboard Export API")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("export_dashboard")

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
EXPORT_TIMEOUT_MS = 240000
TMP_DIR = Path("/tmp")
SNAPSHOT_DIR = TMP_DIR / "snapshots"
SNAPSHOT_DIR.mkdir(exist_ok=True)
KEY_COLUMN = "Candidate ID"
DASHBOARD_TABS = [
    "Today's allocated", "Not started", "Draft",
    "Rejected / Insufficient", "Submitted",
    "Work in progress", "BGV closed"
]


def get_sheets_service():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_JSON), scopes=scopes)
    return build("sheets", "v4", credentials=creds)


async def safe_click(page: Page, selectors: list[str], label: str, post_wait: int = 25):
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        logger.info(f"🔁 Trying to click {label} ({attempt}/{max_retries})")
        try:
            for selector in selectors:
                try:
                    el = page.locator(selector).first
                    await el.scroll_into_view_if_needed()
                    await asyncio.sleep(1)
                    await el.click(timeout=5000)
                    logger.info(f"✅ Clicked {label} normally: {selector}")
                    await asyncio.sleep(post_wait)
                    return
                except Exception:
                    pass
            clicked = await page.evaluate(f"""
                () => {{
                    const nodes = [...document.querySelectorAll('*')];
                    for (const el of nodes) {{
                        const text = (el.innerText || '').toLowerCase();
                        if (text.includes("{label.lower()}")) {{
                            el.scrollIntoView({{behavior:'smooth', block:'center'}});
                            el.click();
                            return true;
                        }}
                    }}
                    return false;
                }}
            """)
            if clicked:
                logger.info(f"✅ Force-clicked {label} via JS")
                await asyncio.sleep(post_wait)
                return
        except Exception as e:
            logger.warning(f"{label} click attempt {attempt} failed: {e}")
        await asyncio.sleep(3)
    screenshot = f"/tmp/{label.replace(' ', '_')}_fail_{datetime.now().strftime('%H%M%S')}.png"
    await page.screenshot(path=screenshot)
    logger.error(f"🚫 Failed to click {label} after retries — screenshot: {screenshot}")
    raise Exception(f"{label} not clickable after retries")


async def export_dashboard(session_id: str, spreadsheet_id: str):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context(storage_state=f"/tmp/pwc/{session_id}.json", accept_downloads=True)
        page = await context.new_page()
        await page.goto("https://compliancenominationportal.in.pwc.com/dashboard", wait_until="networkidle")
        logger.info("🌐 Dashboard loaded — waiting 30s for full render")
        await asyncio.sleep(30)

        # --- Step 1: Click Advance Search ---
        await safe_click(
            page,
            [
                'button:has-text("Advance search")',
                'button.btn-warning',
                '[data-bs-target="#collapse-advance-serach"]',
                'text=/Advance\\s*search/i'
            ],
            label="Advance search",
            post_wait=30
        )

        results = []
        for tab in DASHBOARD_TABS:
            try:
                await safe_click(page, [f'text="{tab}"'], tab)
                await safe_click(page, ['button:has-text("Export to excel")', 'a:has-text("Export to excel")'], "Export to excel", post_wait=30)
                results.append({"tab": tab, "status": "success"})
            except Exception as e:
                logger.error(f"❌ Tab '{tab}' failed: {e}")
                results.append({"tab": tab, "status": "failed", "error": str(e)})

        # --- Step 2: Logout ---
        try:
            await safe_click(
                page,
                [
                    'text="Welcome Sukrutha CR"',
                    'button:has-text("Sukrutha CR")',
                    'div:has-text("Sukrutha CR")',
                    'button:has(.fa-angle-down)'
                ],
                label="Profile dropdown",
                post_wait=10
            )
            await safe_click(page, ['text="Logout"', 'a:has-text("Logout")', 'button:has-text("Logout")'], "Logout", post_wait=5)
            await page.wait_for_url("**/Login/Signout", timeout=15000)
            logout_confirm = await page.is_visible("text='You are logged-out successfully!!!'")
            if logout_confirm:
                logger.info("✅ Logout confirmed.")
            else:
                logger.warning("⚠️ Logout page loaded but confirmation text not visible.")
        except Exception as e:
            logger.error(f"🚫 Logout failed: {e}")
            screenshot = f"/tmp/logout_fail_{datetime.now().strftime('%H%M%S')}.png"
            await page.screenshot(path=screenshot)

        await browser.close()
        return {"ok": True, "tabs": results}


class ExportRequest(BaseModel):
    session_id: str
    spreadsheet_id: Optional[str] = GOOGLE_SHEET_ID


@app.post("/export-dashboard")
async def export_dashboard_endpoint(req: ExportRequest):
    try:
        result = await export_dashboard(req.session_id, req.spreadsheet_id)
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Export error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/screenshots")
async def list_screenshots():
    files = glob.glob("/tmp/*.png")
    return {"ok": True, "screenshots": [os.path.basename(f) for f in files]}


@app.get("/screenshots/{filename}")
async def get_screenshot(filename: str):
    path = f"/tmp/{filename}"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Screenshot not found")
    return FileResponse(path, media_type="image/png")


@app.get("/health")
async def health():
    return {"ok": True, "timestamp": datetime.now().isoformat()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
