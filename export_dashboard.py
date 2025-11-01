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

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("export_dashboard")

app = FastAPI(title="PwC Dashboard Export API")

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
TMP_DIR = Path("/tmp"); TMP_DIR.mkdir(exist_ok=True)
EXPORT_TIMEOUT_MS = 240000
KEY_COLUMN = "Candidate ID"
DASHBOARD_TABS = [
    "Today's allocated","Not started","Draft",
    "Rejected / Insufficient","Submitted",
    "Work in progress","BGV closed"
]

def get_sheets_service():
    creds_info = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets","v4",credentials=creds)

def write_sheet(service, spreadsheet_id, sheet_name, df):
    values = [df.columns.tolist()] + df.fillna("").values.tolist()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        body={"values":values}).execute()
    logger.info(f"Wrote {len(df)} rows to sheet '{sheet_name}'")

def incremental_sync(tab_name, excel_path, spreadsheet_id):
    try:
        service = get_sheets_service()
        df_new = pd.read_excel(excel_path)
        if df_new.empty: return {"new":0}
        write_sheet(service, spreadsheet_id, tab_name, df_new)
        return {"new":len(df_new)}
    except Exception as e:
        logger.error(e); return {"error":str(e)}

async def wait(seconds,msg=""):
    logger.info(f"⏳ Waiting {seconds}s {msg}...")
    await asyncio.sleep(seconds)

async def click_advance_search(page:Page):
    await wait(30,"for dashboard to load fully after login")
    logger.info("🔍 Clicking Advance Search...")
    selectors=[
        "button[data-bs-target='#collapse-advance-serach']",
        "button.btn.btn-warning",
        "button:has-text('Advance search')",
        "text='Advance search'"
    ]
    for attempt in range(3):
        for sel in selectors:
            try:
                await page.wait_for_selector(sel,timeout=8000)
                btn=page.locator(sel).first
                await btn.scroll_into_view_if_needed()
                await btn.click(force=True)
                logger.info(f"Clicked Advance Search using {sel}")
                await wait(30,"for Export button to appear")
                exp=["button:has-text('Export to excel')","a:has-text('Export to excel')"]
                for e in exp:
                    try:
                        await page.wait_for_selector(e,timeout=5000)
                        logger.info("✅ Export button visible after Advance Search")
                        return True
                    except: continue
            except: continue
        logger.warning(f"Retrying Advance Search click {attempt+1}/3")
    raise Exception("Advance Search not clickable after retries")

async def export_tab(page:Page,tab:str,spreadsheet_id:str,download_path:Path):
    logger.info(f"📊 Exporting tab: {tab}")
    try:
        await page.wait_for_selector(f"text=\"{tab}\"",timeout=30000)
        await page.click(f"text=\"{tab}\"")
        await wait(30,f"after selecting tab {tab}")
        exp="button:has-text('Export to excel')"
        await page.wait_for_selector(exp,timeout=15000)
        download_event=asyncio.Event(); file_path=download_path/f"{tab}.xlsx"
        async def on_download(d:Download):
            await d.save_as(file_path); download_event.set()
        page.on("download",on_download)
        await page.click(exp)
        await asyncio.wait_for(download_event.wait(),timeout=EXPORT_TIMEOUT_MS/1000)
        await wait(2)
        if not file_path.exists() or file_path.stat().st_size==0:
            raise Exception("File not found/empty")
        res=incremental_sync(tab,file_path,spreadsheet_id)
        file_path.unlink(missing_ok=True)
        logger.info(f"✅ {tab} exported: {res}")
        return res
    except Exception as e:
        logger.error(f"❌ {tab} failed: {e}")
        return {"error":str(e)}

async def perform_logout(page:Page):
    logger.info("🔒 Logging out...")
    await wait(10,"before logout")
    dropdown_selectors=[
        "span.k-menu-expand-arrow",
        "a.k-menu-link.k-active",
        "text='Welcome'"
    ]
    for sel in dropdown_selectors:
        try:
            await page.wait_for_selector(sel,timeout=8000)
            await page.locator(sel).click(force=True)
            await wait(2)
            break
        except: continue
    try:
        await page.click("text='Logout'",timeout=8000)
        await wait(5)
        await page.wait_for_selector("text='You are logged-out successfully!!!'",timeout=10000)
        logger.info("✅ Logout successful")
    except Exception as e:
        logger.error(f"Logout failed: {e}")

async def export_dashboard(session_id:str,spreadsheet_id:str,storage_state=None):
    results=[]
    async with async_playwright() as p:
        browser=await p.chromium.launch(headless=True,args=['--no-sandbox'])
        context=await browser.new_context(storage_state=storage_state,accept_downloads=True)
        page=await context.new_page()
        await page.goto("https://compliancenominationportal.in.pwc.com/dashboard",wait_until="networkidle")
        await click_advance_search(page)
        download_path=TMP_DIR/"exports"; download_path.mkdir(exist_ok=True)
        for tab in DASHBOARD_TABS:
            results.append(await export_tab(page,tab,spreadsheet_id,download_path))
        await perform_logout(page)
        await browser.close()
    return {"ok":True,"tabs":results}

class ExportRequest(BaseModel):
    session_id:str="latest"
    spreadsheet_id:str=None
    storage_state:dict|None=None

@app.post("/export-dashboard")
async def run_export(req:ExportRequest):
    sid=req.session_id or "latest"
    sid_val=req.spreadsheet_id or GOOGLE_SHEET_ID
    if not sid_val: raise HTTPException(400,"Missing spreadsheet id")
    result=await export_dashboard(sid,sid_val,req.storage_state)
    return JSONResponse(content=result)

@app.get("/health")
async def health(): return {"ok":True,"uptime":datetime.now().isoformat()}

if __name__=="__main__":
    import uvicorn; uvicorn.run(app,host="0.0.0.0",port=int(os.getenv("PORT",8000)))
