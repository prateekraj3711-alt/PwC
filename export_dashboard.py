import asyncio, os, json, logging
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="PwC Dashboard Export API")

start_time = datetime.now()
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
SNAPSHOTS_DIR = TMP_DIR / "snapshots"
SESSION_STORAGE_PATH = os.getenv("SESSION_STORAGE_PATH", "/tmp/pwc")
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
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = None
    if GOOGLE_CREDENTIALS_JSON:
        creds_info = json.loads(GOOGLE_CREDENTIALS_JSON.replace('\\n', '\n'))
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    else:
        creds = service_account.Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH, scopes=scopes)
    return build("sheets", "v4", credentials=creds)

def read_sheet(service, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    try:
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A:ZZ").execute()
        values = result.get('values', [])
        if not values:
            return pd.DataFrame()
        headers = values[0]
        data = values[1:]
        padded = [r + [''] * (len(headers) - len(r)) for r in data]
        return pd.DataFrame(padded, columns=headers)
    except:
        return pd.DataFrame()

def write_sheet(service, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame):
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    names = [s['properties']['title'] for s in spreadsheet.get('sheets', [])]
    if sheet_name not in names:
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={'requests':[{'addSheet':{'properties':{'title':sheet_name}}}]}).execute()
    if df.empty:
        values = [[]]
    else:
        if "LastSyncedAt" not in df.columns:
            df.insert(0,"LastSyncedAt",datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        else:
            df["LastSyncedAt"]=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        values=[df.columns.tolist()]+df.fillna('').values.tolist()
    service.spreadsheets().values().update(spreadsheetId=spreadsheet_id,range=f"{sheet_name}!A1",valueInputOption='RAW',body={'values':values}).execute()

def load_snapshot(tab_name:str)->Optional[pd.DataFrame]:
    p=SNAPSHOTS_DIR/f"{tab_name}.json"
    if not p.exists():return None
    with open(p)as f:d=json.load(f)
    return pd.DataFrame(d.get('rows',[]))

def save_snapshot(tab_name:str,df:pd.DataFrame):
    p=SNAPSHOTS_DIR/f"{tab_name}.json"
    with open(p,'w')as f:json.dump({"timestamp":datetime.now().isoformat(),"rows":df.fillna('').to_dict('records')},f)

def incremental_sync(tab_name:str,excel_path:Path,spreadsheet_id:str,key_col:str=KEY_COLUMN)->Dict:
    service=get_sheets_service()
    df_new=pd.read_excel(excel_path)
    if df_new.empty:return {"new":0,"updated":0,"skipped":0}
    if key_col not in df_new.columns:key_col=df_new.columns[0]
    df_new=df_new.drop_duplicates(subset=[key_col],keep='last');df_new[key_col]=df_new[key_col].astype(str).str.strip()
    df_existing=read_sheet(service,spreadsheet_id,tab_name)
    if df_existing.empty:df_existing=load_snapshot(tab_name)or pd.DataFrame()
    n,u,s=0,0,0
    if df_existing.empty:
        df_merged=df_new;n=len(df_merged)
    else:
        df_existing[key_col]=df_existing[key_col].astype(str).str.strip()
        df_merged=df_existing.copy()
        for _,r in df_new.iterrows():
            k=str(r[key_col]).strip();m=df_merged[key_col]==k;idx=df_merged[m].index
            if len(idx)==0:
                df_merged=pd.concat([df_merged,r.to_frame().T],ignore_index=True);n+=1
            else:
                changed=any(str(df_merged.loc[idx[0],c]).strip()!=str(r[c]).strip() for c in df_new.columns if c!="LastSyncedAt")
                if changed:
                    for c in df_new.columns:df_merged.loc[idx[0],c]=r[c];u+=1
                else:s+=1
    write_sheet(service,spreadsheet_id,tab_name,df_merged);save_snapshot(tab_name,df_merged)
    return {"new":n,"updated":u,"skipped":s}

async def export_dashboard_tab(page:Page,tab_name:str,download_path:Path,spreadsheet_id:str)->Optional[Dict]:
    try:
        logger.info(f"📊 Starting export for tab: {tab_name}")
        tab_selector=f'text="{tab_name}"'
        await page.wait_for_selector(tab_selector,timeout=40000)
        await page.locator(tab_selector).first.click()
        await page.wait_for_load_state('networkidle',timeout=30000)
        await asyncio.sleep(5)
        await page.wait_for_selector('table,[role="table"],.table',timeout=15000)
        await asyncio.sleep(3)
        file_path=download_path/f"{tab_name}.xlsx"
        event=asyncio.Event();downloaded=[]
        async def handle(d:Download):
            await d.save_as(file_path);downloaded.append(file_path);event.set()
        page.on("download",handle)
        await page.click('button:has-text("Export to excel"),button:has-text("Export to Excel")')
        await asyncio.wait_for(event.wait(),timeout=EXPORT_TIMEOUT_MS/1000)
        for _ in range(5):
            await asyncio.sleep(2)
            if file_path.exists() and file_path.stat().st_size>0:break
        if not file_path.exists() or file_path.stat().st_size==0:return None
        res=incremental_sync(tab_name,file_path,spreadsheet_id)
        file_path.unlink(missing_ok=True)
        logger.info(f"✅ Tab '{tab_name}' export done: {res}")
        return {"tab":tab_name,**res}
    except Exception as e:
        logger.error(f"❌ Error processing tab '{tab_name}': {e}")
        return None

async def export_dashboard(session_id:str,spreadsheet_id:str,storage_state:Optional[Dict]=None)->Dict:
    tab_results=[]
    session_path=None
    if not storage_state:
        p=Path(SESSION_STORAGE_PATH)/f"{session_id}.json"
        if p.exists():session_path=p
        else:raise FileNotFoundError("Session not found")
        with open(session_path)as f:storage_state=json.load(f)
    download_path=TMP_DIR/"dashboard_exports";download_path.mkdir(parents=True,exist_ok=True)
    async with async_playwright() as p:
        b=await p.chromium.launch(headless=True,args=['--no-sandbox','--disable-setuid-sandbox'])
        c=await b.new_context(storage_state=storage_state,accept_downloads=True)
        page=await c.new_page()
        await page.goto("https://compliancenominationportal.in.pwc.com/dashboard",wait_until='networkidle',timeout=60000)
        await asyncio.sleep(5)
        logger.info("🔍 Preparing dashboard: expanding Advance Search...")
        advance_selectors=[
            'button[data-bs-target="#collapse-advance-serach"]',
            'button.btn.btn-warning:has-text("Advance search")',
            'text="Advance search"',
            'button:has-text("Advance")'
        ]
        advance_clicked=False
        for attempt in range(3):
            for sel in advance_selectors:
                try:
                    await page.wait_for_selector(sel,timeout=25000)
                    loc=page.locator(sel).first()
                    if await loc.is_visible():
                        await loc.click();await asyncio.sleep(3)
                        logger.info(f"✅ Advance Search clicked using {sel}")
                        advance_clicked=True;break
                except:continue
            if advance_clicked:break
            await asyncio.sleep(2)
        if not advance_clicked:raise Exception("Advance Search not clickable")
        export_visible=False
        for sel in ['button:has-text("Export to excel")','button:has-text("Export to Excel")']:
            try:
                await page.wait_for_selector(sel,timeout=25000)
                if await page.locator(sel).first().is_visible():export_visible=True;break
            except:continue
        if not export_visible:raise Exception("Export to Excel button missing")
        logger.info("🎯 Dashboard ready — proceeding with tab exports...")
        for tab in DASHBOARD_TABS:
            res=await export_dashboard_tab(page,tab,download_path,spreadsheet_id)
            tab_results.append(res or {"tab":tab,"error":"failed"})
            await asyncio.sleep(25)
        try:
            await page.click('text="Welcome"')
            await asyncio.sleep(2)
            await page.click('text="Logout"')
            await page.wait_for_selector('text="You are logged-out sucessfully"',timeout=20000)
            logger.info("✅ Logout successful")
        except Exception as e:
            logger.warning(f"⚠ Logout failed: {e}")
        await b.close()
    success=[r for r in tab_results if r and not r.get('error')]
    fail=[r for r in tab_results if not r or r.get('error')]
    return {"ok":True,"successful":len(success),"failed":len(fail),"tab_results":tab_results}

class ExportRequest(BaseModel):
    session_id:Optional[str]="latest"
    spreadsheet_id:Optional[str]=None
    storage_state:Optional[Dict]=None

@app.get("/")
async def root():
    return {"ok":True,"message":"PwC Dashboard Export API"}

@app.get("/health")
async def health():
    return {"ok":True,"uptime":int((datetime.now()-start_time).total_seconds())}

@app.post("/export-dashboard")
async def export_dashboard_endpoint(request:ExportRequest):
    try:
        sid=request.session_id or "latest"
        sheet=request.spreadsheet_id or GOOGLE_SHEET_ID
        if not sheet:raise HTTPException(status_code=400,detail="spreadsheet_id required")
        res=await export_dashboard(sid,sheet,request.storage_state)
        return JSONResponse(content=res)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404,detail=str(e))
    except Exception as e:
        logger.error(f"Export endpoint error: {e}")
        raise HTTPException(status_code=500,detail=str(e))

@app.get("/test-sheets")
async def test_sheets():
    try:
        if not GOOGLE_SHEET_ID:raise HTTPException(status_code=400,detail="GOOGLE_SHEET_ID missing")
        s=get_sheets_service()
        s.spreadsheets().values().append(spreadsheetId=GOOGLE_SHEET_ID,range="TestConnection!A:B",valueInputOption="RAW",body={"values":[["Test Connected ✅",datetime.now().isoformat()]]}).execute()
        return JSONResponse(content={"ok":True,"message":"Write test successful"})
    except Exception as e:
        raise HTTPException(status_code=500,detail=str(e))

if __name__=="__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0",port=int(os.getenv("PORT",8000)))
