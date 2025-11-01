"""
FastAPI + Playwright + Google Sheets Dashboard Export with Incremental Sync
Auto-triggers after successful login completion
"""
import asyncio
import os
import json
import logging
from pathlib import Path
from typing import Dict, Optional, List
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from playwright.async_api import async_playwright, Browser, Page, Download
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="PwC Dashboard Export API")

# Track start time for uptime
start_time = datetime.now()

# Environment variables
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json")
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))
SNAPSHOTS_DIR = TMP_DIR / "snapshots"
SESSION_STORAGE_PATH = os.getenv("SESSION_STORAGE_PATH", "/tmp/pwc")
EXPORT_TIMEOUT_MS = 240000  # 4 minutes
KEY_COLUMN = "Candidate ID"

# Tabs to process
DASHBOARD_TABS = [
    "Today's allocated",
    "Not started",
    "Draft",
    "Rejected / Insufficient",
    "Submitted",
    "Work in progress",
    "BGV closed"
]

# Ensure directories exist
SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
(TMP_DIR / "dashboard_exports").mkdir(parents=True, exist_ok=True)


def get_sheets_service():
    """Initialize Google Sheets API service using service account credentials
    
    Priority:
    1. GOOGLE_CREDENTIALS_JSON (environment variable, JSON string)
    2. GOOGLE_CREDENTIALS_PATH or credentials.json (file path, backward compatibility)
    """
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = None
    
    if GOOGLE_CREDENTIALS_JSON:
        # Parse directly from environment variable (string → dict)
        creds_json = GOOGLE_CREDENTIALS_JSON
        
        try:
            # Handle escaped newlines and ensure valid JSON
            creds_info = json.loads(creds_json)
        except json.JSONDecodeError:
            # Sometimes the JSON comes escaped, try cleaning it up
            creds_info = json.loads(creds_json.replace('\\n', '\n'))
        
        creds = service_account.Credentials.from_service_account_info(
            creds_info,
            scopes=scopes
        )
        logger.info("Google Sheets service initialized from GOOGLE_CREDENTIALS_JSON")
    else:
        # Fallback to file path (optional)
        creds_path = Path(GOOGLE_CREDENTIALS_PATH)
        if not creds_path.exists():
            raise FileNotFoundError(f"Credentials file not found: {GOOGLE_CREDENTIALS_PATH}")
        
        creds = service_account.Credentials.from_service_account_file(
            creds_path,
            scopes=scopes
        )
        logger.info(f"Google Sheets service initialized from file: {GOOGLE_CREDENTIALS_PATH}")
    
    service = build("sheets", "v4", credentials=creds)
    return service


def read_sheet(service, spreadsheet_id: str, sheet_name: str) -> pd.DataFrame:
    """Read existing data from Google Sheet"""
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A:ZZ"
        ).execute()
        
        values = result.get('values', [])
        if not values:
            return pd.DataFrame()
        
        headers = values[0]
        if len(values) == 1:
            return pd.DataFrame(columns=headers)
        
        data = values[1:]
        max_cols = len(headers)
        padded_data = [row + [''] * (max_cols - len(row)) for row in data]
        
        df = pd.DataFrame(padded_data, columns=headers)
        logger.info(f"Read {len(df)} rows from sheet '{sheet_name}'")
        return df
    except Exception as e:
        logger.warning(f"Error reading sheet '{sheet_name}': {e}")
        return pd.DataFrame()


def write_sheet(service, spreadsheet_id: str, sheet_name: str, df: pd.DataFrame):
    """Write DataFrame to Google Sheet (overwrites entire sheet)"""
    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_names = [s['properties']['title'] for s in spreadsheet.get('sheets', [])]
        
        if sheet_name not in sheet_names:
            requests = [{
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            }]
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            ).execute()
        
        if df.empty:
            headers = [df.columns.tolist()] if len(df.columns) > 0 else [[]]
            values = headers
        else:
            if "LastSyncedAt" not in df.columns:
                df.insert(0, "LastSyncedAt", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            else:
                df["LastSyncedAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            headers = [df.columns.tolist()]
            values = headers + df.fillna('').values.tolist()
        
        range_name = f"{sheet_name}!A1"
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body={'values': values}
        ).execute()
        
        try:
            spreadsheet_after = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            sheet = [s for s in spreadsheet_after.get('sheets', []) if s['properties']['title'] == sheet_name]
            if sheet:
                max_rows = sheet[0]['properties']['gridProperties']['rowCount']
                if len(values) < max_rows and len(values) > 0:
                    clear_range = f"{sheet_name}!A{len(values) + 1}:ZZ{max_rows}"
                    service.spreadsheets().values().clear(
                        spreadsheetId=spreadsheet_id,
                        range=clear_range
                    ).execute()
        except Exception as e:
            logger.warning(f"Could not clear remaining rows: {e}")
        
        logger.info(f"Wrote {len(df)} rows to sheet '{sheet_name}'")
    except Exception as e:
        logger.error(f"Error writing to sheet '{sheet_name}': {e}")
        raise


def load_snapshot(tab_name: str) -> Optional[pd.DataFrame]:
    """Load previous snapshot from JSON file"""
    snapshot_path = SNAPSHOTS_DIR / f"{tab_name}.json"
    if not snapshot_path.exists():
        return None
    
    try:
        with open(snapshot_path, 'r') as f:
            data = json.load(f)
        
        if 'rows' in data and isinstance(data['rows'], list):
            if len(data['rows']) == 0:
                return pd.DataFrame()
            df = pd.DataFrame(data['rows'])
        else:
            df = pd.DataFrame(data)
        
        logger.info(f"Loaded snapshot for '{tab_name}': {len(df)} rows")
        return df
    except Exception as e:
        logger.warning(f"Error loading snapshot for '{tab_name}': {e}")
        return None


def save_snapshot(tab_name: str, df: pd.DataFrame):
    """Save current DataFrame as snapshot"""
    snapshot_path = SNAPSHOTS_DIR / f"{tab_name}.json"
    try:
        data = {
            "timestamp": datetime.now().isoformat(),
            "rows": df.fillna('').to_dict('records')
        }
        with open(snapshot_path, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f"Saved snapshot for '{tab_name}'")
    except Exception as e:
        logger.error(f"Error saving snapshot for '{tab_name}': {e}")


def incremental_sync(
    tab_name: str,
    excel_path: Path,
    spreadsheet_id: str,
    key_col: str = KEY_COLUMN
) -> Dict:
    """
    Perform incremental sync: merge new data with existing sheet data
    
    Returns dict with counts: {"new": X, "updated": Y, "skipped": Z}
    """
    try:
        service = get_sheets_service()
        
        try:
            df_new = pd.read_excel(excel_path)
        except Exception as e:
            logger.error(f"Error reading Excel file {excel_path}: {e}")
            return {"new": 0, "updated": 0, "skipped": 0, "error": str(e)}
        
        if df_new.empty:
            logger.warning(f"Excel file '{excel_path}' is empty")
            return {"new": 0, "updated": 0, "skipped": 0}
        
        if key_col not in df_new.columns:
            logger.warning(f"Key column '{key_col}' not found. Using first column: {df_new.columns[0]}")
            key_col = df_new.columns[0]
        
        df_new = df_new.drop_duplicates(subset=[key_col], keep='last')
        df_new[key_col] = df_new[key_col].astype(str).str.strip()
        
        df_existing = read_sheet(service, spreadsheet_id, tab_name)
        
        if df_existing.empty:
            df_existing = load_snapshot(tab_name)
            if df_existing is None:
                df_existing = pd.DataFrame()
        
        new_count = 0
        updated_count = 0
        skipped_count = 0
        
        if df_existing.empty:
            df_merged = df_new.copy()
            new_count = len(df_merged)
            logger.info(f"All {new_count} rows are new for '{tab_name}'")
        else:
            if key_col not in df_existing.columns:
                logger.warning(f"Key column '{key_col}' not in existing sheet. Treating all as new.")
                df_merged = pd.concat([df_existing, df_new], ignore_index=True)
                new_count = len(df_new)
            else:
                df_existing[key_col] = df_existing[key_col].astype(str).str.strip()
                df_merged = df_existing.copy()
                
                for _, new_row in df_new.iterrows():
                    key_value = str(new_row[key_col]).strip()
                    mask = df_merged[key_col] == key_value
                    existing_idx = df_merged[mask].index
                    
                    if len(existing_idx) == 0:
                        df_merged = pd.concat([df_merged, new_row.to_frame().T], ignore_index=True)
                        new_count += 1
                    else:
                        existing_row = df_merged.loc[existing_idx[0]]
                        changed = False
                        
                        for col in df_new.columns:
                            if col == "LastSyncedAt":
                                continue
                            
                            old_val = existing_row.get(col)
                            new_val = new_row.get(col)
                            
                            old_val_str = '' if pd.isna(old_val) else str(old_val).strip()
                            new_val_str = '' if pd.isna(new_val) else str(new_val).strip()
                            
                            if old_val_str != new_val_str:
                                changed = True
                                break
                        
                        if changed:
                            for col in df_new.columns:
                                df_merged.loc[existing_idx[0], col] = new_row[col]
                            updated_count += 1
                        else:
                            skipped_count += 1
                
                logger.info(
                    f"Sync '{tab_name}': {new_count} new, {updated_count} updated, {skipped_count} skipped"
                )
        
        write_sheet(service, spreadsheet_id, tab_name, df_merged)
        save_snapshot(tab_name, df_merged)
        
        return {
            "new": new_count,
            "updated": updated_count,
            "skipped": skipped_count
        }
        
    except Exception as e:
        logger.error(f"Incremental sync error for '{tab_name}': {e}")
        return {"new": 0, "updated": 0, "skipped": 0, "error": str(e)}


async def export_dashboard_tab(
    page: Page,
    tab_name: str,
    download_path: Path,
    spreadsheet_id: str
) -> Optional[Dict]:
    """
    Export a single dashboard tab with incremental sync
    Processes strictly one by one: click tab → wait for load → click export → wait for download → verify file
    """
    try:
        logger.info(f"📊 Starting export for tab: {tab_name}")
        
        # Step 1: Click the tab
        tab_selector = f'text="{tab_name}"'
        logger.info(f"  → Step 1: Clicking tab '{tab_name}'")
        await page.wait_for_selector(tab_selector, timeout=30000)
        await page.click(tab_selector)
        logger.info(f"  ✓ Tab '{tab_name}' clicked")
        
        # Step 2: Wait for tab to fully load
        logger.info(f"  → Step 2: Waiting for tab '{tab_name}' to fully load...")
        await page.wait_for_load_state('networkidle', timeout=30000)
        await asyncio.sleep(2)
        
        # Additional wait for tab content to be visible
        try:
            # Wait for common tab content indicators
            await page.wait_for_selector('table, [role="table"], .table', timeout=10000)
        except:
            pass
        
        await asyncio.sleep(1)
        logger.info(f"  ✓ Tab '{tab_name}' fully loaded")
        
        # Step 3: Wait for "Export to excel" button to be visible and ready
        logger.info(f"  → Step 3: Waiting for 'Export to excel' button to be visible...")
        export_selectors = [
            'button:has-text("Export to excel")',
            'button:has-text("Export to Excel")',
            'a:has-text("Export to excel")',
            'a:has-text("Export to Excel")',
            '[aria-label*="Export" i]',
            'button[title*="Export" i]'
        ]
        
        export_button_found = False
        export_button_selector = None
        
        for selector in export_selectors:
            try:
                await page.wait_for_selector(selector, timeout=10000)
                is_visible = await page.locator(selector).first().is_visible()
                if is_visible:
                    export_button_found = True
                    export_button_selector = selector
                    logger.info(f"  ✓ Export button found and visible: {selector}")
                    break
            except Exception:
                continue
        
        if not export_button_found:
            raise Exception(f"Could not find or see Export button for tab: {tab_name}")
        
        # Step 4: Setup download listener before clicking
        download_event = asyncio.Event()
        downloaded_file = []
        file_path = download_path / f"{tab_name}.xlsx"
        download_handler = None
        
        async def handle_download(download: Download):
            try:
                await download.save_as(file_path)
                downloaded_file.append(file_path)
                download_event.set()
                logger.info(f"  ✓ Download saved: {file_path}")
            except Exception as e:
                logger.error(f"  ✗ Error saving download: {e}")
                download_event.set()
        
        download_handler = handle_download
        page.on("download", download_handler)
        
        # Step 5: Click "Export to excel" button
        logger.info(f"  → Step 5: Clicking 'Export to excel' button...")
        try:
            await page.click(export_button_selector)
            logger.info(f"  ✓ Export button clicked")
        except Exception as e:
            if download_handler:
                page.remove_listener("download", download_handler)
            raise Exception(f"Failed to click export button: {e}")
        
        # Step 6: Wait for download to complete (with timeout)
        logger.info(f"  → Step 6: Waiting for download to complete (max {EXPORT_TIMEOUT_MS/1000}s)...")
        try:
            await asyncio.wait_for(download_event.wait(), timeout=EXPORT_TIMEOUT_MS / 1000)
        except asyncio.TimeoutError:
            logger.error(f"  ✗ Download timeout for tab: {tab_name} after {EXPORT_TIMEOUT_MS/1000}s")
            if download_handler:
                page.remove_listener("download", download_handler)
            return None
        
        # Step 7: Verify file is saved and not empty
        logger.info(f"  → Step 7: Verifying downloaded file exists...")
        if not downloaded_file:
            logger.error(f"  ✗ No file path recorded for tab: {tab_name}")
            if download_handler:
                page.remove_listener("download", download_handler)
            return None
        
        # Wait a bit more and verify file exists (with retries)
        max_file_check_retries = 5
        file_exists = False
        for retry in range(max_file_check_retries):
            await asyncio.sleep(1)
            if file_path.exists():
                file_exists = True
                break
            logger.info(f"  → Waiting for file to appear... (retry {retry + 1}/{max_file_check_retries})")
        
        if not file_exists:
            logger.error(f"  ✗ File does not exist after waiting: {file_path}")
            if download_handler:
                page.remove_listener("download", download_handler)
            return None
        
        file_size = file_path.stat().st_size
        if file_size == 0:
            logger.error(f"  ✗ Downloaded file is empty: {file_path}")
            if download_handler:
                page.remove_listener("download", download_handler)
            try:
                file_path.unlink()
            except:
                pass
            return None
        
        logger.info(f"  ✓ File verified: {file_path.name} ({file_size} bytes)")
        
        # Step 8: Perform incremental sync
        logger.info(f"  → Step 8: Syncing data to Google Sheets...")
        sync_result = incremental_sync(tab_name, file_path, spreadsheet_id, KEY_COLUMN)
        logger.info(f"  ✓ Sync completed: {sync_result.get('new', 0)} new, {sync_result.get('updated', 0)} updated, {sync_result.get('skipped', 0)} skipped")
        
        # Step 9: Cleanup downloaded file
        try:
            file_path.unlink()
            logger.info(f"  ✓ Cleaned up downloaded file")
        except Exception as e:
            logger.warning(f"  ⚠ Failed to cleanup file {file_path}: {e}")
        
        if download_handler:
            page.remove_listener("download", download_handler)
        
        logger.info(f"✅ Tab '{tab_name}' export completed successfully")
        
        return {
            "tab": tab_name,
            **sync_result
        }
        
    except Exception as e:
        logger.error(f"❌ Error processing tab '{tab_name}': {e}")
        return None


async def export_dashboard(session_id: str, spreadsheet_id: str) -> Dict:
    """
    Main orchestration function to export all dashboard tabs with incremental sync
    
    Args:
        session_id: Session ID from login (or "latest" to use most recent)
        spreadsheet_id: Google Spreadsheet ID
        
    Returns:
        Dict with export summary
    """
    tab_results = []
    
    try:
        if session_id == "latest":
            session_dir = Path(SESSION_STORAGE_PATH)
            session_files = sorted(
                session_dir.glob("*.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True
            )
            if not session_files:
                raise FileNotFoundError("No session files found")
            session_storage_file = session_files[0]
            logger.info(f"Using latest session: {session_storage_file.name}")
        else:
            session_storage_file = Path(SESSION_STORAGE_PATH) / f"{session_id}.json"
            if not session_storage_file.exists():
                raise FileNotFoundError(f"Session storage not found: {session_storage_file}")
        
        with open(session_storage_file, 'r') as f:
            storage_state = json.load(f)
        
        download_path = TMP_DIR / "dashboard_exports"
        download_path.mkdir(parents=True, exist_ok=True)
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--no-sandbox', '--disable-setuid-sandbox']
            )
            
            context = await browser.new_context(
                storage_state=storage_state,
                accept_downloads=True
            )
            
            page = await context.new_page()
            
            dashboard_url = "https://compliancenominationportal.in.pwc.com/dashboard"
            await page.goto(dashboard_url, wait_until='networkidle', timeout=60000)
            await asyncio.sleep(3)
            
            # Ensure "Advance search" is clicked and "Export to excel" button is visible
            export_button_visible = False
            max_retries = 3
            
            for attempt in range(max_retries):
                try:
                    # Click "Advance search"
                    advance_search_selectors = [
                        'text="Advance search"',
                        'button:has-text("Advance search")',
                        'a:has-text("Advance search")',
                        '[aria-label*="Advance search" i]'
                    ]
                    
                    clicked = False
                    for selector in advance_search_selectors:
                        try:
                            await page.wait_for_selector(selector, timeout=10000)
                            await page.click(selector)
                            clicked = True
                            logger.info(f"Clicked Advance search button (attempt {attempt + 1}/{max_retries})")
                            break
                        except Exception:
                            continue
                    
                    if not clicked:
                        logger.warning(f"Advance search button not found (attempt {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2)
                            continue
                    
                    # Wait for page to load
                    await page.wait_for_load_state('networkidle', timeout=30000)
                    await asyncio.sleep(2)
                    
                    # Check if "Export to excel" button is visible
                    export_button_selectors = [
                        'button:has-text("Export to excel")',
                        'button:has-text("Export to Excel")',
                        'a:has-text("Export to excel")',
                        'a:has-text("Export to Excel")',
                        '[aria-label*="Export" i]',
                        'button[title*="Export" i]'
                    ]
                    
                    export_visible = False
                    for selector in export_button_selectors:
                        try:
                            await page.wait_for_selector(selector, timeout=5000)
                            is_visible = await page.locator(selector).first().is_visible()
                            if is_visible:
                                export_visible = True
                                logger.info(f"Export button is visible: {selector}")
                                break
                        except Exception:
                            continue
                    
                    if export_visible:
                        export_button_visible = True
                        logger.info("✅ Advance search expanded successfully, export button is visible")
                        break
                    else:
                        logger.warning(f"Export button not visible after Advance search (attempt {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            logger.info("Retrying Advance search click...")
                            await asyncio.sleep(2)
                            continue
                        else:
                            logger.error("Export button not visible after all retries, continuing anyway...")
                            break
                            
                except Exception as e:
                    logger.warning(f"Error in Advance search setup (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2)
                        continue
            
            if not export_button_visible:
                logger.warning("⚠️ Export button not confirmed visible, but proceeding with tab processing...")
            
            # Small delay before starting tab processing
            await asyncio.sleep(1)
            
            for tab_name in DASHBOARD_TABS:
                try:
                    result = await export_dashboard_tab(
                        page=page,
                        tab_name=tab_name,
                        download_path=download_path,
                        spreadsheet_id=spreadsheet_id
                    )
                    
                    if result:
                        tab_results.append(result)
                        logger.info(f"✅ Tab '{tab_name}': {result.get('new', 0)} new, {result.get('updated', 0)} updated, {result.get('skipped', 0)} skipped")
                    else:
                        tab_results.append({
                            "tab": tab_name,
                            "new": 0,
                            "updated": 0,
                            "skipped": 0,
                            "error": "Timeout or download failed"
                        })
                        logger.warning(f"⚠️ Tab '{tab_name}': Failed")
                    
                    await asyncio.sleep(1)
                    
                except Exception as e:
                    logger.error(f"Failed to process tab '{tab_name}': {e}")
                    tab_results.append({
                        "tab": tab_name,
                        "new": 0,
                        "updated": 0,
                        "skipped": 0,
                        "error": str(e)
                    })
            
            await browser.close()
        
        logger.info(f"Export completed: {len([r for r in tab_results if not r.get('error')])}/{len(DASHBOARD_TABS)} tabs successful")
        
        return {
            "ok": True,
            "tab_results": tab_results,
            "total_tabs": len(DASHBOARD_TABS),
            "successful": len([r for r in tab_results if not r.get("error")]),
            "failed": len([r for r in tab_results if r.get("error")])
        }
        
    except Exception as e:
        logger.error(f"Export dashboard error: {e}")
        raise


class ExportRequest(BaseModel):
    session_id: Optional[str] = "latest"
    spreadsheet_id: Optional[str] = None


@app.get("/")
async def root():
    return {
        "ok": True,
        "message": "PwC Dashboard Export API",
        "endpoints": {
            "POST /export-dashboard": "Export dashboard tabs to Google Sheets with incremental sync",
            "GET /test-sheets": "Test Google Sheets connectivity and write a test row",
            "GET /health": "Health check with uptime"
        }
    }


@app.get("/health")
async def health():
    """Health check endpoint with uptime"""
    uptime_seconds = int((datetime.now() - start_time).total_seconds())
    return {
        "ok": True,
        "uptime": uptime_seconds
    }


@app.post("/export-dashboard")
async def export_dashboard_endpoint(request: ExportRequest):
    """Export PwC dashboard tabs to Google Sheets with incremental sync"""
    try:
        session_id = request.session_id or "latest"
        sheet_id = request.spreadsheet_id or GOOGLE_SHEET_ID
        
        if not sheet_id:
            raise HTTPException(
                status_code=400,
                detail="spreadsheet_id required (set GOOGLE_SHEET_ID env or provide in request)"
            )
        
        result = await export_dashboard(session_id, sheet_id)
        return JSONResponse(content=result)
        
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Export endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/test-sheets")
async def test_sheets():
    """Test Google Sheets connectivity by writing a test row"""
    try:
        if not GOOGLE_SHEET_ID:
            raise HTTPException(
                status_code=400,
                detail="GOOGLE_SHEET_ID environment variable is required"
            )
        
        service = get_sheets_service()
        
        from datetime import datetime, timezone
        test_row = [
            "Test Connected ✅",
            datetime.now(timezone.utc).isoformat()
        ]
        
        sheet_name = "TestConnection"
        range_name = f"{sheet_name}!A:B"
        
        service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=range_name,
            valueInputOption="RAW",
            body={"values": [test_row]}
        ).execute()
        
        logger.info(f"Test row written to {GOOGLE_SHEET_ID} / {sheet_name}")
        
        return JSONResponse(content={
            "ok": True,
            "message": "Write test successful",
            "spreadsheet_id": GOOGLE_SHEET_ID,
            "sheet": sheet_name
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Test sheets error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Test failed: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
