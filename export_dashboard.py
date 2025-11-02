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
from pydantic import BaseModel, field_validator
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
    if not GOOGLE_CREDENTIALS_JSON:
        raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable is required")
    
    try:
        creds_json = GOOGLE_CREDENTIALS_JSON
        
        # Clean control characters from JSON string (except newlines/tabs in string values)
        # This handles cases where the environment variable has invalid control characters
        def clean_json_string(s):
            # Remove control characters except newline, carriage return, tab
            # But preserve them if they're inside JSON string values
            result = []
            in_string = False
            escape_next = False
            for i, char in enumerate(s):
                if escape_next:
                    result.append(char)
                    escape_next = False
                    continue
                if char == '\\':
                    escape_next = True
                    result.append(char)
                    continue
                if char == '"' and (i == 0 or s[i-1] != '\\'):
                    in_string = not in_string
                    result.append(char)
                    continue
                # Keep all characters in JSON structure (outside strings) and printable chars
                # Also keep newline/cr/tab inside strings (for private_key)
                if ord(char) >= 32 or char in '\n\r\t' or (in_string and char in '\n\r\t'):
                    result.append(char)
                elif ord(char) < 32:
                    # Replace control character with space (safe for JSON parsing)
                    result.append(' ')
            return ''.join(result)
        
        # Clean the JSON string
        creds_json_cleaned = clean_json_string(creds_json)
        
        # Handle multiple levels of escaping
        creds_info = None
        parse_errors = []
        
        # Try 1: Direct JSON parse (cleaned)
        try:
            creds_info = json.loads(creds_json_cleaned)
        except json.JSONDecodeError as e1:
            parse_errors.append(f"Direct parse: {e1}")
            # Try 2: Replace escaped newlines
            try:
                creds_json_fixed = creds_json_cleaned.replace("\\n", "\n").replace("\\r", "\r")
                creds_info = json.loads(creds_json_fixed)
            except json.JSONDecodeError as e2:
                parse_errors.append(f"Newline replace: {e2}")
                # Try 3: Use codecs unicode_escape
                try:
                    import codecs
                    # Only decode if there are actual escape sequences
                    if '\\n' in creds_json_cleaned or '\\r' in creds_json_cleaned:
                        creds_json_fixed = codecs.decode(creds_json_cleaned, 'unicode_escape')
                        creds_info = json.loads(creds_json_fixed)
                    else:
                        raise json.JSONDecodeError("No escape sequences found", creds_json_cleaned, 0)
                except (json.JSONDecodeError, UnicodeDecodeError) as e3:
                    parse_errors.append(f"Unicode escape: {e3}")
                    raise ValueError(f"Failed to parse GOOGLE_CREDENTIALS_JSON. Errors: {parse_errors}")
        
        # Validate the structure
        if not isinstance(creds_info, dict):
            raise ValueError("GOOGLE_CREDENTIALS_JSON must be a valid JSON object")
        
        # Ensure private_key has actual newlines (not escaped strings)
        if 'private_key' in creds_info and isinstance(creds_info['private_key'], str):
            # Replace literal \n with actual newlines if needed
            if '\\n' in creds_info['private_key']:
                creds_info['private_key'] = creds_info['private_key'].replace('\\n', '\n')
        
        creds = service_account.Credentials.from_service_account_info(
            creds_info, 
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return build("sheets", "v4", credentials=creds)
    except Exception as e:
        logger.error(f"Failed to parse GOOGLE_CREDENTIALS_JSON: {e}")
        logger.error(f"JSON length: {len(GOOGLE_CREDENTIALS_JSON) if GOOGLE_CREDENTIALS_JSON else 0}")
        logger.error(f"First 200 chars: {GOOGLE_CREDENTIALS_JSON[:200] if GOOGLE_CREDENTIALS_JSON else 'N/A'}")
        raise ValueError(f"Invalid GOOGLE_CREDENTIALS_JSON: {e}")


async def incremental_sync(tab_name: str, excel_path: Path, spreadsheet_id: str, key_col: str = "Candidate ID"):
    """
    Incremental sync: Read Excel, compare with existing Google Sheet data, upload new/changed rows.
    
    Returns: {"tab": tab_name, "new": count, "updated": count, "skipped": count, "rows": total_rows}
    """
    try:
        logger.info(f"📊 Starting incremental sync for {tab_name}...")
        
        # Step 1: Read Excel file
        if not excel_path.exists():
            raise FileNotFoundError(f"Excel file not found: {excel_path}")
        
        logger.info(f"📖 Reading Excel file: {excel_path}")
        df_new = pd.read_excel(excel_path, engine='openpyxl')
        
        if df_new.empty:
            logger.warning(f"⚠️ Excel file for {tab_name} is empty")
            return {"tab": tab_name, "new": 0, "updated": 0, "skipped": 0, "rows": 0, "error": "Excel file is empty"}
        
        # Clean control characters from Excel data immediately after reading
        def clean_excel_value(val):
            if pd.isna(val):
                return val
            val_str = str(val)
            # Remove invalid JSON control characters (characters below 32 except newline, carriage return, tab)
            cleaned = ''.join(char if ord(char) >= 32 or char in '\n\r\t' else ' ' for char in val_str)
            return cleaned
        
        # Clean all object (string) columns
        for col in df_new.select_dtypes(include=['object']).columns:
            df_new[col] = df_new[col].apply(clean_excel_value)
        
        # Clean column names
        df_new.columns = [clean_excel_value(str(col)) for col in df_new.columns]
        
        # Add timestamp column
        if "LastSyncedAt" not in df_new.columns:
            df_new["LastSyncedAt"] = datetime.now().isoformat()
        else:
            df_new["LastSyncedAt"] = datetime.now().isoformat()
        
        logger.info(f"✅ Loaded {len(df_new)} rows from Excel (cleaned control characters)")
        
        # Step 2: Read existing data from Google Sheet (if sheet exists)
        try:
            service = get_sheets_service()
        except Exception as creds_err:
            logger.error(f"❌ Failed to initialize Google Sheets service: {creds_err}")
            logger.error(f"❌ Error type: {type(creds_err).__name__}")
            import traceback
            logger.error(f"❌ Traceback: {traceback.format_exc()}")
            raise Exception(f"Google Sheets credentials error: {creds_err}")
        
        sheet_name = tab_name  # Use tab name as sheet name
        
        try:
            # Check if sheet exists
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            sheet_exists = any(s.get('properties', {}).get('title') == sheet_name for s in spreadsheet.get('sheets', []))
            
            if sheet_exists and key_col in df_new.columns:
                # Read existing data
                logger.info(f"📥 Reading existing data from sheet '{sheet_name}'...")
                try:
                    result = service.spreadsheets().values().get(
                        spreadsheetId=spreadsheet_id,
                        range=f"{sheet_name}!A:ZZ"
                    ).execute()
                    values = result.get('values', [])
                    
                    if values and len(values) > 1:
                        # Convert to DataFrame
                        # Clean control characters from headers and rows
                        def clean_cell(val):
                            if val is None:
                                return ''
                            val_str = str(val)
                            # Remove invalid JSON control characters (keep only printable chars and newlines/tabs)
                            return ''.join(char if ord(char) >= 32 or char in '\n\r\t' else ' ' for char in val_str)
                        
                        headers = [clean_cell(h) for h in values[0]]
                        rows = [[clean_cell(cell) for cell in row] for row in values[1:]]
                        df_existing = pd.DataFrame(rows, columns=headers)
                        
                        logger.info(f"📊 Found {len(df_existing)} existing rows in sheet")
                        
                        # Merge: identify new, updated, and unchanged rows
                        if key_col in df_existing.columns and key_col in df_new.columns:
                            # Merge on key column
                            existing_keys = set(df_existing[key_col].astype(str))
                            new_keys = set(df_new[key_col].astype(str))
                            updated_keys = existing_keys & new_keys
                            
                            # For simplicity, mark all existing keys as updated if present in new data
                            updated_count = len(updated_keys)
                            new_count = len(new_keys - existing_keys)
                            skipped_count = len(existing_keys - new_keys)  # Rows removed from Excel (keep in sheet)
                            
                            # Final DataFrame: new + all updated existing rows + unchanged existing rows
                            df_final = pd.concat([
                                df_new[df_new[key_col].astype(str).isin(new_keys | updated_keys)],  # New + Updated
                                df_existing[df_existing[key_col].astype(str).isin(existing_keys - new_keys)]  # Unchanged (keep)
                            ], ignore_index=True)
                            
                            logger.info(f"📈 Sync stats: {new_count} new, {updated_count} updated, {skipped_count} unchanged")
                        else:
                            # No key column match, just append new data
                            df_final = pd.concat([df_existing, df_new], ignore_index=True)
                            new_count = len(df_new)
                            updated_count = 0
                            skipped_count = len(df_existing)
                            logger.info(f"📈 No key column match - appending {new_count} new rows")
                    else:
                        # Empty sheet, use new data
                        df_final = df_new
                        new_count = len(df_new)
                        updated_count = 0
                        skipped_count = 0
                        logger.info(f"📊 Sheet is empty - uploading all {new_count} rows as new")
                except Exception as read_err:
                    logger.warning(f"⚠️ Could not read existing sheet (treating as empty): {read_err}")
                    df_final = df_new
                    new_count = len(df_new)
                    updated_count = 0
                    skipped_count = 0
            else:
                # Sheet doesn't exist or no key column, upload all new data
                df_final = df_new
                new_count = len(df_new)
                updated_count = 0
                skipped_count = 0
                logger.info(f"📊 Sheet '{sheet_name}' doesn't exist - uploading all {new_count} rows as new")
        except Exception as read_err:
            logger.warning(f"⚠️ Error reading existing sheet (uploading all as new): {read_err}")
            df_final = df_new
            new_count = len(df_new)
            updated_count = 0
            skipped_count = 0
        
        # Step 3: Save snapshot
        snapshot_path = SNAPSHOTS_DIR / f"{tab_name}.json"
        try:
            # Clean DataFrame before saving to JSON (remove/replace control characters)
            df_clean = df_final.copy()
            # Replace control characters (newlines, tabs, etc.) in string columns
            for col in df_clean.select_dtypes(include=['object']).columns:
                df_clean[col] = df_clean[col].astype(str).replace([r'\n', r'\r', r'\t'], [' ', ' ', ' '], regex=True)
                # Remove any remaining control characters
                df_clean[col] = df_clean[col].apply(lambda x: ''.join(char for char in str(x) if ord(char) >= 32 or char in '\n\r\t') if pd.notna(x) else x)
            
            df_clean.to_json(snapshot_path, orient='records', date_format='iso', force_ascii=True)
            logger.info(f"💾 Saved snapshot to {snapshot_path}")
        except Exception as snapshot_err:
            logger.warning(f"⚠️ Could not save snapshot: {snapshot_err}")
            # Don't fail the whole sync if snapshot fails
        
        # Step 4: Upload to Google Sheets
        logger.info(f"📤 Uploading {len(df_final)} rows to sheet '{sheet_name}'...")
        
        # Ensure sheet exists
        try:
            spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            sheet_exists = any(s.get('properties', {}).get('title') == sheet_name for s in spreadsheet.get('sheets', []))
            
            if not sheet_exists:
                logger.info(f"📋 Creating new sheet '{sheet_name}'...")
                request_body = {
                    'requests': [{
                        'addSheet': {
                            'properties': {
                                'title': sheet_name
                            }
                        }
                    }]
                }
                service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body=request_body
                ).execute()
                logger.info(f"✅ Created sheet '{sheet_name}'")
        except Exception as create_err:
            logger.warning(f"⚠️ Could not create sheet (may already exist): {create_err}")
        
        # Convert DataFrame to list of lists for Google Sheets API
        # Clean control characters from values to prevent JSON errors
        def clean_value(val):
            if pd.isna(val):
                return ''
            val_str = str(val)
            # Replace control characters (keep newlines and tabs for readability in sheets)
            # But ensure no invalid JSON control characters
            val_str = ''.join(char if ord(char) >= 32 or char in '\n\r\t' else ' ' for char in val_str)
            return val_str
        
        # Clean column names
        clean_columns = [clean_value(col) for col in df_final.columns.tolist()]
        
        # Clean all values
        clean_values = []
        for row in df_final.fillna('').values:
            clean_values.append([clean_value(val) for val in row])
        
        values = [clean_columns] + clean_values
        
        # Clear existing data and write new data
        range_name = f"{sheet_name}!A1"
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A:ZZ"
        ).execute()
        
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": values}
        ).execute()
        
        logger.info(f"✅ Successfully uploaded {len(df_final)} rows to sheet '{sheet_name}'")
        
        return {
            "tab": tab_name,
            "new": new_count,
            "updated": updated_count,
            "skipped": skipped_count,
            "rows": len(df_final),
            "sheet": sheet_name
        }
        
    except Exception as e:
        logger.error(f"❌ Incremental sync error for {tab_name}: {e}")
        raise


async def sync_all_tabs_to_sheets(download_dir: Path, spreadsheet_id: str):
    """Upload all exported Excel files to Google Sheets"""
    tab_results = []
    
    for tab in TABS:
        try:
            excel_path = download_dir / f"{tab}.xlsx"
            
            if not excel_path.exists():
                logger.warning(f"⚠️ Excel file not found for {tab}: {excel_path}")
                tab_results.append({"tab": tab, "status": "error", "error": "Excel file not found"})
                continue
            
            result = await incremental_sync(tab, excel_path, spreadsheet_id)
            tab_results.append(result)
            logger.info(f"✅ Completed sync for {tab}")
            
        except Exception as e:
            logger.error(f"❌ Error syncing {tab} to Sheets: {e}")
            tab_results.append({"tab": tab, "status": "error", "error": str(e)})
    
    return tab_results


async def click_force(page: Page, selector: str, timeout=5000, name="element"):
    """Universal click function with proper locator usage"""
    for attempt in range(3):
        try:
            await page.wait_for_selector(selector, timeout=timeout)
            # CRITICAL: Use locator.click() not locator()()
            locator = page.locator(selector).first
            await locator.scroll_into_view_if_needed()
            await asyncio.sleep(1)
            await locator.click(force=True)
            logger.info(f"✅ Clicked {name} via {selector}")
            return True
        except Exception as e:
            logger.warning(f"Retrying click for {name} ({attempt+1}/3): {e}")
            await asyncio.sleep(3)
    screenshot_path = f"/tmp/{name.replace(' ', '_')}_fail_{datetime.now().strftime('%H%M%S')}.png"
    await page.screenshot(path=screenshot_path, full_page=True)
    raise Exception(f"{name} not clickable after retries (screenshot: {screenshot_path})")


async def wait_full_load(page: Page, seconds=30, name="page"):
    logger.info(f"⏳ Waiting {seconds}s for {name} to load fully...")
    await asyncio.sleep(seconds)
    await page.wait_for_load_state("networkidle")
    await asyncio.sleep(2)


async def try_click_selector(page: Page, selectors, timeout_per=4000):
    """Try clicking using multiple selectors (same pattern as login process)"""
    for sel in selectors:
        try:
            await page.wait_for_selector(sel, timeout=timeout_per)
            # Use locator.click() not page.click()
            await page.locator(sel).first.click(force=True)
            return True
        except Exception:
            continue
    return False


async def click_advance_search(page: Page):
    """Click Advance search button using the same robust logic as login process"""
    logger.info("🔍 Waiting for dashboard to fully load...")
    await wait_full_load(page, 30, "dashboard")
    
    # Check current URL
    current_url = page.url
    logger.info(f"📍 Current URL: {current_url}")
    
    # Wait for dynamic content
    await asyncio.sleep(3)
    
    # Define all possible selectors (same pattern as login)
    selectors = [
        'button[data-bs-target="#collapse-advance-serach"]',
        'button[data-bs-target="#collapse-advance-search"]',
        'button:has-text("Advance search")',
        'button:has-text("Advance Search")',
        'a:has-text("Advance search")',
        'a:has-text("Advance Search")',
        'text="Advance search"',
        'text="Advance Search"',
        '[aria-label*="Advance search" i]',
        '[aria-label*="Advance Search" i]',
        'button[title*="Advance" i]',
        'a[title*="Advance" i]',
    ]
    
    # Try standard click approach first (same as tryClick in login)
    clicked = await try_click_selector(page, selectors, timeout_per=4000)
    if clicked:
        await asyncio.sleep(2)
        logger.info("✅ Advance Search clicked successfully")
        return
    
    # If standard click failed, try frame-based approach (same as MFA button logic)
    logger.info("Trying frame-based detection...")
    all_frames = [page] + page.frames
    
    clicked = False
    for frame in all_frames:
        btn_selectors = [
            'button:has-text("Advance search")',
            'button:has-text("Advance Search")',
            'a:has-text("Advance search")',
            'a:has-text("Advance Search")',
            'text="Advance search"',
            'text="Advance Search"',
        ]
        
        for sel in btn_selectors:
            try:
                btn = frame.locator(sel).first
                count = await btn.count()
                if count > 0:
                    await btn.wait_for(state='attached', timeout=5000)
                    
                    # Wait for button to be enabled (same as Send my code logic)
                    for i in range(10):
                        disabled = await btn.get_attribute('disabled')
                        if disabled is None or disabled == 'false' or disabled == '':
                            await btn.click(force=True)
                            await asyncio.sleep(2)
                            logger.info("✅ Advance Search clicked via frame detection")
                            clicked = True
                            break
                        await asyncio.sleep(1)  # Wait 1 second before retry
                    
                    if clicked:
                        break
            except Exception as e:
                logger.debug(f"Frame selector {sel} failed: {e}")
                continue
        
        if clicked:
            break
    
    if clicked:
        return
    
    # Last resort: JavaScript click (same pattern as login)
    logger.info("Attempting JavaScript click...")
    try:
        js_clicked = await page.evaluate("""
            () => {
                const elements = Array.from(document.querySelectorAll('button, a, [role="button"]'));
                for (let el of elements) {
                    const text = (el.textContent || '').toLowerCase();
                    if (text.includes('advance') && text.includes('search')) {
                        el.click();
                        return true;
                    }
                }
                return false;
            }
        """)
        if js_clicked:
            await asyncio.sleep(3)
            logger.info("✅ Advance Search clicked via JavaScript")
            return
    except Exception as e:
        logger.warning(f"JavaScript click failed: {e}")
    
    # Final screenshot before error
    error_screenshot = f"/tmp/advance_search_error_{datetime.now().strftime('%H%M%S')}.png"
    await page.screenshot(path=error_screenshot, full_page=True)
    logger.error(f"❌ Advance Search not found. Error screenshot: {error_screenshot}")
    raise Exception(f"Advance Search not clickable after all attempts. URL: {current_url}")


async def export_tab(page: Page, tab_name: str, download_dir: Path):
    logger.info(f"📊 Exporting tab: {tab_name}")
    
    # Try multiple selectors for tab clicking
    tab_selectors = [
        f'text="{tab_name}"',
        f'a:has-text("{tab_name}")',
        f'button:has-text("{tab_name}")',
        f'li:has-text("{tab_name}")',
        f'[data-tab*="{tab_name}"]',
        f'[aria-label*="{tab_name}"]',
    ]
    
    tab_clicked = False
    for tab_sel in tab_selectors:
        try:
            await click_force(page, tab_sel, timeout=5000, name=f"tab_{tab_name}")
            tab_clicked = True
            break
        except Exception as e:
            logger.debug(f"Tab selector {tab_sel} failed: {e}")
            continue
    
    if not tab_clicked:
        raise Exception(f"Could not click tab: {tab_name}")
    
    await wait_full_load(page, 25, f"tab {tab_name}")

    # Step 3: Click "Export to excel" button
    # CRITICAL: Export button appears AFTER Advance search is clicked and tab is selected
    # The button has ID "downloadExcel" and class "btn btn-danger clsdisableAction"
    # HTML: <input type="button" value="Export to excel" id="downloadExcel" class="btn btn-danger clsdisableAction">
    logger.info(f"📥 Step 3: Looking for Export to Excel button for {tab_name}...")
    
    export_selectors = [
        '#downloadExcel',  # Primary selector - exact ID from HTML
        'input[id="downloadExcel"]',  # Input button with this ID
        'input[value="Export to excel"][id="downloadExcel"]',  # More specific
        'button:has-text("Export to excel")',
        'button:has-text("Export to Excel")',
        'input[value="Export to excel"]',
        'input[value="Export to Excel"]',
        'a:has-text("Export to excel")',
        'a:has-text("Export to Excel")',
        '[aria-label*="Export" i]',
        '[title*="Export" i]',
        'button[title*="Export" i]',
        'a[title*="Export" i]',
        'button[aria-label*="Export" i]',
        'a[aria-label*="Export" i]',
    ]
    
    export_clicked = False
    for attempt in range(3):
        for sel in export_selectors:
            try:
                # Wait up to 30 seconds for button to appear
                await page.wait_for_selector(sel, timeout=30000)
                locator = page.locator(sel).first
                await locator.scroll_into_view_if_needed()
                await asyncio.sleep(1)
                await locator.click(force=True)
                logger.info(f"✅ Export button clicked via selector: {sel}")
                export_clicked = True
                break
            except Exception as e:
                logger.debug(f"Export selector {sel} failed (attempt {attempt+1}/3): {e}")
                await asyncio.sleep(2)
                continue
        if export_clicked:
            break
    
    # If all selectors failed, try JavaScript fallback
    if not export_clicked:
        logger.warning("Standard click methods failed, trying JavaScript fallback...")
        try:
            js_clicked = await page.evaluate("""
                () => {
                    // First, try the specific ID from HTML: id="downloadExcel"
                    const downloadExcelBtn = document.getElementById('downloadExcel');
                    if (downloadExcelBtn) {
                        downloadExcelBtn.click();
                        return true;
                    }
                    
                    // Fallback: search by text/attributes
                    const elements = Array.from(document.querySelectorAll('button, input[type="button"], a, [role="button"]'));
                    for (let el of elements) {
                        const text = (el.textContent || el.value || '').toLowerCase();
                        const ariaLabel = (el.getAttribute('aria-label') || '').toLowerCase();
                        const title = (el.getAttribute('title') || '').toLowerCase();
                        if ((text.includes('export') && text.includes('excel')) || 
                            ariaLabel.includes('export') || 
                            title.includes('export')) {
                            el.click();
                            return true;
                        }
                    }
                    return false;
                }
            """)
            if js_clicked:
                await asyncio.sleep(2)
                logger.info("✅ Export button clicked via JavaScript")
                export_clicked = True
        except Exception as js_err:
            logger.warning(f"JavaScript click failed: {js_err}")
    
    if not export_clicked:
        screenshot_path = f"/tmp/Export_button_fail_{tab_name.replace(' ', '_')}_{datetime.now().strftime('%H%M%S')}.png"
        await page.screenshot(path=screenshot_path, full_page=True)
        raise Exception(f"Export to Excel button not visible/clickable for {tab_name} after all retries (screenshot: {screenshot_path})")

    download_event = asyncio.Event()
    file_path = download_dir / f"{tab_name}.xlsx"
    downloaded_file = []

    async def handle_download(d: Download):
        await d.save_as(file_path)
        downloaded_file.append(file_path)
        download_event.set()
        logger.info(f"💾 Download saved: {file_path}")

    page.on("download", handle_download)
    try:
        await asyncio.wait_for(download_event.wait(), timeout=EXPORT_TIMEOUT)
    except asyncio.TimeoutError:
        raise Exception(f"Download timeout for {tab_name}")

    await asyncio.sleep(2)
    if not file_path.exists() or file_path.stat().st_size == 0:
        raise Exception(f"File missing or empty for {tab_name}")

    await asyncio.sleep(1)
    file_size = file_path.stat().st_size
    logger.info(f"✅ Step 4: Export completed for {tab_name} ({file_size} bytes)")
    logger.info(f"✅ ✅ Tab '{tab_name}' export workflow finished successfully")
    return {"tab": tab_name, "status": "done", "file_size": file_size}


async def perform_logout(page: Page):
    """Universal logout logic with correct dropdown selectors"""
    try:
        logger.info("🔒 Attempting logout via top-right dropdown...")
        await asyncio.sleep(5)  # allow dashboard JS to finish rendering
        
        # Correct selectors for Kendo UI menu dropdown
        selectors_dropdown = [
            ".k-menu-expand-arrow",
            "span.k-menu-expand-arrow",
            "span.k-menu-expand-arrow-icon",
            "text='Welcome'",
            "text='Sukrutha CR'"
        ]
        
        clicked = False
        for attempt in range(3):
            for sel in selectors_dropdown:
                try:
                    await page.wait_for_selector(sel, timeout=8000)
                    await page.locator(sel).first.click(force=True)
                    logger.info(f"✅ Profile dropdown opened via selector: {sel}")
                    clicked = True
                    break
                except Exception as e:
                    logger.warning(f"Retrying click for Profile_dropdown ({attempt+1}/3): {e}")
                    await asyncio.sleep(3)
            if clicked:
                break
        
        if not clicked:
            screenshot = f"/tmp/Profile_dropdown_fail_{datetime.now().strftime('%H%M%S')}.png"
            await page.screenshot(path=screenshot, full_page=True)
            logger.error(f"Logout failed: Profile_dropdown not clickable after retries (screenshot: {screenshot})")
            raise Exception(f"Profile_dropdown not clickable after retries (screenshot: {screenshot})")
        
        await asyncio.sleep(10)
        
        # Click logout option
        selectors_logout = [
            "text='Logout'",
            "text='Sign out'",
            "a[href*='Signout']",
            "a[href*='LogOff']",
            'a:has-text("Logout")',
            'a:has-text("Log out")',
        ]
        
        logout_clicked = False
        for sel in selectors_logout:
            try:
                await page.wait_for_selector(sel, timeout=8000)
                await page.locator(sel).first.click(force=True)
                logger.info(f"✅ Logout clicked via selector: {sel}")
                logout_clicked = True
                break
            except Exception as e:
                logger.debug(f"Logout selector {sel} failed: {e}")
                continue
        
        if not logout_clicked:
            logger.warning("Could not click logout link, trying direct URL fallback...")
            try:
                await page.goto("https://compliancenominationportal.in.pwc.com/Account/LogOff", wait_until="networkidle", timeout=30000)
                await asyncio.sleep(3)
                logger.info("✅ Logout via direct URL")
                return
            except Exception as logoff_err:
                logger.warning(f"Direct logout URL failed: {logoff_err}")
        
        await asyncio.sleep(5)
        try:
            await page.wait_for_selector("text='You are logged-out successfully'", timeout=10000)
            logger.info("✅ Logout confirmed successfully")
        except Exception:
            logger.warning("⚠️ Logout confirmation text not detected, but assuming success")
    except Exception as e:
        logger.error(f"Logout failed: {e}")
        # Don't raise - logout is not critical for the export process
        logger.warning("Continuing despite logout failure")


async def export_dashboard(session_id: str, spreadsheet_id: str, storage_state: Optional[Dict] = None):
    try:
        if not storage_state:
            session_file = SESSION_PATH / f"{session_id}.json"
            try:
                with open(session_file, "r") as f:
                    storage_state = json.load(f)
                    logger.info(f"Loaded session from local file: {session_file}")
            except FileNotFoundError:
                logger.warning(f"Session file missing locally for {session_id}")
                raise FileNotFoundError(f"Session file not found: {session_file}. Please provide storage_state in request or ensure session file exists.")
        
        # CRITICAL: Ensure storage_state is a dict, not a string
        # If it came as a JSON string, parse it
        if isinstance(storage_state, str):
            logger.warning("storage_state received as string, parsing JSON...")
            try:
                storage_state = json.loads(storage_state)
                logger.info("✅ Successfully parsed storage_state from string")
            except json.JSONDecodeError as parse_err:
                raise ValueError(f"storage_state is a string but not valid JSON: {parse_err}")
        
        # Validate storage_state structure (must be a dict with expected Playwright structure)
        if not isinstance(storage_state, dict):
            raise TypeError(f"storage_state must be a dict or JSON string, got {type(storage_state)}")
        
        # Validate it has Playwright storage state structure
        if "cookies" not in storage_state and "origins" not in storage_state:
            logger.warning("⚠️ storage_state missing 'cookies' or 'origins' - might be invalid")
            # Log structure for debugging
            logger.debug(f"storage_state keys: {list(storage_state.keys()) if isinstance(storage_state, dict) else 'N/A'}")
        
        logger.info(f"✅ Using storage_state with {len(storage_state.get('cookies', []))} cookies")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            
            # CRITICAL: Node.js already destroyed all old contexts and waited 10 seconds
            # We skip the concurrent session clearing step here to avoid conflicts
            # Just create the authenticated context directly
            logger.info("✅ Creating authenticated context with storage_state (Node.js already cleared old sessions)...")
            
            # Create context with the storage_state from login
            context = await browser.new_context(storage_state=storage_state, accept_downloads=True)
            page = await context.new_page()
            
            # Wait a moment for context to initialize
            await asyncio.sleep(3)
            
            # CRITICAL: Navigate directly to dashboard using the valid cookies from storage_state
            # We skip root URL to avoid any redirect delays or session validation issues
            # The cookies in storage_state are valid - we just need to use them in this NEW browser context
            logger.info("🔍 Navigating directly to dashboard using storage_state cookies...")
            await page.goto("https://compliancenominationportal.in.pwc.com/BGVAdmin/BGVDashboard", wait_until="networkidle", timeout=60000)
            await asyncio.sleep(5)  # Wait for dashboard to fully load
            
            # Check current URL after navigation
            current_url = page.url
            logger.info(f"📍 Current URL after dashboard navigation: {current_url}")
            
            # Check for error page or concurrent access denial (current_url is already set above)
            logger.info(f"📍 Final current URL: {current_url}")
            
            # CRITICAL: Check for AccessDeniedConcurrent - session expired/invalid
            if "AccessDeniedConcurrent" in current_url or "/Login/AccessDeniedConcurrent" in current_url:
                error_screenshot = f"/tmp/session_expired_{datetime.now().strftime('%H%M%S')}.png"
                await page.screenshot(path=error_screenshot, full_page=True)
                await browser.close()
                logger.error(f"❌ Session expired - AccessDeniedConcurrent detected: {current_url}")
                
                # Provide detailed troubleshooting info
                page_text = await page.locator('body').inner_text() if not page.is_closed() else ""
                raise HTTPException(
                    status_code=401,
                    detail=(
                        f"Session expired — please start new login session via Node.js.\n\n"
                        f"AccessDeniedConcurrent detected. This means:\n"
                        f"1. Another session is still active on PwC server (may need manual logout)\n"
                        f"2. Session storage_state is invalid or expired\n"
                        f"3. Timing issue - Node.js may need more time to close old sessions\n\n"
                        f"Recommended actions:\n"
                        f"- Wait 60+ seconds after Node.js login completes\n"
                        f"- Manually logout from PwC portal in any browser\n"
                        f"- Check if scheduler is running (creates new session every 1h45m)\n\n"
                        f"URL: {current_url}\n"
                        f"Screenshot: {error_screenshot}"
                    )
                )
            
            if "ErrorPage" in current_url or "Oops" in current_url:
                logger.warning("Detected error page, trying alternative navigation...")
                error_screenshot = f"/tmp/error_page_{datetime.now().strftime('%H%M%S')}.png"
                await page.screenshot(path=error_screenshot, full_page=True)
                logger.error(f"❌ Error page detected: {current_url}. Screenshot: {error_screenshot}")
                
                # Try navigating to home page first, then dashboard
                try:
                    logger.info("Attempting navigation via home page...")
                    await page.goto("https://compliancenominationportal.in.pwc.com", wait_until="networkidle", timeout=30000)
                    await asyncio.sleep(3)
                    
                    # Try clicking dashboard link if it exists
                    dashboard_clicked = await try_click_selector(page, [
                        'a[href*="BGVDashboard"]',
                        'a[href*="dashboard"]',
                        'a:has-text("Dashboard")',
                        'text="Dashboard"',
                        'a:has-text("Home")',
                        'text="Home"'
                    ], timeout_per=5000)
                    
                    if dashboard_clicked:
                        await page.wait_for_load_state("networkidle", timeout=30000)
                        await asyncio.sleep(3)
                        current_url = page.url
                        logger.info(f"📍 Navigated via link to: {current_url}")
                    
                    # If still on error page, try direct URL again
                    if "ErrorPage" in page.url or "Oops" in page.url:
                        logger.warning("Still on error page, trying direct dashboard URL again...")
                        await page.goto("https://compliancenominationportal.in.pwc.com/BGVAdmin/BGVDashboard", wait_until="networkidle", timeout=30000)
                        await asyncio.sleep(3)
                        
                except Exception as nav_err:
                    logger.error(f"Alternative navigation failed: {nav_err}")
            
            # Final check - verify we're on dashboard, not error page
            final_url = page.url
            logger.info(f"📍 Final URL before validation: {final_url}")
            
            # Check for error page in URL
            if "ErrorPage" in final_url or "Oops" in final_url:
                error_screenshot = f"/tmp/final_error_page_{datetime.now().strftime('%H%M%S')}.png"
                await page.screenshot(path=error_screenshot, full_page=True)
                page_text = await page.locator('body').inner_text()
                
                # Check if session might be invalid
                if "not found" in page_text.lower() or "error" in page_text.lower():
                    raise Exception(
                        f"❌ Unable to access dashboard - stuck on error page. "
                        f"This usually means:\n"
                        f"1. Session expired or invalid\n"
                        f"2. User doesn't have dashboard access\n"
                        f"3. Dashboard URL requires authentication that session lacks\n\n"
                        f"Expected: https://compliancenominationportal.in.pwc.com/BGVAdmin/BGVDashboard\n"
                        f"Actual URL: {final_url}\n"
                        f"Screenshot: {error_screenshot}\n"
                        f"Please verify the session is valid and try logging in again."
                    )
                else:
                    raise Exception(f"Unable to access dashboard - stuck on error page: {final_url}. Screenshot: {error_screenshot}")
            
            # Check if page has dashboard content
            try:
                page_title = await page.title()
                page_text = await page.locator('body').inner_text()
                
                # More thorough error detection
                error_indicators = [
                    "not found" in page_text.lower(),
                    "error" in page_title.lower(),
                    "sorry" in page_text.lower() and "error" in page_text.lower(),
                    "requested page not found" in page_text.lower()
                ]
                
                if any(error_indicators):
                    error_screenshot = f"/tmp/dashboard_error_{datetime.now().strftime('%H%M%S')}.png"
                    await page.screenshot(path=error_screenshot, full_page=True)
                    raise Exception(
                        f"Dashboard page appears to have error content. "
                        f"URL: {final_url}\n"
                        f"Page title: {page_title}\n"
                        f"Screenshot: {error_screenshot}"
                    )
            except Exception as check_err:
                # If we can't check page content, at least verify URL is correct
                if "ErrorPage" not in final_url and "Oops" not in final_url:
                    logger.warning(f"Could not verify page content but URL looks OK: {check_err}")
                else:
                    raise check_err
            
            # Verify we're actually on dashboard (not just not on error page)
            if "BGVDashboard" not in final_url and "/dashboard" not in final_url.lower() and "compliancenominationportal" in final_url:
                logger.warning(f"Not on dashboard URL, but also not on error page: {final_url}")
                
                # CRITICAL: Check again for AccessDeniedConcurrent after retry
                if "AccessDeniedConcurrent" in final_url or "/Login/AccessDeniedConcurrent" in final_url:
                    error_screenshot = f"/tmp/session_expired_retry_{datetime.now().strftime('%H%M%S')}.png"
                    await page.screenshot(path=error_screenshot, full_page=True)
                    await browser.close()
                    logger.error(f"❌ Session expired on retry - AccessDeniedConcurrent: {final_url}")
                    raise HTTPException(
                        status_code=401,
                        detail=f"Session expired — please start new login session via Node.js. URL: {final_url}. Screenshot: {error_screenshot}"
                    )
                
                # Try to navigate to dashboard one more time
                try:
                    await page.goto("https://compliancenominationportal.in.pwc.com/BGVAdmin/BGVDashboard", wait_until="networkidle", timeout=30000)
                    await asyncio.sleep(3)
                    final_url = page.url
                    
                    # Final check for AccessDeniedConcurrent
                    if "AccessDeniedConcurrent" in final_url or "/Login/AccessDeniedConcurrent" in final_url:
                        error_screenshot = f"/tmp/session_expired_final_{datetime.now().strftime('%H%M%S')}.png"
                        await page.screenshot(path=error_screenshot, full_page=True)
                        await browser.close()
                        logger.error(f"❌ Session expired on final retry - AccessDeniedConcurrent: {final_url}")
                        raise HTTPException(
                            status_code=401,
                            detail=f"Session expired — please start new login session via Node.js. URL: {final_url}. Screenshot: {error_screenshot}"
                        )
                    
                    if "ErrorPage" in final_url or "Oops" in final_url:
                        error_screenshot = f"/tmp/dashboard_nav_failed_{datetime.now().strftime('%H%M%S')}.png"
                        await page.screenshot(path=error_screenshot, full_page=True)
                        raise Exception(f"Final navigation to dashboard failed. URL: {final_url}. Screenshot: {error_screenshot}")
                except HTTPException:
                    raise  # Re-raise HTTPException for session expired
                except Exception as nav_err:
                    if "ErrorPage" in str(nav_err) or "Oops" in str(nav_err):
                        raise
                    logger.warning(f"Dashboard navigation warning: {nav_err}")
            
            # CRITICAL: Final validation before proceeding - must not be on error page or AccessDeniedConcurrent
            final_check_url = page.url
            
            # Final check for AccessDeniedConcurrent
            if "AccessDeniedConcurrent" in final_check_url or "/Login/AccessDeniedConcurrent" in final_check_url:
                error_screenshot = f"/tmp/pre_advance_search_expired_{datetime.now().strftime('%H%M%S')}.png"
                await page.screenshot(path=error_screenshot, full_page=True)
                await browser.close()
                logger.error(f"❌ CRITICAL: Session expired before Advance Search - AccessDeniedConcurrent: {final_check_url}")
                raise HTTPException(
                    status_code=401,
                    detail=f"Session expired — please start new login session via Node.js. Current URL: {final_check_url}. Screenshot: {error_screenshot}"
                )
            
            if "ErrorPage" in final_check_url or "Oops" in final_check_url:
                error_screenshot = f"/tmp/pre_advance_search_error_{datetime.now().strftime('%H%M%S')}.png"
                await page.screenshot(path=error_screenshot, full_page=True)
                page_text_final = await page.locator('body').inner_text()
                raise Exception(
                    f"❌ CRITICAL: Still on error page when trying to click Advance Search!\n"
                    f"This means the session cannot access the dashboard.\n\n"
                    f"Possible causes:\n"
                    f"1. Session expired or invalid\n"
                    f"2. User account doesn't have dashboard access permissions\n"
                    f"3. Dashboard URL changed or requires different authentication\n"
                    f"4. Session storage_state is missing required cookies/state\n\n"
                    f"Current URL: {final_check_url}\n"
                    f"Page contains: {page_text_final[:200]}...\n"
                    f"Screenshot: {error_screenshot}\n\n"
                    f"Action: Please verify login session is valid and user has dashboard access."
                )
            
            # Verify URL actually contains dashboard
            if "BGVDashboard" not in final_check_url and "/dashboard" not in final_check_url.lower():
                logger.warning(f"⚠️ URL doesn't contain 'BGVDashboard' or '/dashboard': {final_check_url}")
                # This might be OK if it's a redirect, but log it
            
            logger.info("✅ Successfully navigated to dashboard - URL validated")
            
            # CRITICAL STEP 1: Click "Advance search" FIRST
            # This makes the "Export to excel" button visible
            # The Export button (id="downloadExcel") only appears after Advance search is clicked
            logger.info("🔍 STEP 1: Clicking 'Advance search' to reveal Export button...")
            await click_advance_search(page)
            logger.info("✅ 'Advance search' clicked - Export button (#downloadExcel) should now be visible")
            
            # Verify Export button is visible after Advance search
            try:
                await asyncio.sleep(3)  # Brief wait for UI to update
                export_visible = await page.locator('#downloadExcel').is_visible(timeout=5000)
                if export_visible:
                    logger.info("✅ Verified: Export button (#downloadExcel) is visible after Advance search")
                else:
                    logger.warning("⚠️ Export button (#downloadExcel) not immediately visible, but continuing...")
            except Exception as e:
                logger.warning(f"⚠️ Could not verify Export button visibility (non-critical): {e}")
            
            download_dir = TMP_DIR / "dashboard_exports"
            download_dir.mkdir(parents=True, exist_ok=True)

            # STEP 2: Process each tab sequentially
            # For each tab: Select tab → Wait for load → Click Export → Wait for download → Next tab
            logger.info(f"\n{'='*70}")
            logger.info(f"📋 STEP 2: Processing {len(TABS)} tabs sequentially...")
            logger.info(f"{'='*70}\n")
            results = []
            for idx, tab in enumerate(TABS, 1):
                try:
                    logger.info(f"\n{'='*70}")
                    logger.info(f"🔄 Processing tab {idx}/{len(TABS)}: {tab}")
                    logger.info(f"{'='*70}")
                    result = await export_tab(page, tab, download_dir)
                    results.append(result)
                    # Wait between tabs to ensure system is ready for next export
                    if idx < len(TABS):
                        logger.info(f"⏸️ Waiting 25s before processing next tab...")
                        await asyncio.sleep(25)
                except Exception as e:
                    logger.error(f"❌ Error exporting tab '{tab}': {e}")
                    await page.screenshot(path=f"/tmp/{tab}_fail_{datetime.now().strftime('%H%M%S')}.png")
                    results.append({"tab": tab, "status": "error", "error": str(e)})

            await perform_logout(page)
            await browser.close()
            
            # STEP 3: Upload all exported Excel files to Google Sheets
            logger.info(f"\n{'='*70}")
            logger.info(f"🔍 STEP 3: Checking Google Sheets configuration...")
            logger.info(f"📋 spreadsheet_id provided: {spreadsheet_id}")
            logger.info(f"📋 GOOGLE_SHEET_ID env var: {GOOGLE_SHEET_ID}")
            logger.info(f"📋 GOOGLE_CREDENTIALS_JSON set: {'Yes' if GOOGLE_CREDENTIALS_JSON else 'No'}")
            logger.info(f"{'='*70}\n")
            
            if spreadsheet_id:
                logger.info(f"📤 STEP 3: Uploading exported Excel files to Google Sheets...")
                logger.info(f"📋 Using spreadsheet_id: {spreadsheet_id}\n")
                
                try:
                    # Verify credentials are available
                    if not GOOGLE_CREDENTIALS_JSON:
                        raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable is not set. Please add it in Render environment variables.")
                    
                    logger.info(f"✅ Google credentials found, starting upload...")
                    tab_results = await sync_all_tabs_to_sheets(download_dir, spreadsheet_id)
                    logger.info(f"✅ Google Sheets upload completed: {len(tab_results)} tab(s) processed")
                    return {"ok": True, "tabs": results, "sheets_sync": tab_results}
                except Exception as sync_err:
                    logger.error(f"❌ Google Sheets sync failed: {sync_err}")
                    logger.error(f"❌ Error type: {type(sync_err).__name__}")
                    import traceback
                    logger.error(f"❌ Error traceback: {traceback.format_exc()}")
                    logger.error(f"Export completed but Sheets sync failed - files saved in {download_dir}")
                    return {"ok": True, "tabs": results, "sheets_sync_error": str(sync_err), "error_type": type(sync_err).__name__}
            else:
                logger.warning("⚠️ No GOOGLE_SHEET_ID provided - skipping Google Sheets upload")
                logger.warning("💡 Set GOOGLE_SHEET_ID environment variable in Render to enable Sheets upload")
                return {"ok": True, "tabs": results, "sheets_sync": "skipped", "reason": "GOOGLE_SHEET_ID not set"}

    except Exception as e:
        logger.error(f"Export error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class ExportRequest(BaseModel):
    session_id: str
    spreadsheet_id: Optional[str] = None
    storage_state: Optional[Dict] = None
    
    @field_validator('storage_state', mode='before')
    @classmethod
    def parse_storage_state(cls, v):
        """Ensure storage_state is always a dict, not a string"""
        if v is None:
            return None
        if isinstance(v, str):
            # If it came as a string (double-encoded), parse it
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                raise ValueError(f"storage_state is a string but not valid JSON")
        if isinstance(v, dict):
            return v
        raise ValueError(f"storage_state must be a dict or JSON string, got {type(v)}")


@app.post("/export-dashboard")
async def export_endpoint(req: ExportRequest):
    spreadsheet_id = req.spreadsheet_id or GOOGLE_SHEET_ID
    if not spreadsheet_id:
        raise HTTPException(status_code=400, detail="spreadsheet_id required (set GOOGLE_SHEET_ID env or provide in request)")
    
    # Handle storage_state - ensure it's properly formatted
    storage_state = req.storage_state
    if storage_state:
        # If Pydantic received it as a string (due to JSON double-encoding), parse it
        if isinstance(storage_state, str):
            try:
                storage_state = json.loads(storage_state)
                logger.info("✅ Parsed storage_state from string in request")
            except json.JSONDecodeError:
                raise HTTPException(
                    status_code=400,
                    detail="storage_state is a string but not valid JSON. Ensure Node.js sends it as an object, not a stringified JSON."
                )
        
        # Validate it's a dict
        if not isinstance(storage_state, dict):
            raise HTTPException(
                status_code=400,
                detail=f"storage_state must be a dict or valid JSON string, got {type(storage_state)}"
            )
        
        logger.info(f"✅ Received storage_state with {len(storage_state.get('cookies', []))} cookies")
    
    result = await export_dashboard(req.session_id, spreadsheet_id, storage_state)
    return JSONResponse(content=result)


@app.get("/screenshots")
async def list_screenshots():
    """List all available screenshots"""
    screenshot_files = []
    tmp_dir = Path("/tmp")
    for file in tmp_dir.glob("*.png"):
        if file.is_file():
            screenshot_files.append({
                "filename": file.name,
                "size_bytes": file.stat().st_size,
                "modified": datetime.fromtimestamp(file.stat().st_mtime).isoformat(),
                "url": f"/screenshots/{file.name}"
            })
    return JSONResponse(content={
        "ok": True,
        "screenshots": sorted(screenshot_files, key=lambda x: x["modified"], reverse=True),
        "count": len(screenshot_files)
    })


@app.get("/screenshots/{filename}")
async def get_screenshot(filename: str):
    """Get a specific screenshot image"""
    file_path = Path(f"/tmp/{filename}")
    if file_path.exists():
        return FileResponse(file_path, media_type="image/png")
    raise HTTPException(status_code=404, detail=f"Screenshot not found: {filename}")


class UploadRequest(BaseModel):
    spreadsheet_id: Optional[str] = None


@app.post("/upload-to-sheets")
async def upload_to_sheets_only(req: Optional[UploadRequest] = None):
    """
    Upload existing Excel files to Google Sheets (STEP 3 only - no browser automation)
    Reads Excel files from /tmp/dashboard_exports/ and uploads them to Google Sheets
    """
    try:
        spreadsheet_id = None
        if req:
            spreadsheet_id = req.spreadsheet_id
        spreadsheet_id = spreadsheet_id or GOOGLE_SHEET_ID
        
        if not spreadsheet_id:
            raise HTTPException(
                status_code=400, 
                detail="spreadsheet_id required (set GOOGLE_SHEET_ID env or provide in request body)"
            )
        
        if not GOOGLE_CREDENTIALS_JSON:
            raise HTTPException(
                status_code=400,
                detail="GOOGLE_CREDENTIALS_JSON environment variable is required"
            )
        
        logger.info(f"\n{'='*70}")
        logger.info(f"📤 Uploading existing Excel files to Google Sheets (STEP 3 only)")
        logger.info(f"📋 Using spreadsheet_id: {spreadsheet_id}")
        logger.info(f"{'='*70}\n")
        
        download_dir = TMP_DIR / "dashboard_exports"
        
        if not download_dir.exists():
            raise HTTPException(
                status_code=404,
                detail=f"Export directory not found: {download_dir}. Run full export first to generate Excel files."
            )
        
        # Check which Excel files exist
        existing_files = []
        for tab in TABS:
            excel_path = download_dir / f"{tab}.xlsx"
            if excel_path.exists():
                file_size = excel_path.stat().st_size
                existing_files.append({"tab": tab, "file": str(excel_path), "size_bytes": file_size})
        
        if not existing_files:
            raise HTTPException(
                status_code=404,
                detail=f"No Excel files found in {download_dir}. Files must exist before upload."
            )
        
        logger.info(f"📁 Found {len(existing_files)} Excel file(s) to upload:")
        for file_info in existing_files:
            logger.info(f"   - {file_info['tab']}: {file_info['size_bytes']} bytes")
        
        # Upload all tabs to Sheets
        tab_results = await sync_all_tabs_to_sheets(download_dir, spreadsheet_id)
        
        success_count = sum(1 for r in tab_results if r.get("status") != "error" and "error" not in r)
        error_count = len(tab_results) - success_count
        
        logger.info(f"\n{'='*70}")
        logger.info(f"✅ Upload completed: {success_count} successful, {error_count} errors")
        logger.info(f"{'='*70}\n")
        
        return JSONResponse(content={
            "ok": True,
            "message": f"Uploaded {success_count} tab(s) to Google Sheets",
            "spreadsheet_id": spreadsheet_id,
            "tab_results": tab_results,
            "summary": {
                "total": len(tab_results),
                "successful": success_count,
                "errors": error_count
            }
        })
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload to Sheets error: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")


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
