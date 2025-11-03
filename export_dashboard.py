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

EXPORT_TIMEOUT = 240000  # 4 minutes in milliseconds (240,000 ms)
TABS = [
    "Today's allocated",
    "Not started",
    "Draft",
    "Rejected / Insufficient",
    "Submitted",
    "Work in progress",
    "BGV closed",
]

# Lock to prevent concurrent exports
export_lock = asyncio.Lock()
export_in_progress = False
current_export_session_id = None


def get_sheets_service():
    if not GOOGLE_CREDENTIALS_JSON:
        raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable is required")
    
    try:
        creds_json = GOOGLE_CREDENTIALS_JSON
        
        # Handle the case where JSON might have been stored with actual newlines
        # or where the private_key has literal \n characters that need to be parsed
        
        # Strategy: Try multiple parsing approaches
        creds_info = None
        parse_errors = []
        
        # Try 1: Direct JSON parse (if properly formatted)
        try:
            creds_info = json.loads(creds_json)
        except json.JSONDecodeError as e1:
            parse_errors.append(f"Direct parse: {e1}")
            
            # Try 2: Replace literal newlines with escaped \n (for private_key)
            # This handles cases where the env var was pasted with actual newlines
            try:
                # Remove any actual newlines outside of string values
                # But preserve \n escape sequences inside strings
                import re
                # Replace actual newlines that are not part of an escape sequence
                # Pattern: newline not preceded by backslash
                fixed_json = re.sub(r'(?<!\\)\n', '\\\\n', creds_json)
                fixed_json = re.sub(r'(?<!\\)\r', '\\\\r', fixed_json)
                # Now unescape: convert \\n back to \n for JSON parsing
                fixed_json = fixed_json.replace('\\\\n', '\\n').replace('\\\\r', '\\r')
                creds_info = json.loads(fixed_json)
            except (json.JSONDecodeError, Exception) as e2:
                parse_errors.append(f"Newline fix: {e2}")
                
                # Try 3: Base64 decode if it looks encoded
                try:
                    import base64
                    # Check if it's base64 encoded
                    decoded = base64.b64decode(creds_json).decode('utf-8')
                    creds_info = json.loads(decoded)
                except Exception as e3:
                    parse_errors.append(f"Base64 decode: {e3}")
                    
                    # Try 4: Manual reconstruction (last resort)
                    # Extract key parts and rebuild JSON
                    try:
                        # Try to find the pattern and fix it manually
                        # The error suggests issue at column 166 (likely in private_key)
                        # Try removing control characters from private_key value
                        import re
                        # Pattern: "private_key": "value"
                        pattern = r'"private_key"\s*:\s*"([^"]*)"'
                        match = re.search(pattern, creds_json)
                        if match:
                            # Get the private key value
                            pk_value = match.group(1)
                            # Clean it - replace actual newlines with \n
                            pk_cleaned = pk_value.replace('\n', '\\n').replace('\r', '\\r')
                            # Replace in original JSON
                            fixed_json = creds_json.replace(f'"private_key": "{pk_value}"', f'"private_key": "{pk_cleaned}"')
                            creds_info = json.loads(fixed_json)
                        else:
                            raise ValueError("Could not find private_key in JSON")
                    except Exception as e4:
                        parse_errors.append(f"Manual fix: {e4}")
                        logger.error(f"All parsing attempts failed. Errors: {parse_errors}")
                        logger.error(f"JSON sample (first 300 chars): {creds_json[:300]}")
                        raise ValueError(f"Failed to parse GOOGLE_CREDENTIALS_JSON after all attempts. Errors: {parse_errors}")
        
        # Validate the structure
        if not isinstance(creds_info, dict):
            raise ValueError("GOOGLE_CREDENTIALS_JSON must be a valid JSON object")
        
        # Ensure private_key has actual newlines (not escaped strings)
        if 'private_key' in creds_info and isinstance(creds_info['private_key'], str):
            # Replace literal \n with actual newlines (if they're still escaped)
            if '\\n' in creds_info['private_key'] and '\n' not in creds_info['private_key']:
                creds_info['private_key'] = creds_info['private_key'].replace('\\n', '\n')
        
        creds = service_account.Credentials.from_service_account_info(
            creds_info, 
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        return build("sheets", "v4", credentials=creds)
    except Exception as e:
        logger.error(f"Failed to parse GOOGLE_CREDENTIALS_JSON: {e}")
        logger.error(f"JSON length: {len(GOOGLE_CREDENTIALS_JSON) if GOOGLE_CREDENTIALS_JSON else 0}")
        logger.error(f"First 300 chars: {GOOGLE_CREDENTIALS_JSON[:300] if GOOGLE_CREDENTIALS_JSON else 'N/A'}")
        raise ValueError(f"Invalid GOOGLE_CREDENTIALS_JSON: {e}")


async def sync_to_sheets_with_audit(tab_name: str, excel_path: Path, spreadsheet_id: str):
    """
    Sync Excel data to Google Sheets (adds new, updates changed, skips identical).
    Logs audit only for modified rows (not new ones).
    
    Returns: {"tab": tab_name, "new_rows": count, "updated_rows": count, "skipped": count, "audit_entries": count}
    """
    try:
        # Get Google Sheets service
        service = get_sheets_service()
        sheets = service.spreadsheets()
        
        logger.info(f"📊 Starting incremental sync for {tab_name}...")
        
        # Read Excel data
        df_new = pd.read_excel(excel_path, engine="openpyxl").fillna("").astype(str)
        
        # Fetch existing sheet data
        try:
            sheet_data = (
                sheets.values()
                .get(spreadsheetId=spreadsheet_id, range=f"'{tab_name}'!A:Z")
                .execute()
                .get("values", [])
            )
        except Exception:
            sheet_data = []
        
        if sheet_data:
            headers = sheet_data[0]
            df_existing = pd.DataFrame(sheet_data[1:], columns=headers)
        else:
            df_existing = pd.DataFrame(columns=df_new.columns)
        
        # Align columns safely
        df_new = df_new.reindex(columns=df_existing.columns, fill_value="") if not df_existing.empty else df_new
        
        # Normalize (remove type diff, trim spaces)
        df_existing = df_existing.fillna("").astype(str).apply(lambda x: x.str.strip())
        df_new = df_new.fillna("").astype(str).apply(lambda x: x.str.strip())
        
        # Identify unique key column
        UNIQUE_KEY = "Candidate ID" if "Candidate ID" in df_new.columns else df_new.columns[0]
        
        # Merge datasets to detect changes
        merged = pd.merge(
            df_existing, df_new,
            on=UNIQUE_KEY,
            how="outer",
            indicator=True,
            suffixes=("_old", "_new")
        )
        
        new_rows = merged[merged["_merge"] == "right_only"][df_new.columns]
        updated_rows = []
        audit_log_entries = []
        
        # Detect changes for existing rows
        for _, row in merged[merged["_merge"] == "both"].iterrows():
            changed_cols = []
            for col in df_existing.columns:
                if col == UNIQUE_KEY:
                    continue
                old_val = str(row.get(f"{col}_old", "")).strip()
                new_val = str(row.get(f"{col}_new", "")).strip()
                if old_val != new_val:
                    changed_cols.append((col, old_val, new_val))
            
            if changed_cols:
                updated_row = {col: row.get(f"{col}_new", "") for col in df_existing.columns}
                updated_rows.append(updated_row)
                
                # Create detailed audit log entries
                for c, o, n in changed_cols:
                    audit_log_entries.append([
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        tab_name,
                        row[UNIQUE_KEY],
                        "UPDATED",
                        c,
                        o,
                        n
                    ])
                
                change_details = ", ".join([f"{c}: '{o}' → '{n}'" for c, o, n in changed_cols])
                logger.info(f"✏️ {tab_name} | {UNIQUE_KEY}={row[UNIQUE_KEY]} | {change_details}")
        
        # ➕ Add only new rows (no audit log for these)
        if not new_rows.empty:
            sheets.values().append(
                spreadsheetId=spreadsheet_id,
                range=f"'{tab_name}'!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": new_rows.values.tolist()},
            ).execute()
            logger.info(f"➕ Added {len(new_rows)} new rows to '{tab_name}'")
        
        # ✏️ Update modified rows
        if updated_rows:
            df_updates = pd.DataFrame(updated_rows)
            for _, row in df_updates.iterrows():
                row_index = df_existing[df_existing[UNIQUE_KEY] == row[UNIQUE_KEY]].index
                if not row_index.empty:
                    sheets.values().update(
                        spreadsheetId=spreadsheet_id,
                        range=f"'{tab_name}'!A{row_index[0] + 2}",
                        valueInputOption="RAW",
                        body={"values": [row.tolist()]},
                    ).execute()
            logger.info(f"✅ Updated {len(updated_rows)} modified rows in '{tab_name}'")
        
        # 🧾 Append audit logs (only for modified rows)
        if audit_log_entries:
            try:
                sheets.values().append(
                    spreadsheetId=spreadsheet_id,
                    range="'Audit Log'!A1",
                    valueInputOption="RAW",
                    insertDataOption="INSERT_ROWS",
                    body={"values": audit_log_entries},
                ).execute()
                logger.info(f"🕒 Logged {len(audit_log_entries)} change events to 'Audit Log'")
            except Exception:
                # Create "Audit Log" sheet if not found
                logger.warning("⚠️ 'Audit Log' sheet missing — creating new one.")
                service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": [{"addSheet": {"properties": {"title": "Audit Log"}}}]}
                ).execute()
                headers = [["Timestamp", "Tab Name", "Candidate ID", "Action", "Column", "Old Value", "New Value"]]
                sheets.values().update(
                    spreadsheetId=spreadsheet_id,
                    range="'Audit Log'!A1",
                    valueInputOption="RAW",
                    body={"values": headers + audit_log_entries},
                ).execute()
                logger.info("✅ Created and populated new 'Audit Log' sheet")
        
        skipped = len(df_existing) - len(updated_rows) - len(new_rows) if not df_existing.empty else 0
        logger.info(f"🟢 Skipped {max(skipped, 0)} identical rows in '{tab_name}'")
        logger.info(f"✅ Completed sync for {tab_name}")
        
        return {
            "tab": tab_name,
            "new_rows": len(new_rows),
            "updated_rows": len(updated_rows),
            "skipped": max(skipped, 0),
            "audit_entries": len(audit_log_entries)
        }
        
    except Exception as e:
        logger.error(f"❌ Sync failed for {tab_name}: {e}", exc_info=True)
        return {"tab": tab_name, "error": str(e)}


async def sync_all_tabs_to_sheets(download_dir: Path, spreadsheet_id: str):
    """
    Upload all exported Excel files to Google Sheets.
    
    ROOT FIX: Each tab is processed independently with fresh context.
    """
    import gc
    
    tab_results = []
    
    logger.info(f"\n{'='*70}")
    logger.info(f"🚀 Starting sync for {len(TABS)} tabs")
    logger.info(f"📁 Download directory: {download_dir}")
    logger.info(f"{'='*70}\n")
    
    for idx, tab in enumerate(TABS, 1):
        try:
            logger.info(f"\n{'='*70}")
            logger.info(f"📋 Processing tab {idx}/{len(TABS)}: {tab}")
            logger.info(f"{'='*70}")
            
            excel_path = download_dir / f"{tab}.xlsx"
            
            if not excel_path.exists():
                logger.warning(f"⚠️ Excel file not found for {tab}: {excel_path}")
                tab_results.append({"tab": tab, "status": "error", "error": "Excel file not found"})
                continue
            
            # ROOT FIX: Process each tab independently with explicit cleanup between tabs
            result = await sync_to_sheets_with_audit(tab, excel_path, spreadsheet_id)
            tab_results.append(result)
            
            # ROOT FIX: Explicit cleanup between tabs
            gc.collect()
            logger.info(f"✅ Completed sync for {tab} (tab {idx}/{len(TABS)})")
            
            # Small delay between tabs to ensure file system is ready
            if idx < len(TABS):
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"❌ Error syncing {tab} to Sheets: {e}")
            tab_results.append({"tab": tab, "status": "error", "error": str(e)})
    
    logger.info(f"\n{'='*70}")
    logger.info(f"✅ Google Sheets upload completed: {len(tab_results)} tab(s) processed")
    logger.info(f"{'='*70}\n")
    
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


async def export_tab(page: Page, tab_name: str, download_dir: Path, is_first_tab: bool = False):
    # NOTE: is_first_tab parameter kept for backward compatibility but all tabs are clicked now
    logger.info(f"📊 Exporting tab: {tab_name}")
    
    # USER REQUEST: For ALL tabs (including first), click to confirm selection
    logger.info(f"🖱️ Clicking tab to confirm selection: '{tab_name}'")
    
    tab_selectors = [
        f'text="{tab_name}"',
        f'a:has-text("{tab_name}")',
        f'button:has-text("{tab_name}")',
        f'li:has-text("{tab_name}")',
        f'[data-tab*="{tab_name}"]',
        f'[aria-label*="{tab_name}"]',
        f'*:has-text("{tab_name}")',
    ]
    
    tab_clicked = False
    
    # Strategy 1: Try Playwright locator clicks
    for tab_sel in tab_selectors:
        try:
            locator = page.locator(tab_sel).first
            if await locator.is_visible(timeout=5000):
                await locator.scroll_into_view_if_needed()
                await asyncio.sleep(0.5)
                await locator.click(force=True)
                logger.info(f"✅ Clicked tab '{tab_name}' via selector: {tab_sel}")
                tab_clicked = True
                break
        except Exception as e:
            logger.debug(f"Tab selector {tab_sel} failed: {e}")
            continue
    
    # Strategy 2: If Playwright failed, try JavaScript click
    if not tab_clicked:
        logger.warning(f"Playwright clicks failed, trying JavaScript click for tab '{tab_name}'")
        try:
            js_clicked = await page.evaluate(f"""
                (tabName) => {{
                    // Try to find tab by text content
                    const allElements = Array.from(document.querySelectorAll('*'));
                    for (let el of allElements) {{
                        const text = (el.textContent || el.innerText || '').trim();
                        if (text === tabName || text.includes(tabName)) {{
                            // Check if it's clickable (button, link, or has click handler)
                            if (el.tagName === 'BUTTON' || el.tagName === 'A' || 
                                el.tagName === 'LI' || el.getAttribute('role') === 'tab' ||
                                el.onclick || el.getAttribute('data-tab')) {{
                                el.click();
                                return true;
                            }}
                        }}
                    }}
                    return false;
                }}
            """, tab_name)
            
            if js_clicked:
                logger.info(f"✅ Clicked tab '{tab_name}' via JavaScript")
                tab_clicked = True
                await asyncio.sleep(2)
            else:
                logger.error(f"❌ JavaScript could not find clickable element for tab '{tab_name}'")
        except Exception as js_err:
            logger.error(f"❌ JavaScript click failed: {js_err}")
    
    if not tab_clicked:
        screenshot_path = f"/tmp/tab_click_fail_{tab_name.replace(' ', '_')}_{datetime.now().strftime('%H%M%S')}.png"
        await page.screenshot(path=screenshot_path, full_page=True)
        raise Exception(f"Could not click tab: {tab_name} (screenshot: {screenshot_path})")
    
    # USER REQUEST: Wait 50 seconds for page to load after clicking tab
    logger.info(f"⏳ Waiting 50 seconds for tab '{tab_name}' to load after confirmation...")
    await asyncio.sleep(50)
    logger.info(f"✅ Tab '{tab_name}' loaded after confirmation")

    # Step 3: Click "Export to excel" button
    # CRITICAL: Export button appears AFTER Advance search is clicked and tab is selected
    # The button has ID "downloadExcel" and class "btn btn-danger clsdisableAction"
    # HTML: <input type="button" value="Export to excel" id="downloadExcel" class="btn btn-danger clsdisableAction">
    logger.info(f"📥 Step 3: Looking for Export to Excel button for {tab_name}...")
    
    file_path = download_dir / f"{tab_name}.xlsx"
    
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
    
    # ROOT FIX: Set up download listener BEFORE clicking (must be active when click happens)
    # expect_download context manager catches the NEXT download
    logger.info(f"⏳ Setting up download listener (timeout: {EXPORT_TIMEOUT/1000}s) and clicking export...")
    
    async with page.expect_download(timeout=EXPORT_TIMEOUT) as download_info:
        # Click button while download listener is active
        export_clicked = False
        for attempt in range(3):
            for sel in export_selectors:
                try:
                    # Wait up to 30 seconds for button to appear
                    await page.wait_for_selector(sel, timeout=30000)
                    locator = page.locator(sel).first
                    await locator.scroll_into_view_if_needed()
                    await asyncio.sleep(1)
                    
                    # Click the button (download listener is active)
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
        
        # Small wait for download to initiate after click
        await asyncio.sleep(2)
    
    # Download received - save it
    logger.info(f"⏳ Download received, saving file for '{tab_name}'...")
    try:
        download = await download_info.value
        await download.save_as(file_path)
        logger.info(f"💾 Download saved: {file_path} for tab '{tab_name}'")
    except Exception as download_err:
        error_str = str(download_err)
        if "timeout" in error_str.lower() or "Timeout" in error_str:
            raise Exception(f"Download timeout for {tab_name} after {EXPORT_TIMEOUT/1000} seconds")
        raise Exception(f"Download error for {tab_name}: {download_err}")

    await asyncio.sleep(2)
    
    # CRITICAL: Verify the downloaded file exists and has content
    if not file_path.exists():
        raise Exception(f"Downloaded file not found: {file_path}")
    
    file_size = file_path.stat().st_size
    logger.info(f"📦 Downloaded file size: {file_size} bytes for '{tab_name}'")
    
    if file_size < 100:  # Excel files should be at least 100 bytes (headers)
        raise Exception(f"Downloaded file too small ({file_size} bytes) - likely empty or corrupted")
    
    # Quick verification: Read first few rows to ensure it's not empty
    try:
        import pandas as pd
        df_check = pd.read_excel(file_path, engine='openpyxl', nrows=5)
        row_count_check = len(df_check)
        logger.info(f"✅ File verification: {row_count_check} rows in preview (file size: {file_size} bytes)")
        
        if row_count_check == 0:
            raise Exception(f"Downloaded Excel file appears empty (0 rows found)")
        
        # Log a sample of column names to help debug
        if len(df_check.columns) > 0:
            logger.info(f"📋 File columns preview: {list(df_check.columns[:5])}")
    except Exception as verify_err:
        logger.warning(f"⚠️ Could not verify file content (non-critical): {verify_err}")
        # Don't fail - file exists and has size, that's good enough
    
    # Final verification before returning
    if not file_path.exists() or file_path.stat().st_size == 0:
        raise Exception(f"File missing or empty for {tab_name}")

    await asyncio.sleep(1)
    
    # USER REQUEST: Wait 30 seconds after export before moving to next tab
    logger.info(f"⏳ Waiting 30 seconds after export before moving to next tab...")
    await asyncio.sleep(30)
    
    final_file_size = file_path.stat().st_size
    logger.info(f"✅ Step 4: Export completed for {tab_name} ({final_file_size} bytes)")
    logger.info(f"✅ ✅ Tab '{tab_name}' export workflow finished successfully")
    return {"tab": tab_name, "status": "done", "file_size": final_file_size}


async def perform_logout(page: Page):
    """Universal logout logic - click 'Welcome Sukrutha CR' text, then logout"""
    try:
        logger.info("🔒 Attempting logout...")
        await asyncio.sleep(3)  # allow dashboard JS to finish rendering
        
        # USER REQUEST: Click "Welcome Sukrutha CR" (the text, not dropdown arrow)
        # Then click logout from the dropdown that appears
        logger.info("🔍 Looking for 'Welcome Sukrutha CR' to click...")
        
        welcome_selectors = [
            "text='Welcome Sukrutha CR'",
            "text=/Welcome.*Sukrutha CR/i",
            "*:has-text('Welcome Sukrutha CR')",
            "text='Welcome'",
            "*:has-text('Sukrutha CR')"
        ]
        
        welcome_clicked = False
        for attempt in range(3):
            for sel in welcome_selectors:
                try:
                    element = await page.wait_for_selector(sel, timeout=8000, state="visible")
                    if element:
                        await element.click(force=True)
                        logger.info(f"✅ Clicked 'Welcome Sukrutha CR' via selector: {sel}")
                        welcome_clicked = True
                        await asyncio.sleep(2)  # Wait for dropdown to appear
                        break
                except Exception as e:
                    logger.debug(f"Welcome selector {sel} failed: {e}")
                    continue
            if welcome_clicked:
                break
        
        if not welcome_clicked:
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
                    # USER REQUEST: Click all tabs (including first) - no special handling needed
                    result = await export_tab(page, tab, download_dir, is_first_tab=False)
                    results.append(result)
                    # Note: Already waiting 30 seconds after each export inside export_tab()
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
    global export_in_progress, current_export_session_id
    
    # RULE 1: Only one export can run at a time (across all sessions)
    # This prevents concurrent exports and ensures one session = one export run
    if export_in_progress:
        logger.warning(f"⚠️ Export already in progress for session {current_export_session_id} - rejecting concurrent request for session {req.session_id}")
        raise HTTPException(
            status_code=429,
            detail=f"Export already in progress for session {current_export_session_id}. Only one export can run at a time. This request for session {req.session_id} is cancelled. Wait for the current export to finish and push data to sheets."
        )
    
    # Acquire lock to prevent race conditions
    async with export_lock:
        # Double-check after acquiring lock (handle race condition)
        if export_in_progress:
            logger.warning(f"⚠️ Export already in progress for session {current_export_session_id} (race condition caught) - rejecting request for session {req.session_id}")
            raise HTTPException(
                status_code=429,
                detail=f"Export already in progress for session {current_export_session_id}. Only one export can run at a time."
            )
        
        # Mark this session as the current export
        # This ensures: one session = one export run (any concurrent requests will be rejected above)
        export_in_progress = True
        current_export_session_id = req.session_id
        logger.info(f"🔒 Export lock acquired - starting export for session {req.session_id}. All other concurrent requests will be cancelled.")
        
        try:
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
            logger.info(f"✅ Export completed successfully for session {req.session_id} - data pushed to sheets")
            return JSONResponse(content=result)
        
        finally:
            # Always release the lock and clear session tracking, even if export fails
            # This allows the next auto-run (4 hours later) to start fresh
            export_in_progress = False
            completed_session = current_export_session_id
            current_export_session_id = None
            logger.info(f"🔓 Export lock released - session {completed_session} export finished. Next auto-run can start fresh. Any queued/concurrent requests for this session were cancelled.")


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
        
        # Create directory if it doesn't exist (harmless if no files)
        download_dir.mkdir(parents=True, exist_ok=True)
        
        if not download_dir.exists():
            raise HTTPException(
                status_code=500,
                detail=f"Could not create export directory: {download_dir}"
            )
        
        # Check which Excel files exist
        existing_files = []
        missing_files = []
        for tab in TABS:
            excel_path = download_dir / f"{tab}.xlsx"
            if excel_path.exists():
                file_size = excel_path.stat().st_size
                existing_files.append({"tab": tab, "file": str(excel_path), "size_bytes": file_size})
            else:
                missing_files.append(tab)
        
        if not existing_files:
            error_msg = (
                f"No Excel files found in {download_dir}. "
                f"Expected files: {', '.join([f'{tab}.xlsx' for tab in TABS[:3]])}... "
                f"\n\nTo generate files:\n"
                f"1. Wait for scheduled login/export (every 4 hours)\n"
                f"2. Or trigger full export: POST /export-dashboard\n"
                f"3. Files must be generated before upload."
            )
            raise HTTPException(
                status_code=404,
                detail=error_msg
            )
        
        # Log missing files as warnings (if some files exist, continue with available ones)
        if missing_files:
            logger.warning(f"⚠️ Missing Excel files for {len(missing_files)} tab(s): {', '.join(missing_files)}")
            logger.info(f"📁 Found {len(existing_files)} Excel file(s) - will upload available files only")
        
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
        
        # First, get spreadsheet metadata to see existing sheets
        spreadsheet = service.spreadsheets().get(spreadsheetId=GOOGLE_SHEET_ID).execute()
        sheets = spreadsheet.get('sheets', [])
        
        sheet_name = "TestConnection"
        sheet_id = None
        
        # Check if sheet exists
        for sheet in sheets:
            if sheet['properties']['title'] == sheet_name:
                sheet_id = sheet['properties']['sheetId']
                break
        
        # Create sheet if it doesn't exist
        if sheet_id is None:
            logger.info(f"Creating new sheet: {sheet_name}")
            add_sheet_request = {
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            }
            batch_update = service.spreadsheets().batchUpdate(
                spreadsheetId=GOOGLE_SHEET_ID,
                body={'requests': [add_sheet_request]}
            ).execute()
            sheet_id = batch_update['replies'][0]['addSheet']['properties']['sheetId']
            logger.info(f"✅ Created sheet {sheet_name} (ID: {sheet_id})")
        
        # Write test row to the sheet
        range_name = f"{sheet_name}!A1:B1"
        result = service.spreadsheets().values().append(
            spreadsheetId=GOOGLE_SHEET_ID,
            range=range_name,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": [test_row]},
        ).execute()
        
        logger.info(f"✅ Test row written to {GOOGLE_SHEET_ID} / {sheet_name}")
        return JSONResponse(content={
            "ok": True,
            "message": "Write test successful",
            "spreadsheet_id": GOOGLE_SHEET_ID,
            "sheet": sheet_name,
            "updated_cells": result.get('updates', {}).get('updatedCells', 0)
        })
    except Exception as e:
        logger.error(f"Test Sheets error: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Test failed: {e}")


@app.get("/health")
async def health():
    return {"ok": True, "timestamp": datetime.utcnow().isoformat()}
