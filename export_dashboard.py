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
    creds_json = GOOGLE_CREDENTIALS_JSON
    creds_info = json.loads(creds_json.replace("\\n", "\n")) if "\\n" in creds_json else json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return build("sheets", "v4", credentials=creds)


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

    # Enhanced Export to Excel button clicking with universal strategy
    logger.info(f"🔍 Looking for Export to Excel button for {tab_name}...")
    
    export_selectors = [
        'button:has-text("Export to excel")',
        'button:has-text("Export to Excel")',
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
                    const elements = Array.from(document.querySelectorAll('button, a, [role="button"]'));
                    for (let el of elements) {
                        const text = (el.textContent || '').toLowerCase();
                        const ariaLabel = (el.getAttribute('aria-label') || '').toLowerCase();
                        const title = (el.getAttribute('title') || '').toLowerCase();
                        if (text.includes('export') && text.includes('excel') || 
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
    logger.info(f"✅ Export completed for {tab_name} ({file_path.stat().st_size} bytes)")
    return {"tab": tab_name, "status": "done"}


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
            
            # CRITICAL: Start fresh - don't use storage_state initially to clear any concurrent sessions
            logger.info("🔄 Starting fresh session to clear concurrent logins...")
            context = await browser.new_context(accept_downloads=True)
            page = await context.new_page()
            
            # Navigate to logout/login page to clear any existing sessions
            try:
                logger.info("Clearing any existing concurrent sessions...")
                await page.goto("https://compliancenominationportal.in.pwc.com/Account/LogOff", wait_until="networkidle", timeout=30000)
                await asyncio.sleep(2)
                
                # Check if we're on logout confirmation or login page
                current_url = page.url
                if "LogOff" in current_url or "Login" in current_url or "logout" in current_url.lower():
                    logger.info("✅ Successfully cleared concurrent session")
                    await asyncio.sleep(2)
                else:
                    logger.warning(f"Unexpected page after logout attempt: {current_url}")
            except Exception as clear_err:
                logger.warning(f"Could not clear concurrent session (may not be necessary): {clear_err}")
            
            # Close the temporary context and create a new one with storage_state
            await context.close()
            logger.info("✅ Creating authenticated context with storage_state...")
            
            # Now create context with the storage_state from login
            context = await browser.new_context(storage_state=storage_state, accept_downloads=True)
            page = await context.new_page()
            
            # Wait a moment for context to initialize
            await asyncio.sleep(2)
            
            # CRITICAL: Validate session before proceeding - check for AccessDeniedConcurrent
            logger.info("🔍 Validating session before navigation...")
            await page.goto("https://compliancenominationportal.in.pwc.com/BGVAdmin/BGVDashboard", wait_until="networkidle", timeout=60000)
            await asyncio.sleep(3)
            
            # Check for error page or concurrent access denial
            current_url = page.url
            logger.info(f"📍 Current URL after navigation: {current_url}")
            
            # CRITICAL: Check for AccessDeniedConcurrent - session expired/invalid
            if "AccessDeniedConcurrent" in current_url or "/Login/AccessDeniedConcurrent" in current_url:
                error_screenshot = f"/tmp/session_expired_{datetime.now().strftime('%H%M%S')}.png"
                await page.screenshot(path=error_screenshot, full_page=True)
                await browser.close()
                logger.error(f"❌ Session expired - AccessDeniedConcurrent detected: {current_url}")
                raise HTTPException(
                    status_code=401,
                    detail=f"Session expired — please start new login session via Node.js. URL: {current_url}. Screenshot: {error_screenshot}"
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
