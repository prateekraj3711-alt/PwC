const express = require('express');
const { chromium } = require('playwright');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs').promises;
const path = require('path');
require('dotenv').config();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const PWC_EMAIL = process.env.PWC_EMAIL;
const PWC_PASSWORD = process.env.PWC_PASSWORD;
const SESSION_TTL = 15 * 60 * 1000;
const CLEANUP_INTERVAL = 60 * 1000;

const sessions = new Map();
let latestSessionId = null;
const startTime = Date.now();
let cachedTmpDir = null;

async function ensureTmpDir() {
  if (cachedTmpDir) return cachedTmpDir;
  
  const dirsToTry = [
    path.join('/tmp', 'pwc'),
    path.join(__dirname, 'tmp', 'pwc'),
    path.join(process.cwd(), 'tmp', 'pwc')
  ];
  
  for (const dir of dirsToTry) {
    try {
      await fs.mkdir(dir, { recursive: true });
      await fs.access(dir);
      cachedTmpDir = dir;
      return dir;
    } catch (err) {
      continue;
    }
  }
  
  throw new Error('Could not create or access tmp directory');
}

function chromiumLaunchOptions() {
  const o = {
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox']
  };
  if (!process.env.PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS) {
    process.env.PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS = '1';
  }
  try {
    if (!process.env.CHROMIUM_PATH) {
      const { execSync } = require('child_process');
      const found = execSync(
        'command -v chromium || command -v chromium-browser || command -v google-chrome || true',
        { encoding: 'utf8' }
      ).trim();
      if (found) {
        process.env.CHROMIUM_PATH = found;
      }
    }
    if (process.env.CHROMIUM_PATH && process.env.CHROMIUM_PATH.trim() !== '') {
      o.executablePath = process.env.CHROMIUM_PATH.trim();
    }
  } catch (_) {}
  return o;
}

async function tryFill(page, selectors, value) {
  for (const sel of selectors) {
    try { await page.waitForSelector(sel, { timeout: 4000 }); await page.fill(sel, value); return true; } catch (_) {}
  }
  return false;
}

async function tryClick(page, selectors) {
  for (const sel of selectors) {
    try { await page.waitForSelector(sel, { timeout: 4000 }); await page.click(sel); return true; } catch (_) {}
  }
  return false;
}

async function waitForAnySelector(page, selectors, timeoutPer = 5000) {
  for (const sel of selectors) {
    try { await page.waitForSelector(sel, { timeout: timeoutPer }); return sel; } catch (_) {}
  }
  return null;
}

async function findOtpInputInAllFrames(page, totalTimeoutMs = 30000) {
  const otpSelectors = [
    'input[placeholder="One-time verification code"]',
    'input[aria-label="One-time verification code"]',
    'input[autocomplete="one-time-code"]',
    'input[type="text"][inputmode="numeric"]',
    'input[type="tel"]',
    'input[name="callback_2"]',
    'input[name*="otp" i]',
    'input[id*="otp" i]',
    'input[name*="code" i]',
    'input[id*="code" i]',
    'input[aria-label*="verification" i]',
    'input[aria-label*="one-time" i]',
    'input[placeholder*="verification" i]',
    'input[type="text"]',
    'input[type="tel"]'
  ];

  const submitSelectors = [
    'button:has-text("Send my code")',
    'button:has-text("Email me a code")',
    'button:has-text("Send code")',
    'button:has-text("Send verification code")',
    'button:has-text("Submit")',
    'button:has-text("Continue")',
    'button:has-text("Next")',
    'input[type="submit"]',
    'button[type="submit"]'
  ];

  const startTime = Date.now();
  const allFrames = [page, ...page.frames()];
  
  while (Date.now() - startTime < totalTimeoutMs) {
    for (const frame of allFrames) {
      for (const selector of otpSelectors) {
        try {
          const locator = frame.locator(selector).first();
          const isVisible = await locator.isVisible({ timeout: 2000 }).catch(() => false);
          
          if (isVisible) {
            const hasSubmit = await Promise.race(
              submitSelectors.map(s => 
                frame.locator(s).first().isVisible({ timeout: 1000 }).catch(() => false)
              )
            ).catch(() => false);
            
            if (hasSubmit) {
              return { frame, locator };
            }
            
            const hasSubmitAnywhere = await page.locator(submitSelectors.join(', ')).first().isVisible({ timeout: 1000 }).catch(() => false);
            if (hasSubmitAnywhere || frame === page) {
              return { frame, locator };
            }
          }
        } catch (e) {
          continue;
        }
      }
      
      try {
        const labelAnchored = frame.locator('text=/One-time verification code/i').first();
        const labelExists = await labelAnchored.isVisible({ timeout: 1000 }).catch(() => false);
        if (labelExists) {
          const nearbyInput = labelAnchored.locator('..').locator('input').first();
          const inputVisible = await nearbyInput.isVisible({ timeout: 1000 }).catch(() => false);
          if (inputVisible) {
            return { frame, locator: nearbyInput };
          }
        }
      } catch (e) {
        continue;
      }
    }
    
    await page.waitForTimeout(1000);
  }
  
  return null;
}


async function getSessionPath(sessionId) {
  const baseDir = await ensureTmpDir();
  return path.join(baseDir, `${sessionId}.json`);
}

async function cleanupExpiredSessions() {
  const now = Date.now();
  for (const [sessionId, session] of sessions.entries()) {
    if (session.expiry < now) {
      try {
        if (session.browser) {
          await session.browser.close();
        }
        const sessionPath = await getSessionPath(sessionId);
        await fs.unlink(sessionPath).catch(() => {});
        sessions.delete(sessionId);
      } catch (err) {
        sessions.delete(sessionId);
      }
    }
  }
}

setInterval(cleanupExpiredSessions, CLEANUP_INTERVAL);

app.get('/', (req, res) => {
  return res.status(200).json({
    ok: true,
    message: 'PwC Login Automation API',
    endpoints: {
      'POST /start-login': 'Initiate login and wait for OTP',
      'POST /complete-login': 'Complete login with OTP (requires session_id and otp)',
      'POST /zapier-start-login': 'Queue login asynchronously (returns ticket_id)',
      'POST /zapier-complete-login': 'Queue OTP completion asynchronously (returns ticket_id)',
      'GET /status/:ticket_id': 'Check status of async task',
      'GET /fetch-data': 'Fetch data from portal after login (query: ?session_id=...&url=...)',
      'GET /debug/html': 'Get HTML and screenshot of current page (query: ?session_id=...)',
      'DELETE /sessions/all': 'Delete all sessions',
      'DELETE /sessions/:session_id': 'Delete specific session',
      'GET /health': 'Health check with uptime',
      'POST /schedule/start': 'Start background scheduler',
      'POST /schedule/stop': 'Stop background scheduler'
    },
    version: '1.0.0'
  });
});

// Helper function to clean up old session files (older than 5 minutes)
async function cleanupOldSessions() {
  try {
    const baseDir = await ensureTmpDir();
    const files = await fs.readdir(baseDir).catch(() => []);
    const jsonFiles = files.filter(f => f.endsWith('.json'));
    const now = Date.now();
    const maxAge = 5 * 60 * 1000; // 5 minutes
    
    for (const file of jsonFiles) {
      try {
        const filePath = path.join(baseDir, file);
        const stats = await fs.stat(filePath);
        const age = now - stats.mtimeMs;
        
        if (age > maxAge) {
          await fs.unlink(filePath).catch(() => {});
          console.log(`[Cleanup] Deleted old session file: ${file} (age: ${Math.round(age / 1000)}s)`);
        }
      } catch (e) {
        // Ignore errors for individual files
      }
    }
  } catch (err) {
    console.warn(`[Cleanup] Error cleaning old sessions: ${err.message}`);
  }
}

// Helper function to destroy all existing browser contexts
// CRITICAL: This function ensures complete cleanup of all previous sessions
async function destroyAllBrowserContexts() {
  try {
    console.log('[Session Isolation] Starting complete cleanup of all previous sessions...');
    const sessionCount = sessions.size;
    console.log(`[Session Isolation] Found ${sessionCount} active session(s) to destroy`);
    
    // Step 1: Log out from PwC for all active sessions (critical for preventing concurrent sessions)
    const logoutPromises = [];
    for (const [sessionId, session] of sessions.entries()) {
      try {
        if (session.page && !session.page.isClosed()) {
          console.log(`[Session Isolation] Logging out session ${sessionId} from PwC...`);
          // Try to navigate to logout URL for each session
          logoutPromises.push(
            session.page.goto('https://compliancenominationportal.in.pwc.com/Account/LogOff', { timeout: 10000 }).catch(() => {})
          );
        }
      } catch (e) {
        console.warn(`[Session Isolation] Could not logout session ${sessionId}: ${e.message}`);
      }
    }
    
    // Wait for all logout attempts to complete
    if (logoutPromises.length > 0) {
      await Promise.allSettled(logoutPromises);
      await new Promise(resolve => setTimeout(resolve, 3000)); // Wait 3s for logout to process
      console.log(`[Session Isolation] Completed logout attempts for ${logoutPromises.length} session(s)`);
    }
    
    // Step 2: Close all browser instances
    const closePromises = [];
    for (const [sessionId, session] of sessions.entries()) {
      try {
        if (session.browser) {
          console.log(`[Session Isolation] Closing browser for session ${sessionId}...`);
          closePromises.push(session.browser.close().catch(() => {}));
        }
      } catch (e) {
        console.warn(`[Session Isolation] Error closing browser for session ${sessionId}: ${e.message}`);
      }
      sessions.delete(sessionId);
    }
    
    // Wait for all browsers to close
    if (closePromises.length > 0) {
      await Promise.all(closePromises);
      console.log(`[Session Isolation] Closed ${closePromises.length} browser instance(s)`);
    }
    
    // Step 3: Delete all session files from disk
    const baseDir = await ensureTmpDir();
    const files = await fs.readdir(baseDir).catch(() => []);
    const jsonFiles = files.filter(f => f.endsWith('.json'));
    console.log(`[Session Isolation] Found ${jsonFiles.length} session file(s) to delete`);
    
    const deletePromises = [];
    for (const file of jsonFiles) {
      try {
        const filePath = path.join(baseDir, file);
        deletePromises.push(
          fs.unlink(filePath).then(() => {
            console.log(`[Session Isolation] Deleted session file: ${file}`);
          }).catch(() => {})
        );
      } catch (e) {
        // Ignore individual file errors
      }
    }
    
    if (deletePromises.length > 0) {
      await Promise.all(deletePromises);
      console.log(`[Session Isolation] Deleted ${jsonFiles.length} session file(s) from disk`);
    }
    
    // Step 4: Clear in-memory session tracking
    latestSessionId = null;
    
    // Step 5: CRITICAL - Wait for PwC server to recognize all sessions are closed
    // This ensures no concurrent session conflicts when creating new login
    // Increased wait time to ensure PwC processes all logout requests
    await new Promise(resolve => setTimeout(resolve, 15000)); // 15 seconds instead of 10
    
    console.log('[Session Isolation] ✅ Complete cleanup finished. All previous sessions destroyed.');
    console.log(`[Session Isolation] Summary: ${sessionCount} session(s) destroyed, ${jsonFiles.length} file(s) deleted, waited 10s for PwC server recognition`);
  } catch (err) {
    console.error(`[Session Isolation] ❌ Error during cleanup: ${err.message}`);
    console.error(`[Session Isolation] Stack: ${err.stack}`);
    // Still try to clear sessions map even if cleanup fails
    sessions.clear();
    latestSessionId = null;
  }
}

app.post('/start-login', async (req, res) => {
  let browser = null;
  try {
    if (!PWC_EMAIL || !PWC_PASSWORD) {
      return res.status(400).json({
        ok: false,
        error: 'Missing credentials',
        details: { step: 'start-login' }
      });
    }

    // CRITICAL: Destroy ALL previous sessions before creating new one
    // This ensures only one session exists at any time
    console.log('[Start-Login] Initiating complete cleanup of all previous sessions...');
    await destroyAllBrowserContexts();
    
    // Additional cleanup: Remove any session files older than 5 minutes (safety net)
    await cleanupOldSessions();
    
    // Verify cleanup was successful
    if (sessions.size > 0) {
      console.warn(`[Start-Login] ⚠️ Warning: ${sessions.size} session(s) still in memory after cleanup`);
      sessions.clear(); // Force clear
    }
    
    console.log('[Start-Login] Cleanup verified. Creating new login session...');

    const sessionId = uuidv4();
    const stateToken = uuidv4();
    browser = await chromium.launch(chromiumLaunchOptions());
    const context = await browser.newContext();
    const page = await context.newPage();

    await page.goto(`https://login.pwc.com/login/?goto=https:%2F%2Flogin.pwc.com:443%2Fopenam%2Foauth2%2Fauthorize%3Fresponse_type%3Dcode%26client_id%3Durn%253Acompliancenominationportal.in.pwc.com%26redirect_uri%3Dhttps%253A%252F%252Fcompliancenominationportal.in.pwc.com%26scope%3Dopenid%26state%3D${stateToken}&realm=%2Fpwc`);

    const emailFilled = await tryFill(page, [
      'input[name="callback_0"]',
      'input[type="email"]',
      'input[name="email" i]'
    ], PWC_EMAIL);
    if (!emailFilled) {
      await browser.close();
      return res.status(500).json({ ok: false, error: 'Email field not found', details: { step: 'login:email' } });
    }

    // Some flows are username-first; advance to password screen if needed
    await tryClick(page, [
      'button:has-text("Next")',
      'button:has-text("Continue")',
      'input[type="submit"][value*="Next" i]',
      'input[type="submit"][value*="Continue" i]'
    ]);

    const passFilled = await tryFill(page, [
      'input[name="callback_1"]',
      'input[name="IDToken2"]',
      'input#password',
      'input[name="password" i]',
      'input[autocomplete="current-password"]',
      'input[type="password"]'
    ], PWC_PASSWORD);
    if (!passFilled) {
      await browser.close();
      return res.status(500).json({ ok: false, error: 'Password field not found', details: { step: 'login:password' } });
    }

    const submitted = await tryClick(page, [
      'button[type="submit"]',
      'input[type="submit"]',
      'button:has-text("Sign in")',
      'button:has-text("Log in")'
    ]);
    if (!submitted) {
      await browser.close();
      return res.status(500).json({ ok: false, error: 'Submit button not found', details: { step: 'login:submit' } });
    }

    await page.waitForTimeout(3000);

    try {
      await page.waitForSelector('text=Choose one of the following options', { timeout: 20000 });

      const emailOption = page.locator('text=/Email me at/i').first();
      await emailOption.click({ force: true });

      const frames = [page.mainFrame(), ...page.frames()];
      let clicked = false;

      for (const frame of frames) {
        const btnSelectors = [
          'button:has-text("Send my code")',
          'button:has-text("Send code")',
          'input[value*="Send my code" i]',
          'input[value*="Send code" i]'
        ];

        for (const sel of btnSelectors) {
          try {
            const btn = frame.locator(sel).first();
            const count = await btn.count().catch(() => 0);
            if (count > 0) {
              await btn.waitFor({ state: 'attached', timeout: 5000 }).catch(() => {});
              
              for (let i = 0; i < 10; i++) {
                const disabled = await btn.getAttribute('disabled').catch(() => null);
                if (!disabled) {
                  await btn.click({ force: true });
                  clicked = true;
                  break;
                }
                await page.waitForTimeout(1000);
              }
              
              if (clicked) break;
            }
          } catch (e) {
            continue;
          }
        }
        if (clicked) break;
      }

      if (!clicked) {
        const jsClicked = await page.evaluate(() => {
          const btns = Array.from(document.querySelectorAll('button, input[type=submit]'));
          const target = btns.find(b => /send.*code/i.test(b.textContent || b.value || ''));
          if (target) {
            target.click();
            return true;
          }
          return false;
        });
        if (jsClicked) {
          clicked = true;
        }
      }

      if (!clicked) {
        throw new Error('Could not find or click Send my code button in any frame');
      }

      await page.waitForSelector('input[type="text"], input[type="tel"], input[placeholder*="code" i]', { timeout: 30000 });
    } catch (err) {
      const scr = await page.screenshot({ fullPage: true, type: 'png' }).catch(() => null);
      await browser.close();
      return res.status(500).json({
        ok: false,
        error: 'MFA selection failed',
        details: { step: 'mfa:select-email', reason: err.message },
        screenshot_base64: scr ? scr.toString('base64') : undefined
      });
    }

    await page.waitForTimeout(2000);

    const otpInAny = await findOtpInputInAllFrames(page, 30000);
    if (!otpInAny) {
      const pageTextAfter = await page.textContent('body').catch(() => '');
      const stillOnMfa = pageTextAfter.includes('Choose one of the following options') || 
                        pageTextAfter.includes('To verify your identity');
      
      if (stillOnMfa) {
        const scr = await page.screenshot({ fullPage: true, type: 'png' }).catch(() => null);
        await browser.close();
        return res.status(500).json({ 
          ok: false, 
          error: 'Still on MFA page - Send my code not clicked', 
          details: { step: 'mfa:send-code', reason: 'Page did not navigate after clicking Send my code' }, 
          screenshot_base64: scr ? scr.toString('base64') : undefined 
        });
      }
      
      try { await page.waitForSelector('text="Resend Code"', { timeout: 2000 }); } catch (_) {}
      const scr = await page.screenshot({ fullPage: true, type: 'png' }).catch(() => null);
      await browser.close();
      return res.status(500).json({ 
        ok: false, 
        error: 'OTP field not found', 
        details: { step: 'otp:input', reason: 'Input not visible in any frame after 30s' }, 
        screenshot_base64: scr ? scr.toString('base64') : undefined 
      });
    }

    const sessionPath = await getSessionPath(sessionId);
    const storageState = await context.storageState();
    
    try {
      await fs.writeFile(sessionPath, JSON.stringify(storageState), 'utf-8');
      console.log(`[Session] Saved new session to: ${sessionPath}`);
    } catch (saveErr) {
      return res.status(500).json({
        ok: false,
        error: 'Failed to save session',
        details: { step: 'start-login', error: saveErr.message }
      });
    }
    
    // Clean up old sessions again after creating new one
    await cleanupOldSessions();

    sessions.set(sessionId, {
      browser,
      context,
      page,
      expiry: Date.now() + SESSION_TTL
    });
    
    latestSessionId = sessionId;

    return res.status(200).json({
      ok: true,
      session_id: sessionId,
      message: 'Awaiting OTP'
    });

  } catch (err) {
    let screenshot_base64 = undefined;
    if (browser) {
      try {
        const contexts = browser.contexts();
        if (contexts.length > 0) {
          const pages = await contexts[0].pages();
          if (pages.length > 0) {
            const scr = await pages[0].screenshot({ fullPage: true, type: 'png' }).catch(() => null);
            screenshot_base64 = scr ? scr.toString('base64') : undefined;
          }
        }
      } catch (_) {}
      await browser.close().catch(() => {});
    }
    return res.status(500).json({
      ok: false,
      error: err.message,
      details: { step: 'start-login' },
      screenshot_base64
    });
  }
});

app.post('/complete-login', async (req, res) => {
  let browser = null;
  try {
    const { session_id, otp } = req.body;

    if (!otp) {
      return res.status(400).json({
        ok: false,
        error: 'Missing otp',
        details: { step: 'OTP' }
      });
    }

    // CRITICAL: Destroy ALL other existing contexts and session files
    // Keep only the current session being completed
    const currentSessionId = session_id || latestSessionId;
    console.log(`[Complete-Login] Destroying all sessions except ${currentSessionId}...`);
    
    const baseDir = await ensureTmpDir();
    const files = await fs.readdir(baseDir).catch(() => []);
    const jsonFiles = files.filter(f => f.endsWith('.json'));
    
    // Delete all session files EXCEPT the current one
    const deletePromises = [];
    for (const file of jsonFiles) {
      const fileSessionId = file.replace('.json', '');
      if (fileSessionId !== currentSessionId) {
        try {
          const filePath = path.join(baseDir, file);
          deletePromises.push(
            fs.unlink(filePath).then(() => {
              console.log(`[Complete-Login] Deleted old session file: ${file}`);
            }).catch(() => {})
          );
        } catch (e) {}
      }
    }
    await Promise.all(deletePromises);
    
    // Close all browser instances EXCEPT the current one
    for (const [sessionId, session] of sessions.entries()) {
      if (sessionId !== currentSessionId) {
        try {
          if (session.browser) {
            console.log(`[Complete-Login] Closing browser for session ${sessionId}...`);
            await session.browser.close().catch(() => {});
          }
        } catch (e) {
          console.warn(`[Complete-Login] Error closing browser for session ${sessionId}: ${e.message}`);
        }
        sessions.delete(sessionId);
      }
    }
    
    console.log(`[Complete-Login] Cleaned up ${deletePromises.length} old session file(s) and ${sessions.size === 1 ? 0 : sessions.size - 1} old session(s)`);

    let effectiveSessionId = currentSessionId;
    
    if (!effectiveSessionId) {
      const activeSessions = Array.from(sessions.keys());
      return res.status(400).json({
        ok: false,
        error: 'No active session. Please start login first.',
        details: { 
          step: 'OTP',
          latest_session_id: latestSessionId,
          active_sessions_count: activeSessions.length,
          hint: 'The scheduler should automatically create a session. Check if SCHEDULE_ENABLED=true is set.'
        }
      });
    }
    
    let session = sessions.get(effectiveSessionId);
    
    if (!session) {
      let sessionLoaded = false;
      const sessionPathsToTry = [
        await getSessionPath(effectiveSessionId),
        path.join('/tmp', 'pwc', `${effectiveSessionId}.json`),
        path.join(__dirname, 'tmp', 'pwc', `${effectiveSessionId}.json`),
        path.join(process.cwd(), 'tmp', 'pwc', `${effectiveSessionId}.json`)
      ];
      
      for (const sessionPath of sessionPathsToTry) {
        try {
          await fs.access(sessionPath);
          const storageStateData = await fs.readFile(sessionPath, 'utf-8');
          const storageState = JSON.parse(storageStateData);
        
          browser = await chromium.launch(chromiumLaunchOptions());
          const context = await browser.newContext({ storageState });
          const page = await context.newPage();
          
          await page.goto('https://login.pwc.com/login/?goto=https:%2F%2Flogin.pwc.com:443%2Fopenam%2Foauth2%2Fauthorize%3Fresponse_type%3Dcode%26client_id%3Durn%253Acompliancenominationportal.in.pwc.com%26redirect_uri%3Dhttps%253A%252F%252Fcompliancenominationportal.in.pwc.com%26scope%3Dopenid%26state%3Ddemo&realm=%2Fpwc');
          
          await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});
          await page.waitForTimeout(3000);
          
          try {
            await findOtpInputInAllFrames(page, 10000);
          } catch (e) {
            await page.waitForSelector('input[type="text"], input[type="tel"], input[placeholder*="code" i]', { timeout: 5000 }).catch(() => {});
          }
          
          session = { browser, context, page, expiry: Date.now() + SESSION_TTL };
          sessions.set(effectiveSessionId, session);
          sessionLoaded = true;
          break;
        } catch (err) {
          continue;
        }
      }
      
      if (!sessionLoaded) {
        const baseDir = await ensureTmpDir();
        const sessionFiles = await fs.readdir(baseDir).catch(() => []);
        const jsonFiles = sessionFiles.filter(f => f.endsWith('.json'));
        
        if (jsonFiles.length > 0) {
          for (const file of jsonFiles) {
            const fileSessionId = file.replace('.json', '');
            try {
              const filePath = path.join(baseDir, file);
              const storageStateData = await fs.readFile(filePath, 'utf-8');
              const storageState = JSON.parse(storageStateData);
              
              browser = await chromium.launch(chromiumLaunchOptions());
              const context = await browser.newContext({ storageState });
              const page = await context.newPage();
              
              await page.goto('https://login.pwc.com/login/?goto=https:%2F%2Flogin.pwc.com:443%2Fopenam%2Foauth2%2Fauthorize%3Fresponse_type%3Dcode%26client_id%3Durn%253Acompliancenominationportal.in.pwc.com%26redirect_uri%3Dhttps%253A%252F%252Fcompliancenominationportal.in.pwc.com%26scope%3Dopenid%26state%3Ddemo&realm=%2Fpwc');
              
              await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});
              await page.waitForTimeout(3000);
              
              try {
                await findOtpInputInAllFrames(page, 10000);
              } catch (e) {
                await page.waitForSelector('input[type="text"], input[type="tel"], input[placeholder*="code" i]', { timeout: 5000 }).catch(() => {});
              }
              
              session = { browser, context, page, expiry: Date.now() + SESSION_TTL };
              sessions.set(fileSessionId, session);
              effectiveSessionId = fileSessionId;
              sessionLoaded = true;
              break;
            } catch (e) {
              continue;
            }
          }
        }
        
        if (!sessionLoaded) {
          const activeSessions = Array.from(sessions.keys());
          return res.status(400).json({
            ok: false,
            error: 'Session not found',
            details: { 
              step: 'OTP',
              requested_session_id: session_id || 'none',
              effective_session_id: effectiveSessionId,
              latest_session_id: latestSessionId,
              active_sessions: activeSessions,
              session_files: jsonFiles,
              session_files_count: jsonFiles.length,
              hint: 'Tried multiple paths to load session file but failed. Session may have expired (15 min TTL) or file may not exist.'
            }
          });
        }
      }
    } else {
      browser = session.browser;
      if (session.expiry < Date.now()) {
        sessions.delete(effectiveSessionId);
        return res.status(400).json({
          ok: false,
          error: 'Session expired',
          details: { 
            step: 'OTP',
            session_id: effectiveSessionId,
            expiry: new Date(session.expiry).toISOString(),
            hint: 'Session TTL is 15 minutes. Please wait for the next scheduled login.'
          }
        });
      }
      
      let pageClosed = false;
      try {
        pageClosed = !session.page || session.page.isClosed();
      } catch (e) {
        pageClosed = true;
      }
      
      if (pageClosed) {
        const sessionPathsToTry = [
          await getSessionPath(effectiveSessionId),
          path.join('/tmp', 'pwc', `${effectiveSessionId}.json`),
          path.join(__dirname, 'tmp', 'pwc', `${effectiveSessionId}.json`),
          path.join(process.cwd(), 'tmp', 'pwc', `${effectiveSessionId}.json`)
        ];
        
        let restored = false;
        for (const sessionPath of sessionPathsToTry) {
          try {
            await fs.access(sessionPath);
            const storageStateData = await fs.readFile(sessionPath, 'utf-8');
            const storageState = JSON.parse(storageStateData);
            
            if (session.browser) {
              await session.browser.close().catch(() => {});
            }
            
            browser = await chromium.launch(chromiumLaunchOptions());
            const context = await browser.newContext({ storageState });
            const page = await context.newPage();
            
            await page.goto('https://login.pwc.com/login/?goto=https:%2F%2Flogin.pwc.com:443%2Fopenam%2Foauth2%2Fauthorize%3Fresponse_type%3Dcode%26client_id%3Durn%253Acompliancenominationportal.in.pwc.com%26redirect_uri%3Dhttps%253A%252F%252Fcompliancenominationportal.in.pwc.com%26scope%3Dopenid%26state%3Ddemo&realm=%2Fpwc');
            
            await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});
            await page.waitForTimeout(3000);
            
            const currentUrlAfterRestore = page.url();
            const pageText = await page.textContent('body').catch(() => '');
            
            if (!pageText.toLowerCase().includes('verification code') && !pageText.toLowerCase().includes('one-time')) {
              try {
                const otpInput = await findOtpInputInAllFrames(page, 10000);
                if (!otpInput) {
                  await page.waitForSelector('input[type="text"], input[type="tel"], input[placeholder*="code" i]', { timeout: 5000 }).catch(() => {});
                }
              } catch (e) {}
            }
            
            session = { browser, context, page, expiry: Date.now() + SESSION_TTL };
            sessions.set(effectiveSessionId, session);
            restored = true;
            break;
          } catch (restoreErr) {
            continue;
          }
        }
        
        if (!restored) {
          sessions.delete(effectiveSessionId);
          return res.status(400).json({
            ok: false,
            error: 'Session page closed and cannot be restored',
            details: { 
              step: 'OTP',
              session_id: effectiveSessionId,
              hint: 'Tried multiple paths to restore session but failed. Session may need to be recreated. Wait for next scheduled login.'
            }
          });
        }
      }
    }

    const { page, context } = session;

    await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {});
    await page.waitForTimeout(2000);

    let found = null;
    try {
      found = await findOtpInputInAllFrames(page, 30000);
      if (!found) {
        const scr = await page.screenshot({ fullPage: true, type: 'png' }).catch(() => null);
        return res.status(500).json({ 
          ok: false, 
          error: 'OTP field not found', 
          details: { step: 'otp:input', reason: 'Input not visible in any frame after 30s' }, 
          screenshot_base64: scr ? scr.toString('base64') : undefined 
        });
      }
      
      await found.locator.fill(otp);
      
      const submitSelectors = [
        'button:has-text("Submit")',
        'button:has-text("Continue")',
        'button:has-text("Verify")',
        'button[type="submit"]',
        'input[type="submit"]',
        'button:has-text("Send my code")',
        'button:has-text("Email me a code")'
      ];
      
      let submitted = false;
      for (const sel of submitSelectors) {
        try {
          const submitBtn = found.frame.locator(sel).first();
          if (await submitBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
            await submitBtn.click();
            submitted = true;
            break;
          }
        } catch (_) {}
      }
      
      if (!submitted) {
        for (const sel of submitSelectors) {
          try {
            const submitBtn = page.locator(sel).first();
            if (await submitBtn.isVisible({ timeout: 2000 }).catch(() => false)) {
              await submitBtn.click();
              submitted = true;
              break;
            }
          } catch (_) {}
        }
      }
      
      if (!submitted) {
        const scr = await page.screenshot({ fullPage: true, type: 'png' }).catch(() => null);
        return res.status(500).json({ 
          ok: false, 
          error: 'Submit button not found', 
          details: { step: 'otp:submit' }, 
          screenshot_base64: scr ? scr.toString('base64') : undefined 
        });
      }
    } catch (e) {
      const scr = await page.screenshot({ fullPage: true, type: 'png' }).catch(() => null);
      return res.status(500).json({ 
        ok: false, 
        error: 'OTP entry failed', 
        details: { step: 'otp:input', reason: String(e && e.message ? e.message : e) }, 
        screenshot_base64: scr ? scr.toString('base64') : undefined 
      });
    }

    await page.waitForLoadState('networkidle', { timeout: 30000 });
    await page.waitForTimeout(2000);

    const currentUrl = page.url();
    const pageTitle = await page.title().catch(() => '');
    const cookies = await context.cookies();
    
    let loginSuccess = false;
    const successIndicators = [];

    if (currentUrl.includes('compliancenomination') || currentUrl.includes('pwc.com')) {
      successIndicators.push('URL matches PwC domain');
    }

    const selectorsToTry = [
      { sel: 'table', name: 'table element' },
      { sel: '#dashboard', name: 'dashboard element' },
      { sel: 'text="Background Verification"', name: 'Background Verification text' },
      { sel: 'body', name: 'page body' }
    ];

    for (const { sel, name } of selectorsToTry) {
      try {
        await page.waitForSelector(sel, { timeout: 3000 });
        successIndicators.push(`Found ${name}`);
        if (sel !== 'body') {
          loginSuccess = true;
          break;
        }
      } catch (e) {
        continue;
      }
    }

    if (!loginSuccess && cookies.length > 0) {
      const hasAuthCookie = cookies.some(c => 
        c.name.includes('session') || 
        c.name.includes('token') || 
        c.name.includes('auth') ||
        c.domain.includes('pwc.com')
      );
      if (hasAuthCookie) {
        loginSuccess = true;
        successIndicators.push('Auth cookies present');
      }
    }

    if (!loginSuccess && (currentUrl.includes('pwc.com') || currentUrl.includes('compliancenomination'))) {
      const bodyText = await page.textContent('body').catch(() => '');
      if (bodyText && bodyText.length > 100 && !bodyText.toLowerCase().includes('sign in') && !bodyText.toLowerCase().includes('login')) {
        loginSuccess = true;
        successIndicators.push('Page content suggests logged-in state');
      }
    }

    if (!loginSuccess) {
      const scr = await page.screenshot({ fullPage: true, type: 'png' }).catch(() => null);
      await browser.close();
      sessions.delete(effectiveSessionId);
      await fs.unlink(await getSessionPath(effectiveSessionId)).catch(() => {});
      return res.status(500).json({
        ok: false,
        error: 'Login incomplete',
        details: { 
          step: 'login:success-check',
          url: currentUrl,
          title: pageTitle,
          cookies_count: cookies.length,
          checked_indicators: successIndicators
        },
        screenshot_base64: scr ? scr.toString('base64') : undefined
      });
    }

    const finalCookies = await context.cookies();
    const screenshot = await page.screenshot({ type: 'png' });
    const screenshot_base64 = screenshot.toString('base64');

    // CRITICAL: Save storage_state FIRST (while session is still valid)
    // Then close browser WITHOUT logging out (logout invalidates cookies)
    // The storage_state contains valid cookies that Python can reuse
    const sessionPath = await getSessionPath(effectiveSessionId);
    const storageState = await context.storageState();
    try {
      await fs.writeFile(sessionPath, JSON.stringify(storageState), 'utf-8');
      console.log(`[Session] ✅ Saved complete login session to: ${sessionPath} (${storageState?.cookies?.length || 0} cookies)`);
    } catch (saveErr) {
      console.error(`[Session] ❌ Failed to save session: ${saveErr.message}`);
    }
    
    // CRITICAL: Close browser WITHOUT logging out
    // Why: Logging out invalidates cookies in storage_state
    // Instead: Close browser (ends Node.js session), keep storage_state with valid cookies
    // Python will use storage_state to create NEW browser context (new session, same auth)
    // The 90-second wait ensures PwC recognizes Node.js browser closure before Python starts
    console.log(`[Session Isolation] Closing Node.js browser context (NOT logging out - preserving cookies for Python)...`);
    await browser.close().catch(() => {});
    sessions.delete(effectiveSessionId);
    console.log(`[Session Isolation] ✅ Browser closed, session removed from memory`);
    console.log(`[Session Isolation] Storage_state preserved with valid cookies - Python can recreate session`);
    
    // Clean up old session files (older than 5 minutes) after successful login
    await cleanupOldSessions();

    // Store session metadata (without browser) for tracking
    // Python will use the storage_state file to recreate the session
    console.log(`[Session] Session ${effectiveSessionId} saved and browser closed. Python can now use it without conflicts.`);

    const exportServiceUrl = process.env.EXPORT_SERVICE_URL || 'http://localhost:8000';
    
    // CRITICAL: Wait 180 seconds (3 minutes) to ensure:
    // 1. Session file is fully written
    // 2. Node.js browser context is closed (terminates session on PwC server)
    // 3. PwC server has sufficient time to fully recognize browser closure and session termination
    // 4. Python can safely create new context using storage_state cookies without AccessDeniedConcurrent
    // Note: We DON'T logout (which would invalidate cookies) - we just close browser
    // The extended wait ensures PwC's session tracking system recognizes the closure
    console.log(`[Auto-Export] Waiting 180 seconds (3 minutes) before triggering export (to ensure PwC fully recognizes browser closure)...`);
    setTimeout(async () => {
      try {
        // Read the session storage state to send it directly
        let storageStateData = null;
        let storageStateObj = null;
        try {
          const sessionPath = await getSessionPath(effectiveSessionId);
          storageStateData = await fs.readFile(sessionPath, 'utf-8');
          
          // CRITICAL: Parse JSON string to object (not send as string)
          if (storageStateData) {
            storageStateObj = JSON.parse(storageStateData);
            console.log(`[Auto-Export] Loaded storage_state with ${storageStateObj?.cookies?.length || 0} cookies`);
            
            // Validate it's an object with expected structure
            if (!storageStateObj || typeof storageStateObj !== 'object' || Array.isArray(storageStateObj)) {
              throw new Error('storage_state is not a valid object');
            }
          }
        } catch (readErr) {
          console.warn(`[Auto-Export] Could not read/parse session file: ${readErr.message}`);
          return; // Don't proceed if we can't read the session
        }
        
        // Build request body - storage_state MUST be an object, not a string
        const requestBody = { 
          session_id: effectiveSessionId,
          storage_state: storageStateObj  // Send as object, not string!
        };
        
        // Verify before sending
        if (requestBody.storage_state && typeof requestBody.storage_state === 'string') {
          console.error('[Auto-Export] ERROR: storage_state is a string! Should be an object.');
          throw new Error('storage_state must be an object, not a string');
        }
        
        console.log(`[Auto-Export] Triggering export for session ${effectiveSessionId}...`);
        console.log(`[Auto-Export] Export service URL: ${exportServiceUrl}/export-dashboard`);
        
        // ROOT FIX: Wake up service first with health check, then call export endpoint
        // Render free tier services can sleep - health check wakes them up faster
        const HEALTH_CHECK_TIMEOUT = 30000; // 30 seconds for health check
        const FETCH_TIMEOUT = 300000; // 5 minutes for export (export takes time)
        const MAX_RETRIES = 3;
        const RETRY_DELAY = 10000; // 10 seconds between retries
        
        let exportSuccess = false;
        let lastError = null;
        
        for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
          try {
            console.log(`[Auto-Export] Attempt ${attempt}/${MAX_RETRIES} to connect to export service...`);
            
            // STEP 1: Wake up service with health check (faster than waiting for export endpoint)
            console.log(`[Auto-Export] 🔔 Waking up service with health check...`);
            try {
              const healthController = new AbortController();
              const healthTimeoutId = setTimeout(() => healthController.abort(), HEALTH_CHECK_TIMEOUT);
              
              const healthResponse = await fetch(`${exportServiceUrl}/health`, {
                method: 'GET',
                signal: healthController.signal
              }).catch(() => null);
              
              clearTimeout(healthTimeoutId);
              
              if (healthResponse && healthResponse.ok) {
                console.log(`[Auto-Export] ✅ Service is awake (health check OK)`);
              } else {
                console.log(`[Auto-Export] ⏳ Service may be waking up...`);
                await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5s after health check
              }
            } catch (healthErr) {
              console.log(`[Auto-Export] ⏳ Health check timeout (service waking up), proceeding anyway...`);
              await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5s
            }
            
            // STEP 2: Call export endpoint (now service should be awake)
            console.log(`[Auto-Export] 📤 Calling export endpoint...`);
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), FETCH_TIMEOUT);
            
            try {
              const exportResponse = await fetch(`${exportServiceUrl}/export-dashboard`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody),
                signal: controller.signal
              });
              clearTimeout(timeoutId);
              
              const exportResult = await exportResponse.json().catch(() => null);
              if (exportResult && exportResult.ok) {
                console.log(`[Auto-Export] ✅ Export completed successfully for session ${effectiveSessionId}`);
                exportSuccess = true;
                break; // Success - exit retry loop
              } else {
                console.warn(`[Auto-Export] ⚠️ Export failed for session ${effectiveSessionId}: ${exportResult?.error || exportResult?.detail || exportResponse?.status || 'Unknown error'}`);
                if (exportResponse && !exportResponse.ok) {
                  console.warn(`[Auto-Export] HTTP Status: ${exportResponse.status} ${exportResponse.statusText}`);
                  
                  // If it's a 5xx error, retry. If it's 4xx, don't retry (likely config issue)
                  if (exportResponse.status >= 500 && attempt < MAX_RETRIES) {
                    console.log(`[Auto-Export] Server error (${exportResponse.status}), will retry in ${RETRY_DELAY/1000}s...`);
                    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
                    continue;
                  }
                }
                lastError = new Error(exportResult?.error || exportResult?.detail || `HTTP ${exportResponse?.status}`);
                break; // Don't retry on 4xx errors
              }
            } catch (fetchErr) {
              clearTimeout(timeoutId);
              lastError = fetchErr;
              
              // Detailed error logging
              if (fetchErr.name === 'AbortError') {
                console.error(`[Auto-Export] ❌ Timeout (${FETCH_TIMEOUT/1000}s) connecting to ${exportServiceUrl} (attempt ${attempt}/${MAX_RETRIES})`);
                if (attempt < MAX_RETRIES) {
                  console.log(`[Auto-Export] ⏳ Retrying in ${RETRY_DELAY/1000}s... (Render service may be waking up)`);
                  await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
                  continue; // Retry
                }
              } else if (fetchErr.code === 'ECONNREFUSED') {
                console.error(`[Auto-Export] ❌ Connection refused to ${exportServiceUrl} (attempt ${attempt}/${MAX_RETRIES})`);
                console.error(`[Auto-Export] 💡 Service may be sleeping (Render free tier) or URL incorrect`);
                if (attempt < MAX_RETRIES) {
                  console.log(`[Auto-Export] ⏳ Retrying in ${RETRY_DELAY/1000}s... (service may be waking up)`);
                  await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
                  continue; // Retry
                }
              } else if (fetchErr.code === 'ENOTFOUND' || fetchErr.code === 'EAI_AGAIN') {
                console.error(`[Auto-Export] ❌ DNS resolution failed for ${exportServiceUrl} (attempt ${attempt}/${MAX_RETRIES})`);
                if (attempt < MAX_RETRIES) {
                  console.log(`[Auto-Export] ⏳ Retrying in ${RETRY_DELAY/1000}s...`);
                  await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
                  continue; // Retry
                }
              } else if (fetchErr.message && fetchErr.message.includes('fetch failed')) {
                console.error(`[Auto-Export] ❌ Network error: ${fetchErr.message} (attempt ${attempt}/${MAX_RETRIES})`);
                console.error(`[Auto-Export] Error code: ${fetchErr.code || 'N/A'}`);
                if (attempt < MAX_RETRIES) {
                  console.log(`[Auto-Export] ⏳ Retrying in ${RETRY_DELAY/1000}s...`);
                  await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
                  continue; // Retry
                }
                console.error(`[Auto-Export] 💡 Ensure EXPORT_SERVICE_URL is set to the Python service's public URL (e.g., https://your-python-service.onrender.com)`);
              } else {
                console.error(`[Auto-Export] ❌ Error: ${fetchErr.message} (attempt ${attempt}/${MAX_RETRIES})`);
                if (attempt < MAX_RETRIES) {
                  console.log(`[Auto-Export] ⏳ Retrying in ${RETRY_DELAY/1000}s...`);
                  await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
                  continue; // Retry
                }
              }
            }
          } catch (retryErr) {
            console.error(`[Auto-Export] ❌ Retry attempt ${attempt} error: ${retryErr.message}`);
            lastError = retryErr;
            if (attempt < MAX_RETRIES) {
              await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
            }
          }
        }
        
        if (!exportSuccess) {
          console.error(`[Auto-Export] ❌ Failed after ${MAX_RETRIES} attempts: ${lastError?.message || 'Unknown error'}`);
          console.error(`[Auto-Export] 💡 Export service may be sleeping. Check: ${exportServiceUrl}/health`);
        }
      } catch (exportErr) {
        console.error(`[Auto-Export] ❌ Unexpected error: ${exportErr.message}`);
        console.error(`[Auto-Export] Stack: ${exportErr.stack}`);
      }
    }, 180000); // Wait 180 seconds (3 minutes) to ensure PwC fully recognizes Node.js browser closure

    return res.status(200).json({
      ok: true,
      message: 'Login complete',
      session_id: effectiveSessionId,
      cookies: finalCookies,
      screenshot_base64
    });

  } catch (err) {
    let screenshot_base64 = undefined;
    // Only close browser if we haven't already closed it after successful login
    if (browser && !sessions.has(effectiveSessionId)) {
      // Browser already closed after login, don't close again
      try {
        const contexts = browser.contexts();
        if (contexts.length > 0) {
          const pages = await contexts[0].pages();
          if (pages.length > 0) {
            const scr = await pages[0].screenshot({ fullPage: true, type: 'png' }).catch(() => null);
            screenshot_base64 = scr ? scr.toString('base64') : undefined;
          }
        }
      } catch (_) {}
      await browser.close().catch(() => {});
    } else if (browser) {
      // Browser still open (error occurred before closing)
      try {
        const contexts = browser.contexts();
        if (contexts.length > 0) {
          const pages = await contexts[0].pages();
          if (pages.length > 0) {
            const scr = await pages[0].screenshot({ fullPage: true, type: 'png' }).catch(() => null);
            screenshot_base64 = scr ? scr.toString('base64') : undefined;
          }
        }
      } catch (_) {}
      await browser.close().catch(() => {});
      if (effectiveSessionId) {
        sessions.delete(effectiveSessionId);
      }
    }
    return res.status(500).json({
      ok: false,
      error: err.message,
      details: { step: 'OTP' },
      screenshot_base64
    });
  }
});

app.delete('/sessions/all', async (req, res) => {
  let deleted = 0;
  let errors = 0;
  
  try {
    for (const [sessionId, session] of sessions.entries()) {
      try {
        if (session.browser) {
          await session.browser.close();
        }
        const sessionPath = await getSessionPath(sessionId);
        await fs.unlink(sessionPath).catch(() => {});
        sessions.delete(sessionId);
        deleted++;
      } catch (err) {
        errors++;
      }
    }

    try {
      const baseDir = await ensureTmpDir();
      const files = await fs.readdir(baseDir);
      for (const file of files) {
        if (file.endsWith('.json')) {
          await fs.unlink(path.join(baseDir, file)).catch(() => {});
        }
      }
    } catch (err) {
    }

    return res.status(200).json({
      ok: true,
      message: 'All sessions deleted',
      deleted,
      errors
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err.message,
      details: { deleted, errors }
    });
  }
});

app.delete('/sessions/:session_id', async (req, res) => {
  const sessionId = req.params.session_id;
  const session = sessions.get(sessionId);

  if (!session) {
    return res.status(404).json({
      ok: false,
      error: 'Session not found',
      details: { session_id: sessionId }
    });
  }

  try {
    if (session.browser) {
      await session.browser.close();
    }
    const sessionPath = await getSessionPath(sessionId);
    await fs.unlink(sessionPath).catch(() => {});
    sessions.delete(sessionId);

    return res.status(200).json({
      ok: true,
      message: 'Session deleted',
      session_id: sessionId
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err.message,
      details: { session_id: sessionId }
    });
  }
});

app.get('/health', (req, res) => {
  return res.status(200).json({
    ok: true,
    uptime: Date.now() - startTime
  });
});

app.get('/session/latest', (req, res) => {
  if (!latestSessionId) return res.status(404).json({ ok: false, error: 'No session yet' });
  return res.status(200).json({ ok: true, session_id: latestSessionId });
});

app.get('/debug/html', async (req, res) => {
  try {
    const sessionId = req.query.session_id || latestSessionId;
    if (!sessionId) {
      return res.status(404).json({ ok: false, error: 'No active session' });
    }
    
    const session = sessions.get(sessionId);
    if (!session || !session.page) {
      return res.status(404).json({ ok: false, error: 'Session not found or page closed' });
    }
    
    const html = await session.page.content();
    const url = session.page.url();
    const screenshot = await session.page.screenshot({ fullPage: true, type: 'png' }).catch(() => null);
    
    return res.status(200).json({
      ok: true,
      url,
      html,
      screenshot_base64: screenshot ? screenshot.toString('base64') : undefined
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err.message,
      details: { step: 'debug' }
    });
  }
});

app.get('/fetch-data', async (req, res) => {
  let browser = null;
  try {
    const sessionId = req.query.session_id || latestSessionId;
    const targetUrl = req.query.url || 'https://compliancenominationportal.in.pwc.com';
    
    if (!sessionId) {
      return res.status(404).json({ ok: false, error: 'No active session' });
    }
    
    let session = sessions.get(sessionId);
    
    let pageClosed = false;
    try {
      pageClosed = !session || !session.page || session.page.isClosed();
    } catch (e) {
      pageClosed = true;
    }
    
    if (pageClosed) {
      const sessionPath = await getSessionPath(sessionId);
      const sessionPathsToTry = [
        sessionPath,
        path.join('/tmp', 'pwc', `${sessionId}.json`),
        path.join(__dirname, 'tmp', 'pwc', `${sessionId}.json`),
        path.join(process.cwd(), 'tmp', 'pwc', `${sessionId}.json`)
      ];
      
      let loaded = false;
      for (const spath of sessionPathsToTry) {
        try {
          await fs.access(spath);
          const storageStateData = await fs.readFile(spath, 'utf-8');
          const storageState = JSON.parse(storageStateData);
          
          browser = await chromium.launch(chromiumLaunchOptions());
          const context = await browser.newContext({ storageState });
          const page = await context.newPage();
          
          session = { browser, context, page, expiry: Date.now() + SESSION_TTL };
          sessions.set(sessionId, session);
          loaded = true;
          break;
        } catch (e) {
          continue;
        }
      }
      
      if (!loaded) {
        return res.status(404).json({ ok: false, error: 'Session not found or expired' });
      }
    } else {
      browser = session.browser;
    }
    
    const { page } = session;
    
    const currentUrl = page.url();
    if (!currentUrl.includes('compliancenominationportal.in.pwc.com')) {
      await page.goto(targetUrl, { waitUntil: 'networkidle', timeout: 30000 });
      await page.waitForTimeout(2000);
    }
    
    await page.waitForLoadState('networkidle', { timeout: 15000 }).catch(() => {});
    await page.waitForTimeout(2000);
    
    const data = await page.evaluate(() => {
      const result = {
        url: window.location.href,
        title: document.title,
        tables: [],
        text_content: [],
        links: [],
        forms: []
      };
      
      const tables = Array.from(document.querySelectorAll('table'));
      tables.forEach((table, idx) => {
        const rows = Array.from(table.querySelectorAll('tr'));
        const tableData = rows.map(row => {
          const cells = Array.from(row.querySelectorAll('th, td'));
          return cells.map(cell => cell.textContent.trim());
        }).filter(row => row.some(cell => cell.length > 0));
        
        if (tableData.length > 0) {
          result.tables.push({
            index: idx,
            headers: tableData[0] || [],
            rows: tableData.slice(1),
            row_count: tableData.length - 1
          });
        }
      });
      
      const headings = Array.from(document.querySelectorAll('h1, h2, h3, h4, h5, h6'));
      result.text_content = headings.map(h => ({
        level: h.tagName.toLowerCase(),
        text: h.textContent.trim()
      })).filter(h => h.text.length > 0);
      
      const links = Array.from(document.querySelectorAll('a[href]'));
      result.links = links.map(a => ({
        text: a.textContent.trim(),
        href: a.href,
        target: a.target || '_self'
      })).filter(l => l.text.length > 0 || l.href.length > 0).slice(0, 50);
      
      const forms = Array.from(document.querySelectorAll('form'));
      result.forms = forms.map((form, idx) => {
        const inputs = Array.from(form.querySelectorAll('input, select, textarea'));
        return {
          index: idx,
          action: form.action || '',
          method: form.method || 'get',
          fields: inputs.map(input => ({
            name: input.name || '',
            type: input.type || input.tagName.toLowerCase(),
            value: input.value || '',
            label: input.labels?.[0]?.textContent?.trim() || ''
          }))
        };
      });
      
      const mainContent = document.querySelector('main, .content, #content, .main-content, [role="main"]') || document.body;
      result.body_text = mainContent.textContent?.trim()?.substring(0, 5000) || '';
      
      return result;
    });
    
    return res.status(200).json({
      ok: true,
      message: 'Data fetched successfully',
      session_id: sessionId,
      data
    });
    
  } catch (err) {
    let screenshot_base64 = undefined;
    if (browser) {
      try {
        const contexts = browser.contexts();
        if (contexts.length > 0) {
          const pages = await contexts[0].pages();
          if (pages.length > 0) {
            const scr = await pages[0].screenshot({ fullPage: true, type: 'png' }).catch(() => null);
            screenshot_base64 = scr ? scr.toString('base64') : undefined;
          }
        }
      } catch (_) {}
    }
    return res.status(500).json({
      ok: false,
      error: err.message,
      details: { step: 'fetch-data' },
      screenshot_base64
    });
  }
});

const tickets = new Map();

function queueJob(run) {
  const id = uuidv4();
  const now = Date.now();
  tickets.set(id, { status: 'queued', timestamp: now });
  setImmediate(async () => {
    try {
      const result = await run();
      tickets.set(id, { status: 'done', result, timestamp: now, completed_at: Date.now() });
    } catch (err) {
      tickets.set(id, { status: 'error', error: String(err && err.message ? err.message : err), timestamp: now, completed_at: Date.now() });
    }
  });
  return id;
}

app.post('/zapier-start-login', async (req, res) => {
  const ticket_id = queueJob(async () => {
    const r = await fetch(`http://localhost:${PORT}/start-login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({})
    });
    const out = await r.json();
    if (out && out.session_id) latestSessionId = out.session_id;
    return out;
  });
  res.status(200).json({ ok: true, message: 'Login queued', ticket_id });
});

app.post('/zapier-complete-login', async (req, res) => {
  const { session_id, otp } = req.body || {};
  const ticket_id = queueJob(async () => {
    const r = await fetch(`http://localhost:${PORT}/complete-login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id, otp })
    });
    return await r.json();
  });
  res.status(200).json({ ok: true, message: 'Completion queued', ticket_id });
});

app.get('/status/:ticket_id', (req, res) => {
  const t = tickets.get(req.params.ticket_id);
  if (!t) return res.status(404).json({ ok: false, error: 'Unknown ticket' });
  
  const operation = t.result?.message?.includes('Awaiting OTP') ? 'start-login' : 
                    t.result?.message?.includes('Login complete') ? 'complete-login' :
                    t.error ? 'error' : 'unknown';
  
  return res.status(200).json({ 
    ok: true, 
    ...t,
    operation 
  });
});

app.get('/tickets/recent', (req, res) => {
  const recent = Array.from(tickets.entries())
    .slice(-10)
    .map(([id, data]) => ({
      ticket_id: id,
      status: data.status,
      operation: data.result?.message?.includes('Awaiting OTP') ? 'start-login' : 
                 data.result?.message?.includes('Login complete') ? 'complete-login' :
                 data.error ? 'error' : 'unknown',
      timestamp: data.timestamp || null
    }))
    .reverse();
  return res.status(200).json({ ok: true, tickets: recent });
});

app.listen(PORT, '0.0.0.0', () => {});

// Scheduler: auto-trigger start-login periodically
let scheduleTimer = null;
function startScheduler() {
  const minutes = Number(process.env.SCHEDULE_INTERVAL_MIN || 240); // Default: 4 hours (240 minutes)
  const ms = Math.max(1, minutes) * 60 * 1000;
  if (scheduleTimer) clearInterval(scheduleTimer);
  scheduleTimer = setInterval(async () => {
    try {
      const r = await fetch(`http://localhost:${PORT}/start-login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({})
      });
      const out = await r.json().catch(() => null);
      if (out && out.session_id) latestSessionId = out.session_id;
    } catch (_) {}
  }, ms);
}

if (String(process.env.SCHEDULE_ENABLED).toLowerCase() === 'true') {
  startScheduler();
}

app.post('/schedule/start', (req, res) => {
  startScheduler();
  res.status(200).json({ ok: true, running: true });
});

app.post('/schedule/stop', (req, res) => {
  if (scheduleTimer) clearInterval(scheduleTimer);
  scheduleTimer = null;
  res.status(200).json({ ok: true, running: false });
});
