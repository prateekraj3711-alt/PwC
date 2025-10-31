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
    } catch (saveErr) {
      return res.status(500).json({
        ok: false,
        error: 'Failed to save session',
        details: { step: 'start-login', error: saveErr.message }
      });
    }

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

    let effectiveSessionId = session_id || latestSessionId;
    
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

    await browser.close();
    sessions.delete(effectiveSessionId);
    await fs.unlink(await getSessionPath(effectiveSessionId)).catch(() => {});

    return res.status(200).json({
      ok: true,
      message: 'Login complete',
      cookies: finalCookies,
      screenshot_base64
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

const tickets = new Map();

function queueJob(run) {
  const id = uuidv4();
  tickets.set(id, { status: 'queued' });
  setImmediate(async () => {
    try {
      const result = await run();
      tickets.set(id, { status: 'done', result });
    } catch (err) {
      tickets.set(id, { status: 'error', error: String(err && err.message ? err.message : err) });
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
  return res.status(200).json({ ok: true, ...t });
});

app.listen(PORT, '0.0.0.0', () => {});

// Scheduler: auto-trigger start-login periodically
let scheduleTimer = null;
function startScheduler() {
  const minutes = Number(process.env.SCHEDULE_INTERVAL_MIN || 105);
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

