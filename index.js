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
const SESSION_TTL = 5 * 60 * 1000;
const CLEANUP_INTERVAL = 60 * 1000;

const sessions = new Map();
const startTime = Date.now();

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

async function ensureTmpDir() {
  const dir = path.join('/tmp', 'pwc');
  try {
    await fs.mkdir(dir, { recursive: true });
  } catch (err) {
    const dir = path.join(__dirname, 'tmp', 'pwc');
    await fs.mkdir(dir, { recursive: true });
    return dir;
  }
  return dir;
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
      'DELETE /sessions/all': 'Delete all sessions',
      'DELETE /sessions/:session_id': 'Delete specific session',
      'GET /health': 'Health check with uptime'
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

    try {
      await page.fill('input[name="callback_0"]', PWC_EMAIL);
    } catch {
      await page.fill('input[type="email"]', PWC_EMAIL);
    }

    try {
      await page.fill('input[name="callback_1"]', PWC_PASSWORD);
    } catch {
      await page.fill('input[type="password"]', PWC_PASSWORD);
    }

    try {
      await page.click('button[type="submit"]');
    } catch {
      await page.click('input[type="submit"]');
    }

    try {
      await page.waitForTimeout(500);
      try { await page.check('input[type="radio"][value*="email" i]'); } catch {}
      try { await page.click('label:has-text("Email me at")'); } catch {}
      try { await page.click('text=Email me at'); } catch {}
      try { await page.click('text=Email'); } catch {}
      try { await page.click('text=E-mail'); } catch {}
      try { await page.click('button:has-text("Send my code")'); } catch {}
      try { await page.click('button:has-text("Email me a code")'); } catch {}
      try { await page.click('button:has-text("Send code")'); } catch {}
      try { await page.click('button:has-text("Send verification code")'); } catch {}
      try { await page.click('button:has-text("Continue")'); } catch {}
      try { await page.click('button:has-text("Next")'); } catch {}
      try { await page.click('input[type="submit"][value*="Email" i]'); } catch {}
    } catch {}

    let otpSelector = null;
    try {
      await page.waitForSelector('input[name="callback_2"]', { timeout: 30000 });
      otpSelector = 'input[name="callback_2"]';
    } catch {
      try {
        await page.waitForSelector('input[name="otp"]', { timeout: 5000 });
        otpSelector = 'input[name="otp"]';
      } catch {
        try {
          await page.waitForSelector('input[id*="otp"]', { timeout: 5000 });
          otpSelector = 'input[id*="otp"]';
        } catch (err) {
          try {
            await page.waitForSelector('input[placeholder*="verification" i]', { timeout: 5000 });
            otpSelector = 'input[placeholder*="verification" i]';
          } catch (e2) {
            await browser.close();
            return res.status(500).json({
              ok: false,
              error: 'OTP field not found',
              details: { step: 'start-login' }
            });
          }
        }
      }
    }

    const sessionPath = await getSessionPath(sessionId);
    const storageState = await context.storageState();
    await fs.writeFile(sessionPath, JSON.stringify(storageState));

    sessions.set(sessionId, {
      browser,
      context,
      page,
      expiry: Date.now() + SESSION_TTL
    });

    return res.status(200).json({
      ok: true,
      session_id: sessionId,
      message: 'Awaiting OTP'
    });

  } catch (err) {
    if (browser) {
      await browser.close().catch(() => {});
    }
    return res.status(500).json({
      ok: false,
      error: err.message,
      details: { step: 'start-login' }
    });
  }
});

app.post('/complete-login', async (req, res) => {
  let browser = null;
  try {
    const { session_id, otp } = req.body;

    if (!session_id || !otp) {
      return res.status(400).json({
        ok: false,
        error: 'Missing session_id or otp',
        details: { step: 'OTP' }
      });
    }

    let session = sessions.get(session_id);
    
    if (!session) {
      const sessionPath = await getSessionPath(session_id);
      try {
        const storageStateData = await fs.readFile(sessionPath, 'utf-8');
        const storageState = JSON.parse(storageStateData);
        
        browser = await chromium.launch(chromiumLaunchOptions());
        const context = await browser.newContext({ storageState });
        const page = await context.newPage();
        
        await page.goto('https://login.pwc.com/login/?goto=https:%2F%2Flogin.pwc.com:443%2Fopenam%2Foauth2%2Fauthorize%3Fresponse_type%3Dcode%26client_id%3Durn%253Acompliancenominationportal.in.pwc.com%26redirect_uri%3Dhttps%253A%252F%252Fcompliancenominationportal.in.pwc.com%26scope%3Dopenid%26state%3Ddemo&realm=%2Fpwc');
        
        session = { browser, context, page, expiry: Date.now() + SESSION_TTL };
        sessions.set(session_id, session);
      } catch (err) {
        return res.status(400).json({
          ok: false,
          error: 'Session not found',
          details: { step: 'OTP' }
        });
      }
    } else {
      browser = session.browser;
    }

    const { page, context } = session;

    try {
      await page.fill('input[name="callback_2"]', otp);
    } catch {
      try {
        await page.fill('input[name="otp"]', otp);
      } catch {
        await page.fill('input[id*="otp"]', otp);
      }
    }

    try {
      await page.click('button[type="submit"]');
    } catch {
      await page.click('input[type="submit"]');
    }

    await page.waitForLoadState('networkidle', { timeout: 30000 });

    const currentUrl = page.url();
    let loginSuccess = false;

    if (currentUrl.includes('compliancenomination')) {
      try {
        await page.waitForSelector('table', { timeout: 10000 });
        loginSuccess = true;
      } catch {
        try {
          await page.waitForSelector('#dashboard', { timeout: 2000 });
          loginSuccess = true;
        } catch {
          try {
            await page.waitForSelector('text="Background Verification"', { timeout: 2000 });
            loginSuccess = true;
          } catch (err) {
            loginSuccess = false;
          }
        }
      }
    }

    if (!loginSuccess) {
      await browser.close();
      sessions.delete(session_id);
      await fs.unlink(await getSessionPath(session_id)).catch(() => {});
      return res.status(500).json({
        ok: false,
        error: 'Login incomplete',
        details: { step: 'OTP' }
      });
    }

    const cookies = await context.cookies();
    const screenshot = await page.screenshot({ type: 'png' });
    const screenshot_base64 = screenshot.toString('base64');

    await browser.close();
    sessions.delete(session_id);
    await fs.unlink(await getSessionPath(session_id)).catch(() => {});

    return res.status(200).json({
      ok: true,
      message: 'Login complete',
      cookies,
      screenshot_base64
    });

  } catch (err) {
    if (browser) {
      await browser.close().catch(() => {});
    }
    return res.status(500).json({
      ok: false,
      error: err.message,
      details: { step: 'OTP' }
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
    return await r.json();
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

