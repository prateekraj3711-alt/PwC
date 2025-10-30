# PwC Login Automation Backend

Replit-ready Node.js + Express + Playwright backend for PwC portal login automation with OTP, designed for Zapier integration.

## Setup Instructions for Replit

### 1. Add Environment Variables in Replit Secrets

Navigate to the Secrets tab (lock icon) and add:
- `PWC_EMAIL` - Your PwC email
- `PWC_PASSWORD` - Your PwC password

### 2. Run the Project

Click the **Run** button in Replit. The workflow will automatically:
1. Install npm dependencies
2. Install Playwright Chromium with dependencies
3. Start the Express server on port 3000

## API Endpoints

### POST /start-login

Initiates login and waits for OTP.

**Response:**
```json
{
  "ok": true,
  "session_id": "uuid-here",
  "message": "Awaiting OTP"
}
```

### POST /complete-login

Completes login with OTP.

**Request Body:**
```json
{
  "session_id": "uuid-from-start-login",
  "otp": "123456"
}
```

**Response:**
```json
{
  "ok": true,
  "message": "Login complete",
  "cookies": [...],
  "screenshot_base64": "..."
}
```

### DELETE /sessions/:session_id

Deletes a specific session by ID.

**Response:**
```json
{
  "ok": true,
  "message": "Session deleted",
  "session_id": "uuid-here"
}
```

### DELETE /sessions/all

Closes all browsers and deletes all session files.

**Response:**
```json
{
  "ok": true,
  "message": "All sessions deleted",
  "deleted": 3,
  "errors": 0
}
```

### GET /health

Health check endpoint.

**Response:**
```json
{
  "ok": true,
  "uptime": 123456
}
```

## Zapier Integration Flow

1. **Zap1:** Trigger → POST https://your-replit-url.repl.co/start-login → Store session_id
2. Wait for OTP (email/SMS)
3. **Zap2:** Trigger with OTP → POST https://your-replit-url.repl.co/complete-login with session_id and OTP

## Features

- Headless Chromium with Replit-compatible args (--no-sandbox)
- Auto session cleanup every minute (5-minute TTL)
- Manual cleanup endpoint
- Unified JSON response schema
- Proper error handling with HTTP status codes
- Sequential selector fallbacks for robustness

