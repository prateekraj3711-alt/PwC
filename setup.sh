#!/bin/bash
echo "Installing dependencies..."
npm install
echo "Installing Playwright Chromium..."
npx playwright install --with-deps chromium
echo "Setup complete! Run 'npm start' to start the server."

