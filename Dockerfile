# ===== Base Image =====
FROM python:3.11-slim

# ===== Install System Dependencies for Chromium =====
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl gnupg ca-certificates fonts-liberation \
    libasound2 libatk-bridge2.0-0 libcups2 libdbus-1-3 \
    libdrm2 libgbm1 libgtk-3-0 libnspr4 libnss3 \
    libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 \
    xdg-utils && rm -rf /var/lib/apt/lists/*

# ===== Set Working Directory =====
WORKDIR /app

# ===== Copy Requirements First (for build cache) =====
COPY requirements.txt ./requirements.txt

# ===== Install Python Dependencies =====
RUN pip install --upgrade pip setuptools wheel
RUN pip install --prefer-binary -r requirements.txt

# ===== Install Playwright and Chromium =====
# The deps step ensures Playwright doesn’t fail if Render base image changes
RUN python -m playwright install-deps chromium && python -m playwright install chromium

# ===== Copy Application Code =====
COPY . ./

# ===== Environment Config =====
ENV PORT=8000
EXPOSE 8000

# ===== Health Check =====
HEALTHCHECK CMD curl --fail http://localhost:${PORT}/health || exit 1

# ===== Start FastAPI App =====
CMD ["sh", "-c", "uvicorn export_dashboard:app --host 0.0.0.0 --port ${PORT:-8000}"]
