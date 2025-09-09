# Dockerfile — Runs Telegram bot + tiny HTTP server for Render health checks
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/usr/local/bin:${PATH}"

WORKDIR /app

# System deps (kept minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python deps
# Make sure your repo includes a requirements.txt with:
#   python-telegram-bot==20.* 
#   solana==0.30.* 
#   solders>=0.20 
#   base58 
#   python-dotenv
COPY req.txt .
RUN pip install --no-cache-dir -r req.txt

# App files
COPY . .

# Ensure data dir exists for SQLite (path can be overridden by env)
RUN mkdir -p /app/data

# Expose an arbitrary default (Render sets $PORT at runtime)
EXPOSE 10000

# Run the bot AND a tiny HTTP server on $PORT.
# If either process dies, container exits (Render will restart).
CMD ["/bin/sh", "-lc", "set -e; python -u main.py & python -m http.server ${PORT:-10000} & wait -n"]
