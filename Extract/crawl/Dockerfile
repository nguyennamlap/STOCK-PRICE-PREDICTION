FROM python:3.11-slim

# Cài dependencies cơ bản
RUN apt-get update && apt-get install -y \
    wget gnupg curl unzip \
    libglib2.0-0 libnss3 libfontconfig1 \
    libxrender1 libxss1 libxtst6 libxi6 libxrandr2 \
    libatk1.0-0 libatk-bridge2.0-0 libcups2 libasound2 libgbm1 \
    && rm -rf /var/lib/apt/lists/*

# Thêm Google Chrome repo & cài Chrome
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub \
    | gpg --dearmor -o /usr/share/keyrings/google-linux-signing-key.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux-signing-key.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
       > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements và cài Python packages
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code
COPY . .