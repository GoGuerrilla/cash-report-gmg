FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN pip install playwright
RUN playwright install-deps chromium
RUN playwright install chromium

COPY . .

ENV PLAYWRIGHT_BROWSERS_PATH=/app/ms-playwright

CMD ["python3", "webhook_server.py"]
