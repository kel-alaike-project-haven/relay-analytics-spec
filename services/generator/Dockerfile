FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PORT=8080

WORKDIR /app

# 1) Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Copy code + configs + data contracts
COPY app ./app
COPY main.py .
COPY configs ./configs
COPY data_contracts ./data_contracts

# 3) (Optional) create a non-root user
RUN useradd -m -u 10001 runner
USER runner

EXPOSE 8080

# 4) Start the API
# Use the module that DEFINES your FastAPI() instance:
# - If app/__init__.py exposes `app`, keep "app:app".
# - If main.py exposes `app`, change to ["uvicorn","main:app",...]
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8080"]

