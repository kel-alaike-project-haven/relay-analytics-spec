FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY main.py .
COPY data_contracts ./data_contracts

RUN useradd -m -u 10001 runner
USER runner

CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8080"]

