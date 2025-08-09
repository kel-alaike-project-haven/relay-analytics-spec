FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app, configs, and data contracts
COPY app.py .
COPY configs ./configs
COPY data_contracts ./data_contracts

EXPOSE 8080
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8080"]
