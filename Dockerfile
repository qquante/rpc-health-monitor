FROM python:3.12-slim

# nao gera .pyc e loga sem buffer
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY monitor.py dashboard.py auditoria.py ./

CMD ["python", "-u", "monitor.py"]
