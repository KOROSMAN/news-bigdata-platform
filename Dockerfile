# ════════════════════════════════════════════════════
#  Dockerfile — News Scraper Service
#  News Big Data Platform | EMSI IADATA 2025/2026
# ════════════════════════════════════════════════════

FROM python:3.11-slim

# Métadonnées
LABEL maintainer="EMSI IADATA"
LABEL description="News scraper service — Hespress, BBC News, Al Jazeera"
LABEL version="1.0"

# Variables d'environnement
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    MINIO_ENDPOINT=http://minio:9000 \
    MINIO_ACCESS_KEY=minioadmin \
    MINIO_SECRET_KEY=minioadmin \
    KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# Répertoire de travail
WORKDIR /app

# Dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code source
COPY scrapers/     ./scrapers/
COPY medallion/    ./medallion/
COPY quality/      ./quality/
COPY governance/   ./governance/

# Création du dossier de sortie
RUN mkdir -p /app/data/raw

# Healthcheck
HEALTHCHECK --interval=60s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import requests; import feedparser; print('OK')" || exit 1

# Point d'entrée par défaut : mode batch
CMD ["python", "scrapers/run_scrapers.py", "--mode", "batch"]
