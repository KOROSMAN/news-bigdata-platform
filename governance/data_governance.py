"""
Gouvernance des données — News Platform
Catalogue de données + Traçabilité (Lineage) + Métadonnées
"""

import json
import logging
import os
from datetime import datetime
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Catalogue de données (Data Catalog)
# ──────────────────────────────────────────────

DATA_CATALOG = {
    "datasets": {
        "bronze.articles_raw": {
            "description": "Articles bruts collectés depuis les scrapers web",
            "layer":       "bronze",
            "format":      "JSON",
            "location":    "s3://bronze/source={source}/year={y}/month={m}/day={d}/",
            "owner":       "data-team",
            "update_freq": "Horaire",
            "fields": {
                "id":           {"type": "string",    "description": "Hash MD5 de l'URL — identifiant unique"},
                "title":        {"type": "string",    "description": "Titre de l'article"},
                "author":       {"type": "string",    "description": "Auteur ou rédaction"},
                "published_at": {"type": "timestamp", "description": "Date de publication (ISO 8601)"},
                "category":     {"type": "string",    "description": "Catégorie éditoriale"},
                "content":      {"type": "string",    "description": "Corps complet de l'article (HTML inclus à ce stade)"},
                "source":       {"type": "string",    "description": "Site source (hespress, bbc_news, aljazeera…)"},
                "url":          {"type": "string",    "description": "URL canonique de l'article"},
                "language":     {"type": "string",    "description": "Langue détectée (ar/en/fr)"},
                "scraped_at":   {"type": "timestamp", "description": "Timestamp de collecte"},
            },
        },

        "silver.articles_clean": {
            "description": "Articles nettoyés, normalisés et dédupliqués",
            "layer":       "silver",
            "format":      "Parquet",
            "location":    "s3://silver/source={source}/pub_year={y}/pub_month={m}/",
            "owner":       "data-team",
            "update_freq": "Horaire (après Bronze)",
            "quality_rules": [
                "Titre non vide (min 5 caractères)",
                "Contenu non vide (min 100 caractères)",
                "URL valide (http/https)",
                "Date parsable et non future",
                "Déduplication par ID",
            ],
            "transformations": [
                "Suppression balises HTML (BeautifulSoup/regex)",
                "Normalisation espaces et caractères de contrôle",
                "Détection langue (heuristique Unicode)",
                "Normalisation date → ISO 8601",
                "Ajout colonnes dérivées: content_length, pub_year, pub_month",
            ],
            "fields": {
                "id":                    {"type": "string",    "pii": False},
                "title":                 {"type": "string",    "pii": False},
                "author":                {"type": "string",    "pii": True,  "note": "Peut contenir un nom de journaliste"},
                "published_at":          {"type": "timestamp", "pii": False},
                "category":              {"type": "string",    "pii": False},
                "content":               {"type": "string",    "pii": False},
                "source":                {"type": "string",    "pii": False},
                "url":                   {"type": "string",    "pii": False},
                "language":              {"type": "string",    "pii": False},
                "content_length":        {"type": "integer",   "pii": False},
                "silver_processed_at":   {"type": "timestamp", "pii": False},
                "pub_date":              {"type": "date",      "pii": False},
                "pub_year":              {"type": "integer",   "pii": False},
                "pub_month":             {"type": "integer",   "pii": False},
            },
        },

        "gold.articles_par_jour": {
            "description": "Volume d'articles par jour et par source",
            "layer":       "gold",
            "format":      "Parquet + PostgreSQL",
            "location":    "s3://gold/articles_par_jour/ | gold.articles_par_jour (PostgreSQL)",
            "owner":       "analytics-team",
            "update_freq": "Horaire (après Silver)",
        },

        "gold.top_mots_cles": {
            "description": "Fréquence des termes significatifs par source",
            "layer":       "gold",
            "format":      "Parquet + PostgreSQL",
            "location":    "s3://gold/top_mots_cles/ | gold.top_mots_cles (PostgreSQL)",
            "owner":       "analytics-team",
            "update_freq": "Horaire",
        },

        "gold.tendances_news": {
            "description": "Score de tendance par catégorie (7j vs 30j)",
            "layer":       "gold",
            "format":      "Parquet + PostgreSQL",
            "location":    "s3://gold/tendances_news/ | gold.tendances_news (PostgreSQL)",
            "owner":       "analytics-team",
            "update_freq": "Horaire",
        },
    }
}


# ──────────────────────────────────────────────
# Traçabilité (Lineage)
# ──────────────────────────────────────────────

class LineageTracker:
    """
    Enregistre les événements du pipeline pour assurer la traçabilité.
    En production: utiliser Apache Atlas ou DataHub.
    """

    def __init__(self, storage_path: str = "/opt/airflow/governance/lineage"):
        self.storage_path = storage_path
        os.makedirs(storage_path, exist_ok=True)

    def record_event(
        self,
        pipeline_step: str,
        input_path:    str,
        output_path:   str,
        records_in:    int,
        records_out:   int,
        status:        str = "SUCCESS",
        metadata:      dict[str, Any] | None = None,
    ) -> dict:
        event = {
            "event_id":     f"{pipeline_step}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            "timestamp":    datetime.utcnow().isoformat(),
            "pipeline_step": pipeline_step,
            "input": {
                "path":    input_path,
                "records": records_in,
            },
            "output": {
                "path":    output_path,
                "records": records_out,
            },
            "status":   status,
            "metadata": metadata or {},
        }

        # Sauvegarde locale
        filename = f"{self.storage_path}/{event['event_id']}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(event, f, indent=2)

        logger.info(
            f"[LINEAGE] {pipeline_step}: "
            f"{records_in} in → {records_out} out | {status}"
        )
        return event

    def get_lineage_for_dataset(self, dataset_path: str) -> list[dict]:
        """Retourne tous les événements impliquant un dataset donné."""
        events = []
        for filename in os.listdir(self.storage_path):
            if not filename.endswith(".json"):
                continue
            with open(f"{self.storage_path}/{filename}", encoding="utf-8") as f:
                event = json.load(f)
            if (dataset_path in event.get("input", {}).get("path", "") or
                    dataset_path in event.get("output", {}).get("path", "")):
                events.append(event)
        return sorted(events, key=lambda e: e["timestamp"])


# ──────────────────────────────────────────────
# Export du catalogue
# ──────────────────────────────────────────────

def export_catalog(output_path: str = "governance/data_catalog.json"):
    """Exporte le catalogue dans un fichier JSON."""
    catalog = {
        "platform":    "News Big Data Platform — EMSI IADATA 2025/2026",
        "generated":   datetime.utcnow().isoformat(),
        "pipeline_layers": ["Bronze (Raw)", "Silver (Cleaned)", "Gold (Analytical)"],
        "sources": ["hespress", "bbc_news", "aljazeera"],
        **DATA_CATALOG,
    }
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)
    logger.info(f"Data catalog exported to: {output_path}")
    return catalog


if __name__ == "__main__":
    export_catalog()
    print("Catalog exported.")

    # Demo lineage
    tracker = LineageTracker(storage_path="/tmp/lineage_demo")
    tracker.record_event(
        pipeline_step="bronze_to_silver",
        input_path="s3://bronze/source=hespress/2025/01/15/10_00.json",
        output_path="s3://silver/source=hespress/pub_year=2025/pub_month=1/",
        records_in=45,
        records_out=42,
        metadata={"filter_reason": "3 articles trop courts"},
    )
