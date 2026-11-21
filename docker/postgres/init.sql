-- ════════════════════════════════════════════════════
--  Initialisation PostgreSQL — News Platform Data Warehouse
-- ════════════════════════════════════════════════════

-- Base pour Metabase
CREATE DATABASE IF NOT EXISTS metabase;

-- Base principale
CREATE DATABASE newsdb;

\connect newsdb;

-- Utilisateur applicatif
CREATE USER newsuser WITH PASSWORD 'newspass';
GRANT ALL PRIVILEGES ON DATABASE newsdb TO newsuser;

-- Schéma Gold (tables analytiques)
CREATE SCHEMA IF NOT EXISTS gold;
GRANT ALL ON SCHEMA gold TO newsuser;

-- ── Table: Articles par jour ─────────────────────
CREATE TABLE IF NOT EXISTS gold.articles_par_jour (
    pub_date            DATE NOT NULL,
    source              VARCHAR(50) NOT NULL,
    nb_articles         INTEGER NOT NULL DEFAULT 0,
    avg_content_length  FLOAT,
    nb_categories       INTEGER,
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pub_date, source)
);

-- ── Table: Articles par catégorie ───────────────
CREATE TABLE IF NOT EXISTS gold.articles_par_categorie (
    category            VARCHAR(100) NOT NULL,
    source              VARCHAR(50) NOT NULL,
    nb_articles         INTEGER NOT NULL DEFAULT 0,
    pct_total           FLOAT,
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category, source)
);

-- ── Table: Articles par langue ───────────────────
CREATE TABLE IF NOT EXISTS gold.articles_par_langue (
    language            VARCHAR(10) NOT NULL,
    source              VARCHAR(50) NOT NULL,
    nb_articles         INTEGER NOT NULL DEFAULT 0,
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (language, source)
);

-- ── Table: Top mots-clés ─────────────────────────
CREATE TABLE IF NOT EXISTS gold.top_mots_cles (
    word                VARCHAR(200) NOT NULL,
    source              VARCHAR(50) NOT NULL,
    frequency           INTEGER NOT NULL DEFAULT 0,
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (word, source)
);

-- ── Table: Tendances ─────────────────────────────
CREATE TABLE IF NOT EXISTS gold.tendances_news (
    category            VARCHAR(100) NOT NULL,
    source              VARCHAR(50) NOT NULL,
    count_recent        INTEGER,
    count_avg_week      FLOAT,
    trend_score         FLOAT,
    trend_label         VARCHAR(50),
    computed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category, source)
);

-- ── Index pour les requêtes fréquentes ───────────
CREATE INDEX IF NOT EXISTS idx_articles_jour_date  ON gold.articles_par_jour (pub_date);
CREATE INDEX IF NOT EXISTS idx_articles_jour_src   ON gold.articles_par_jour (source);
CREATE INDEX IF NOT EXISTS idx_mots_cles_freq      ON gold.top_mots_cles (frequency DESC);
CREATE INDEX IF NOT EXISTS idx_tendances_score     ON gold.tendances_news (trend_score DESC);

-- ── Vue: Dashboard résumé ────────────────────────
CREATE OR REPLACE VIEW gold.v_dashboard_summary AS
SELECT
    (SELECT SUM(nb_articles) FROM gold.articles_par_jour
     WHERE pub_date >= CURRENT_DATE - 7)                AS articles_last_7_days,
    (SELECT COUNT(DISTINCT source) FROM gold.articles_par_jour)  AS nb_sources_actives,
    (SELECT COUNT(DISTINCT category) FROM gold.articles_par_categorie) AS nb_categories,
    (SELECT category FROM gold.tendances_news ORDER BY trend_score DESC LIMIT 1) AS top_trending_category,
    CURRENT_TIMESTAMP                                   AS refreshed_at;

GRANT ALL ON ALL TABLES IN SCHEMA gold TO newsuser;
GRANT ALL ON ALL SEQUENCES IN SCHEMA gold TO newsuser;

SELECT 'Database initialized successfully' AS status;
