-- ════════════════════════════════════════════════════
--  Seed Gold Data — News Big Data Platform
-- ════════════════════════════════════════════════════

-- Articles par jour (30 derniers jours × 3 sources)
DELETE FROM gold.articles_par_jour;
INSERT INTO gold.articles_par_jour (pub_date, source, nb_articles, avg_content_length, nb_categories, computed_at) VALUES
('2026-05-10','hespress',42,1820,5,NOW()),('2026-05-10','bbc_news',38,2100,5,NOW()),('2026-05-10','aljazeera',31,1950,4,NOW()),
('2026-05-09','hespress',55,1750,5,NOW()),('2026-05-09','bbc_news',44,2050,5,NOW()),('2026-05-09','aljazeera',36,1880,4,NOW()),
('2026-05-08','hespress',48,1900,5,NOW()),('2026-05-08','bbc_news',41,2200,5,NOW()),('2026-05-08','aljazeera',29,1700,4,NOW()),
('2026-05-07','hespress',61,1650,5,NOW()),('2026-05-07','bbc_news',52,1980,5,NOW()),('2026-05-07','aljazeera',40,2100,4,NOW()),
('2026-05-06','hespress',35,1800,5,NOW()),('2026-05-06','bbc_news',33,2300,5,NOW()),('2026-05-06','aljazeera',25,1600,4,NOW()),
('2026-05-05','hespress',28,1700,5,NOW()),('2026-05-05','bbc_news',30,2150,5,NOW()),('2026-05-05','aljazeera',22,1850,4,NOW()),
('2026-05-04','hespress',50,1950,5,NOW()),('2026-05-04','bbc_news',45,2000,5,NOW()),('2026-05-04','aljazeera',35,1900,4,NOW()),
('2026-05-03','hespress',58,1820,5,NOW()),('2026-05-03','bbc_news',49,2080,5,NOW()),('2026-05-03','aljazeera',38,1780,4,NOW()),
('2026-05-02','hespress',44,1700,5,NOW()),('2026-05-02','bbc_news',40,2200,5,NOW()),('2026-05-02','aljazeera',32,1950,4,NOW()),
('2026-05-01','hespress',52,1880,5,NOW()),('2026-05-01','bbc_news',47,2100,5,NOW()),('2026-05-01','aljazeera',37,1820,4,NOW()),
('2026-04-30','hespress',39,1750,5,NOW()),('2026-04-30','bbc_news',35,1980,5,NOW()),('2026-04-30','aljazeera',28,1700,4,NOW()),
('2026-04-29','hespress',63,1900,5,NOW()),('2026-04-29','bbc_news',55,2250,5,NOW()),('2026-04-29','aljazeera',42,2000,4,NOW()),
('2026-04-28','hespress',47,1820,5,NOW()),('2026-04-28','bbc_news',43,2100,5,NOW()),('2026-04-28','aljazeera',34,1880,4,NOW()),
('2026-04-27','hespress',33,1650,5,NOW()),('2026-04-27','bbc_news',31,2000,5,NOW()),('2026-04-27','aljazeera',24,1750,4,NOW()),
('2026-04-26','hespress',56,1950,5,NOW()),('2026-04-26','bbc_news',50,2150,5,NOW()),('2026-04-26','aljazeera',39,1900,4,NOW());

-- Articles par catégorie
DELETE FROM gold.articles_par_categorie;
INSERT INTO gold.articles_par_categorie (category, source, nb_articles, pct_total, computed_at) VALUES
('politique','hespress',850,12.8,NOW()),
('societe','hespress',720,10.8,NOW()),
('economie','hespress',680,10.2,NOW()),
('sport','hespress',920,13.8,NOW()),
('monde','hespress',540,8.1,NOW()),
('world','bbc_news',780,11.7,NOW()),
('technology','bbc_news',520,7.8,NOW()),
('business','bbc_news',480,7.2,NOW()),
('science','bbc_news',310,4.7,NOW()),
('health','bbc_news',290,4.4,NOW()),
('top_news','aljazeera',620,9.3,NOW()),
('middle_east','aljazeera',480,7.2,NOW()),
('africa','aljazeera',350,5.3,NOW()),
('economy','aljazeera',290,4.4,NOW());

-- Articles par langue
DELETE FROM gold.articles_par_langue;
INSERT INTO gold.articles_par_langue (language, source, nb_articles, computed_at) VALUES
('ar','hespress',3710,NOW()),
('fr','hespress',400,NOW()),
('en','bbc_news',2380,NOW()),
('en','aljazeera',1740,NOW());

-- Top mots clés
DELETE FROM gold.top_mots_cles;
INSERT INTO gold.top_mots_cles (word, source, frequency, computed_at) VALUES
('المغرب','hespress',1180,NOW()),
('الحكومة','hespress',950,NOW()),
('البرلمان','hespress',820,NOW()),
('الاقتصاد','hespress',780,NOW()),
('التعليم','hespress',650,NOW()),
('الصحة','hespress',610,NOW()),
('الرياضة','hespress',890,NOW()),
('الأمن','hespress',540,NOW()),
('التنمية','hespress',480,NOW()),
('الاستثمار','hespress',420,NOW()),
('morocco','bbc_news',870,NOW()),
('government','bbc_news',820,NOW()),
('economy','bbc_news',750,NOW()),
('climate','bbc_news',680,NOW()),
('technology','bbc_news',640,NOW()),
('election','bbc_news',590,NOW()),
('africa','bbc_news',520,NOW()),
('energy','bbc_news',480,NOW()),
('health','bbc_news',440,NOW()),
('security','bbc_news',390,NOW()),
('palestine','aljazeera',920,NOW()),
('israel','aljazeera',880,NOW()),
('ceasefire','aljazeera',760,NOW()),
('africa','aljazeera',640,NOW()),
('economy','aljazeera',580,NOW()),
('refugees','aljazeera',520,NOW()),
('climate','aljazeera',460,NOW()),
('trade','aljazeera',410,NOW()),
('conflict','aljazeera',380,NOW()),
('development','aljazeera',340,NOW());

-- Tendances news
DELETE FROM gold.tendances_news;
INSERT INTO gold.tendances_news (category, source, count_recent, count_avg_week, trend_score, trend_label, computed_at) VALUES
('politique','hespress',48,31,1.55,'🔥 Trending',NOW()),
('sport','hespress',52,38,1.37,'📈 Stable',NOW()),
('economie','hespress',35,28,1.25,'📈 Stable',NOW()),
('societe','hespress',22,24,0.92,'📉 Declining',NOW()),
('monde','hespress',18,20,0.90,'📉 Declining',NOW()),
('world','bbc_news',61,40,1.53,'🔥 Trending',NOW()),
('technology','bbc_news',44,30,1.47,'🔥 Trending',NOW()),
('business','bbc_news',33,28,1.18,'📈 Stable',NOW()),
('health','bbc_news',28,25,1.12,'📈 Stable',NOW()),
('science','bbc_news',19,22,0.86,'📉 Declining',NOW()),
('top_news','aljazeera',55,36,1.53,'🔥 Trending',NOW()),
('middle_east','aljazeera',42,32,1.31,'📈 Stable',NOW()),
('africa','aljazeera',31,27,1.15,'📈 Stable',NOW()),
('economy','aljazeera',24,26,0.92,'📉 Declining',NOW());

SELECT 'Done! Gold data inserted.' AS status;
