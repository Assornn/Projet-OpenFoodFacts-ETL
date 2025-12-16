/*
=============================================================================
PROJET ETL OPENFOODFACTS - REQUETES ANALYTIQUES
=============================================================================
Table  : gold_products (Wide Table optimisée pour l'analytique Big Data)
=============================================================================
*/

USE openfoodfacts;

-- 1. TOP 10 MARQUES (Par volume de produits)
-- Adaptation : On utilise la colonne 'brands' directement présente dans la table gold
SELECT 
    brands,
    COUNT(*) AS total_products
FROM gold_products
WHERE brands IS NOT NULL 
  AND brands != ''
GROUP BY brands
ORDER BY total_products DESC
LIMIT 10;


-- 2. DISTRIBUTION NUTRI-SCORE ET QUALITÉ MOYENNE
-- Analyse de la corrélation entre la note nutritionnelle et la complétude de la fiche
SELECT 
    nutriscore_grade,
    COUNT(*) AS product_count,
    ROUND(AVG(completeness_score) * 100, 2) AS avg_completeness_pct
FROM gold_products
WHERE nutriscore_grade IS NOT NULL 
  AND nutriscore_grade != ''
GROUP BY nutriscore_grade
ORDER BY nutriscore_grade;


-- 3. FOCUS SUCRE & SEL (Détection manuelle)
-- Produits très sucrés (>80g) ou très salés (>25g)
SELECT 
    code,
    product_name,
    brands,
    sugars_100g,
    salt_100g
FROM gold_products
WHERE sugars_100g > 80 
   OR salt_100g > 25
ORDER BY sugars_100g DESC
LIMIT 20;


-- 4. ANALYSE DES ANOMALIES (BONUS IQR & RÈGLES)
-- Utilise la colonne 'quality_issues' générée par Spark (String)
-- Permet de voir quels sont les problèmes de qualité les plus fréquents
SELECT 
    'Sucre Hors Bornes (>100g)' as type_anomalie, COUNT(*) as count FROM gold_products WHERE quality_issues LIKE '%sugars_out_of_bounds%'
UNION ALL
SELECT 
    'Energie Aberrante (>9000kcal)', COUNT(*) FROM gold_products WHERE quality_issues LIKE '%energy_out_of_bounds%'
UNION ALL
SELECT 
    'Outlier Statistique (IQR Sucre)', COUNT(*) FROM gold_products WHERE quality_issues LIKE '%sugars_100g_stat_high%'
ORDER BY count DESC;


-- 5. COMPLÉTUDE GLOBALE
-- KPI général pour le rapport
SELECT 
    COUNT(*) as total_produits,
    ROUND(AVG(completeness_score) * 100, 2) as score_completude_moyen
FROM gold_products;