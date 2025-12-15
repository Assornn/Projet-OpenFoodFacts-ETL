-- Requêtes Analytiques OpenFoodFacts

USE openfoodfacts;

-- 1. Top 10 marques par produits
SELECT 
    b.brand_name,
    COUNT(*) AS total_products
FROM dim_product p
JOIN dim_brand b ON p.brand_sk = b.brand_sk
WHERE p.is_current = TRUE
GROUP BY b.brand_name
ORDER BY total_products DESC
LIMIT 10;

-- 2. Distribution Nutri-Score
SELECT 
    nutriscore_grade,
    COUNT(*) AS product_count,
    ROUND(AVG(completeness_score), 4) AS avg_completeness
FROM fact_nutrition_snapshot
WHERE nutriscore_grade IS NOT NULL
GROUP BY nutriscore_grade
ORDER BY nutriscore_grade;

-- 3. Produits avec anomalies (sucres > 80g)
SELECT 
    p.code,
    p.product_name,
    f.sugars_100g,
    f.salt_100g
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
WHERE f.sugars_100g > 80 OR f.salt_100g > 25
LIMIT 20;
