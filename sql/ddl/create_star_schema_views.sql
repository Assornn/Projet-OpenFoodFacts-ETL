-- Création virtuelle des Dimensions et Faits à partir de la table Gold
USE openfoodfacts;

-- Dimension Produit
CREATE OR REPLACE VIEW dim_product AS
SELECT DISTINCT code, product_name, brands, nutriscore_grade, countries, quality_issues
FROM gold_products;

-- Dimension Temps (Basée sur la date de modif)
CREATE OR REPLACE VIEW dim_time AS
SELECT DISTINCT 
    last_modified_ts,
    YEAR(last_modified_ts) as year,
    MONTH(last_modified_ts) as month,
    DAY(last_modified_ts) as day
FROM gold_products;

-- Table de Faits Nutrition
CREATE OR REPLACE VIEW fact_nutrition AS
SELECT 
    code as product_fk,
    last_modified_ts as time_fk,
    energy_kcal_100g,
    fat_100g,
    sugars_100g,
    proteins_100g,
    salt_100g,
    completeness_score
FROM gold_products;