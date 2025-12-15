# Dictionnaire de Données

## dim_time
- time_sk (PK): Clé surrogate
- date: Date calendaire
- year, month, week: Dimensions temporelles

## dim_brand
- brand_sk (PK): Clé surrogate
- brand_name: Nom de la marque

## dim_product
- product_sk (PK): Clé surrogate
- code: Code-barres
- product_name: Nom du produit
- is_current: Version actuelle (SCD Type 2)

## fact_nutrition_snapshot
- fact_id (PK): Clé primaire
- product_sk, time_sk (FK): Clés étrangères
- Mesures: energy_kcal_100g, sugars_100g, salt_100g, etc.
- completeness_score: Score de qualité
