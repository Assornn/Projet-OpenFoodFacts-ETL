# Note d'Architecture

## Vue d'ensemble
Pipeline ETL pour OpenFoodFacts avec architecture Bronze/Silver/Gold.

## Technologies
- **ETL:** Apache Spark (PySpark 3.5)
- **Datawarehouse:** MySQL 8.0
- **Langage:** Python 3.8+

## Architecture
```
OpenFoodFacts → Bronze → Silver → Gold → MySQL
```

## Modèle de données
Schéma en étoile avec:
- dim_time, dim_brand, dim_product
- fact_nutrition_snapshot

Voir data-dictionary.md pour les détails.
