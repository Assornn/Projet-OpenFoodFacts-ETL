-- Création base de données
CREATE DATABASE IF NOT EXISTS openfoodfacts;
USE openfoodfacts;

-- Table de dimension temps
CREATE TABLE dim_time (
    time_sk BIGINT PRIMARY KEY AUTO_INCREMENT,
    date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    month INT NOT NULL,
    week INT NOT NULL
);

-- Table de dimension marque
CREATE TABLE dim_brand (
    brand_sk BIGINT PRIMARY KEY AUTO_INCREMENT,
    brand_name VARCHAR(255) NOT NULL UNIQUE
);

-- Table de dimension produit
CREATE TABLE dim_product (
    product_sk BIGINT PRIMARY KEY AUTO_INCREMENT,
    code VARCHAR(50) NOT NULL,
    product_name VARCHAR(500),
    brand_sk BIGINT,
    effective_from TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (brand_sk) REFERENCES dim_brand(brand_sk)
);

-- Table de faits nutrition
CREATE TABLE fact_nutrition_snapshot (
    fact_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    product_sk BIGINT,
    time_sk BIGINT,
    energy_kcal_100g DECIMAL(10,2),
    sugars_100g DECIMAL(10,2),
    salt_100g DECIMAL(10,2),
    nutriscore_grade CHAR(1),
    completeness_score DECIMAL(5,4),
    FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
    FOREIGN KEY (time_sk) REFERENCES dim_time(time_sk)
);

-- Table de logs ETL
CREATE TABLE etl_log (
    log_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    phase VARCHAR(50),
    status VARCHAR(20),
    records_processed INT
);
