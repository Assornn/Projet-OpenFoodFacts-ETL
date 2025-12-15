# 🗄️ Guide Installation et Configuration MySQL

Ce guide vous permet d'installer MySQL et de tester la phase Gold (chargement datamart).

---

## 📥 Installation MySQL 8.0

### Windows

**Option 1 : MySQL Installer (Recommandé)**

1. Télécharger : https://dev.mysql.com/downloads/installer/
2. Choisir : "mysql-installer-community-8.0.XX.msi"
3. Exécuter l'installeur
4. Choisir "Developer Default"
5. Définir un mot de passe root (noter quelque part !)

**Option 2 : Chocolatey**

```powershell
# Installer Chocolatey si pas déjà fait
# Puis :
choco install mysql
```

### macOS

```bash
# Avec Homebrew
brew install mysql

# Démarrer MySQL
brew services start mysql

# Sécuriser l'installation
mysql_secure_installation
```

### Linux (Ubuntu/Debian)

```bash
sudo apt update
sudo apt install mysql-server

# Démarrer MySQL
sudo systemctl start mysql
sudo systemctl enable mysql

# Sécuriser
sudo mysql_secure_installation
```

---

## ✅ Vérification Installation

```bash
# Vérifier version
mysql --version

# Se connecter
mysql -u root -p
# Entrer votre mot de passe
```

Si vous voyez `mysql>`, c'est bon ! ✅

---

## 🔧 Configuration pour le Projet

### 1. Créer la Base de Données

```sql
-- Se connecter à MySQL
mysql -u root -p

-- Créer la base
CREATE DATABASE openfoodfacts CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Créer un utilisateur dédié (optionnel mais recommandé)
CREATE USER 'etl_user'@'localhost' IDENTIFIED BY 'VotreMotDePasse123!';

-- Donner les droits
GRANT ALL PRIVILEGES ON openfoodfacts.* TO 'etl_user'@'localhost';
FLUSH PRIVILEGES;

-- Quitter
EXIT;
```

### 2. Créer le Schéma Datamart

```bash
# Depuis le dossier du projet
mysql -u root -p openfoodfacts < sql/ddl/create_datamart.sql

# Ou avec l'utilisateur dédié
mysql -u etl_user -p openfoodfacts < sql/ddl/create_datamart.sql
```

### 3. Vérifier les Tables Créées

```sql
mysql -u root -p openfoodfacts

-- Lister les tables
SHOW TABLES;

-- Devrait afficher :
-- dim_time
-- dim_brand
-- dim_category
-- dim_country
-- dim_product
-- fact_nutrition_snapshot
-- etl_log
-- data_quality_issues

-- Voir structure d'une table
DESCRIBE dim_product;

EXIT;
```

---

## ⚙️ Configuration du Projet

### 1. Mettre à Jour config.local.yaml

```yaml
mysql:
  host: "localhost"
  port: 3306
  database: "openfoodfacts"
  user: "root"  # ou "etl_user"
  password: "VotreMot DePasseMySQL"
  jdbc_url: "jdbc:mysql://localhost:3306/openfoodfacts"

spark:
  mysql_jar: "mysql-connector-j-8.0.33.jar"  # Chemin relatif
```

### 2. Télécharger le Connecteur JDBC MySQL

```powershell
# Windows PowerShell
Invoke-WebRequest -Uri "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar" -OutFile "mysql-connector-j-8.0.33.jar"
```

Ou télécharger manuellement :
https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar

**Important** : Le fichier JAR doit être à la racine du projet.

---

## 🚀 Tester la Phase Gold

### 1. Activer Gold dans config.local.yaml

```yaml
pipeline:
  phases:
    - bronze
    - silver
    - gold  # Décommenter
```

### 2. Décommenter Gold dans etl/main.py

```python
# Gold
self.logger.info("\n>>> PHASE GOLD: Chargement")
loader = GoldLoader(self.spark, self.config)
loader.load(df_silver)
self.metrics['gold'] = loader.get_metrics()
self.logger.info(f"Gold OK - {self.metrics['gold'].get('fact_nutrition_loaded', 0)} faits charges")
```

### 3. Lancer l'ETL Complet

```bash
python etl/main.py
```

**Résultat attendu :**

```
>>> PHASE BRONZE: Extraction
Bronze OK - 25 produits extraits

>>> PHASE SILVER: Transformation
Silver OK - 25 produits transformés
Complétude: 36.80%

>>> PHASE GOLD: Chargement
Chargement dim_time...
✓ dim_time chargée: 1 lignes
Chargement dim_brand...
✓ dim_brand chargée: 15 lignes
Chargement dim_category...
✓ dim_category chargée: 12 lignes
Chargement dim_country...
✓ dim_country chargée: 5 lignes
Chargement dim_product...
✓ dim_product chargée: 25 lignes
Chargement fact_nutrition_snapshot...
✓ fact_nutrition_snapshot chargée: 25 lignes
Gold OK - 25 faits charges

>>> ETL TERMINE (Bronze + Silver + Gold)
```

---

## 🔍 Vérifier les Données Chargées

```sql
mysql -u root -p openfoodfacts

-- Compter les enregistrements
SELECT 'dim_time' as table_name, COUNT(*) as count FROM dim_time
UNION ALL
SELECT 'dim_brand', COUNT(*) FROM dim_brand
UNION ALL
SELECT 'dim_category', COUNT(*) FROM dim_category
UNION ALL
SELECT 'dim_country', COUNT(*) FROM dim_country
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'fact_nutrition_snapshot', COUNT(*) FROM fact_nutrition_snapshot;

-- Voir quelques produits
SELECT 
    p.code,
    p.product_name,
    b.brand_name,
    f.nutriscore_grade,
    f.completeness_score
FROM dim_product p
JOIN dim_brand b ON p.brand_sk = b.brand_sk
JOIN fact_nutrition_snapshot f ON p.product_sk = f.product_sk
WHERE p.is_current = TRUE
LIMIT 10;
```

---

## 📊 Tester les Requêtes Analytiques

```bash
# Exécuter toutes les requêtes
mysql -u root -p openfoodfacts < sql/analytics/queries.sql

# Ou une par une dans MySQL
mysql -u root -p openfoodfacts
SOURCE sql/analytics/queries.sql;
```

---

## 🆘 Troubleshooting

### Erreur : "Access denied for user"

```sql
-- Vérifier les utilisateurs
SELECT user, host FROM mysql.user;

-- Réinitialiser le mot de passe
ALTER USER 'root'@'localhost' IDENTIFIED BY 'NouveauMotDePasse';
FLUSH PRIVILEGES;
```

### Erreur : "Can't connect to MySQL server"

```bash
# Vérifier que MySQL est démarré
# Windows
net start MySQL80

# macOS
brew services start mysql

# Linux
sudo systemctl start mysql
```

### Erreur : "Unknown database 'openfoodfacts'"

```sql
-- Créer la base
mysql -u root -p
CREATE DATABASE openfoodfacts;
EXIT;

# Puis recréer le schéma
mysql -u root -p openfoodfacts < sql/ddl/create_datamart.sql
```

### Erreur : "Driver com.mysql.cj.jdbc.Driver not found"

Vérifiez que :
1. Le JAR est téléchargé : `mysql-connector-j-8.0.33.jar`
2. Le chemin dans `config.local.yaml` est correct
3. Le fichier existe à la racine du projet

---

## 🔄 Réinitialiser le Datamart

```sql
-- Se connecter
mysql -u root -p openfoodfacts

-- Supprimer toutes les données (garde la structure)
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE fact_nutrition_snapshot;
TRUNCATE TABLE dim_product;
TRUNCATE TABLE dim_brand;
TRUNCATE TABLE dim_category;
TRUNCATE TABLE dim_country;
TRUNCATE TABLE dim_time;
SET FOREIGN_KEY_CHECKS = 1;

-- Ou tout supprimer et recréer
DROP DATABASE openfoodfacts;
CREATE DATABASE openfoodfacts;
EXIT;

# Recréer le schéma
mysql -u root -p openfoodfacts < sql/ddl/create_datamart.sql
```

---

## 📈 Monitoring Performance

```sql
-- Taille de la base
SELECT 
    table_schema AS 'Database',
    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS 'Size (MB)'
FROM information_schema.tables
WHERE table_schema = 'openfoodfacts'
GROUP BY table_schema;

-- Taille par table
SELECT 
    table_name,
    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'Size (MB)',
    table_rows
FROM information_schema.tables
WHERE table_schema = 'openfoodfacts'
ORDER BY (data_length + index_length) DESC;

-- Voir les index
SHOW INDEX FROM dim_product;
```

---

## ✅ Checklist Finale

- [ ] MySQL 8.0 installé
- [ ] Base `openfoodfacts` créée
- [ ] Schéma datamart créé (toutes les tables)
- [ ] Connecteur JDBC MySQL téléchargé (JAR à la racine)
- [ ] `config.local.yaml` mis à jour (password, mysql_jar)
- [ ] Phase Gold activée dans config et main.py
- [ ] ETL exécuté avec succès (Bronze + Silver + Gold)
- [ ] Données visibles dans MySQL (SELECT * FROM dim_product LIMIT 10)
- [ ] Requêtes analytiques testées

---

**Une fois tout validé, votre projet est 100% complet avec les 3 phases fonctionnelles ! 🎉**