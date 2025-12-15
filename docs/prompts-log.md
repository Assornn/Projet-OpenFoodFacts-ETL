# Journal des Prompts - Projet OpenFoodFacts ETL

## Date : 16 Décembre 2024

### Contexte
Développement d'un pipeline ETL avec Apache Spark pour le module TRDE703.

### Prompts utilisés avec Claude (Anthropic)

#### 1. Demande initiale
**Prompt :** "j'ai ce sujet fais moi tous le code"
**Résultat :** Génération de la structure complète du projet avec :
- Code ETL PySpark (main.py, quality.py)
- Scripts SQL (DDL, requêtes analytiques)
- Documentation (README, architecture, dictionnaire)
- Tests unitaires
- Configuration

#### 2. Setup automatisé
**Prompt :** Discussion sur l'automatisation du setup
**Résultat :** Script Python `setup_project.py` créé pour générer automatiquement toute la structure

#### 3. Configuration Git et sécurité
**Prompt :** Problèmes de configuration Git et sécurité des credentials
**Résultat :** 
- Configuration de `.gitignore`
- Séparation `config.yaml` (public) / `config.local.yaml` (privé)
- Résolution des problèmes SSL/TLS

#### 4. Installation des dépendances
**Prompt :** Erreurs d'installation pip avec certificats SSL
**Résultat :** Utilisation de `--trusted-host` pour contourner les problèmes réseau

### Apprentissages
- Architecture Bronze/Silver/Gold pour l'ETL
- Schéma en étoile pour le datamart
- Bonnes pratiques Git (séparation config publique/privée)
- Gestion de la qualité des données avec règles automatisées

### Temps total
Environ 2 heures pour la mise en place complète du projet.