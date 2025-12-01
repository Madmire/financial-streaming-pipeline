# 📁 Structure du Projet - Financial Streaming Pipeline

Ce document décrit l'organisation complète du projet Financial Streaming Pipeline et les conventions utilisées.

## 🏗️ Architecture Générale

```
financial-streaming-pipeline/
├── 📂 data-producers/           # 🐍 Producteurs de données Python
├── 📂 flink-jobs/              # ⚡ Jobs de traitement Flink (Scala)
├── 📂 ml-models/               # 🤖 Modèles de Machine Learning
├── 📂 infrastructure/          # 🐳 Configuration Docker
├── 📂 scripts/                 # 📜 Scripts utilitaires
├── 📂 docs/                    # 📚 Documentation
├── 📄 README.md                # 📖 Documentation principale
├── 📄 CONTRIBUTING.md          # 🤝 Guide de contribution
├── 📄 .gitignore              # 🚫 Fichiers ignorés
├── 📄 LICENSE                 # ⚖️ Licence du projet
└── 📄 PROJECT_STRUCTURE.md    # 📋 Ce fichier
```

## 📂 Détail des Dossiers

### 🐍 data-producers/
Producteurs et consommateurs de données financières.

```
data-producers/
├── 📄 market_data_producer.py     # Producteur principal de données
├── 📄 market_data_consumer.py     # Consumer de test
├── 📄 requirements.txt           # Dépendances Python
└── 📂 config/
    └── 📄 kafka_config.yaml      # Configuration Kafka
```

**Conventions**:
- Scripts Python en `snake_case`
- Configuration en YAML
- Un fichier `requirements.txt` par module

### ⚡ flink-jobs/
Jobs de traitement temps réel avec Apache Flink.

```
flink-jobs/
├── 📂 src/main/scala/com/financial/streaming/
│   ├── 📄 MarketDataProcessor.scala      # Processor principal
│   ├── 📄 VolatilityCalculator.scala     # Calcul volatilité
│   ├── 📄 AnomalyDetector.scala          # Détection anomalies
│   ├── 📄 TechnicalIndicators.scala      # Indicateurs techniques
│   └── 📄 UnifiedMarketProcessor.scala   # Processor unifié
├── 📂 trino-config/catalog/              # Configuration Trino
│   ├── 📄 hive.properties
│   ├── 📄 iceberg.properties
│   ├── 📄 memory.properties
│   └── 📄 system.properties
├── 📂 project/
│   ├── 📄 plugins.sbt                    # Plugins SBT
│   └── 📂 project/                       # Configuration SBT
├── 📂 target/                           # ⚠️ Build artifacts (ignoré)
├── 📂 venv/                             # 🐍 Virtual env Python
└── 📄 build.sbt                         # Configuration SBT
```

**Conventions**:
- Classes Scala en `PascalCase`
- Package: `com.financial.streaming`
- Configuration dans `trino-config/`

### 🤖 ml-models/
Modèles de Machine Learning et prédictions temps réel.

```
ml-models/
├── 📄 train_volatility_model.py      # Entraînement modèle volatilité
├── 📄 train_anomaly_model.py         # Entraînement modèle anomalies
├── 📄 predict_realtime.py            # Prédictions temps réel
├── 📄 requirements-ml.txt            # Dépendances ML
├── 📂 models/                        # Modèles sauvegardés
│   ├── 📄 volatility_model.pkl
│   └── 📄 anomaly_model.pkl
└── 📂 notebooks/                     # Notebooks Jupyter (optionnel)
```

**Conventions**:
- Scripts en `snake_case`
- Modèles dans `models/` avec extension `.pkl`
- Dépendances séparées dans `requirements-ml.txt`

### 🐳 infrastructure/
Configuration Docker Compose pour tous les services.

```
infrastructure/
├── 📄 docker-compose.yml              # Stack complet
├── 📄 docker-compose-mlflow.yml       # Stack MLflow seulement
└── 📂 iceberg/
    └── 📄 docker-compose-iceberg.yml  # Stack Iceberg seulement
```

**Conventions**:
- Un fichier par stack logique
- Nommage: `docker-compose-{service}.yml`
- Version 3.8+ pour tous

### 📜 scripts/
Scripts utilitaires pour la gestion du pipeline.

```
scripts/
├── 📄 start-complete-pipeline.sh      # Démarrage complet (Linux/Mac)
├── 📄 create-kafka-topics.ps1         # Création topics Kafka (Windows)
└── 📄 test_kafka_pipeline.sh          # Tests du pipeline
```

**Conventions**:
- Scripts Bash: `.sh`
- Scripts PowerShell: `.ps1`
- Headers avec droits d'exécution
- Commentaires détaillés

### 📚 docs/
Documentation technique et guides.

```
docs/
└── 📂 trino-queries/                  # Requêtes SQL Trino
```

**Conventions**:
- Documentation en Markdown
- Structure par domaine
- Exemples pratiques

## 📋 Conventions de Nommage

### Fichiers et Dossiers
| Type | Convention | Exemple |
|------|------------|---------|
| **Python** | `snake_case.py` | `market_data_producer.py` |
| **Scala** | `PascalCase.scala` | `MarketDataProcessor.scala` |
| **Config** | `snake_case.yaml` | `kafka_config.yaml` |
| **Scripts** | `snake_case.sh` | `start_pipeline.sh` |
| **Dossiers** | `kebab-case/` | `data-producers/` |

### Code
| Élément | Convention | Exemple |
|---------|------------|---------|
| **Variables Python** | `snake_case` | `market_data` |
| **Variables Scala** | `camelCase` | `marketData` |
| **Classes** | `PascalCase` | `VolatilityModel` |
| **Fonctions** | `snake_case` | `calculate_volatility()` |
| **Constants** | `UPPER_SNAKE_CASE` | `KAFKA_BROKERS` |

### Docker Services
| Service | Convention | Exemple |
|---------|------------|---------|
| **Conteneurs** | `kebab-case` | `kafka-broker` |
| **Réseaux** | `snake_case` | `financial_streaming` |
| **Volumes** | `snake_case_data` | `kafka_data` |

## 🏷️ Tags et Labels

### Git Branches
- `main` : Branche principale
- `develop` : Branche de développement
- `feature/*` : Nouvelles fonctionnalités
- `fix/*` : Corrections de bugs
- `docs/*` : Documentation

### Git Commits
Suivre [Conventional Commits](https://conventionalcommits.org/):
- `feat:` : Nouvelle fonctionnalité
- `fix:` : Correction de bug
- `docs:` : Documentation
- `style:` : Formatage
- `refactor:` : Refactorisation
- `test:` : Tests

## 🔧 Outils de Développement

### Python
- **Formatage** : Black
- **Linting** : Flake8
- **Imports** : isort
- **Tests** : pytest

### Scala
- **Formatage** : Scalafmt
- **Build** : SBT
- **Tests** : ScalaTest

### Configuration
- **Pre-commit** : Hooks automatiques
- **CI/CD** : GitHub Actions
- **Docker** : Multi-stage builds

## 📊 Métriques et Qualité

### Code Quality
- **Coverage** : > 80% pour Python
- **Linting** : Score A pour tous les fichiers
- **Tests** : Tous passent en CI
- **Documentation** : Couverture complète

### Performance
- **Latence** : < 100ms processing temps réel
- **Throughput** : > 1000 msg/sec
- **Disponibilité** : 99.9% uptime

### Sécurité
- **Dependencies** : Scanning automatique
- **Secrets** : Gestion centralisée
- **Access** : Principe du moindre privilège

## 🚀 Déploiement

### Environnements
- **Local** : Développement avec Docker
- **Dev** : Environnement de test
- **Staging** : Pré-production
- **Prod** : Production

### Configuration
- Variables d'environnement par environnement
- Secrets dans vaults sécurisés
- Configuration immutable
- Rollbacks automatiques

## 🔄 Workflows

### Développement
1. Créer une branche feature
2. Écrire les tests
3. Implémenter la fonctionnalité
4. Tests passent + linting OK
5. Commit conventionnel
6. Pull Request
7. Revue de code
8. Merge dans main

### Release
1. Version bump automatique
2. Génération changelog
3. Tests d'intégration
4. Déploiement staging
5. Tests manuels
6. Déploiement production
7. Monitoring post-release

## 📈 Monitoring

### Métriques
- **Application** : Latence, throughput, erreurs
- **Infrastructure** : CPU, mémoire, disque
- **Business** : Volume données, prédictions ML

### Alertes
- Seuils configurables
- Escalade automatique
- Runbooks détaillés
- Résolution rapide

---

## 📞 Support

Pour toute question sur la structure du projet :
1. Consulter ce document
2. Vérifier les conventions existantes
3. Ouvrir une issue GitHub
4. Demander à l'équipe

**Dernière mise à jour** : Décembre 2024