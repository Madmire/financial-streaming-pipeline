# 🏦 Financial Streaming Pipeline

Un pipeline de données temps réel pour l'analyse des marchés financiers utilisant Apache Kafka, Apache Flink, Trino, MLflow et Superset.

## 📋 Vue d'ensemble

Ce projet implémente une architecture de streaming moderne pour traiter et analyser des données financières en temps réel. Le pipeline ingère des données de marché (actions, forex, crypto), applique des transformations temps réel avec Flink, stocke les données dans Iceberg/Hive via Trino, et fournit des analyses ML avec MLflow et des visualisations avec Superset.

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────┐    ┌─────────────┐
│   Data Sources  │ => │   Kafka     │ => │   Flink     │
│                 │    │  Streaming  │    │  Processing │
└─────────────────┘    └─────────────┘    └─────────────┘
                                                       │
┌─────────────────┐    ┌─────────────┐    ┌─────────────┐
│   Trino Query   │ <= │   Iceberg   │ <= │   Storage   │
│   Engine        │    │   Tables    │    │             │
└─────────────────┘    └─────────────┘    └─────────────┘
                                                       │
┌─────────────────┐    ┌─────────────┐    ┌─────────────┐
│   MLflow        │    │   Models    │    │   Superset  │
│   Training      │ => │   Registry  │ => │   Dashboards│
└─────────────────┘    └─────────────┘    └─────────────┘
```

## 🚀 Démarrage Rapide

### Prérequis
- Docker & Docker Compose
- Python 3.8+
- Git

### 1. Cloner le projet
```bash
git clone https://github.com/sanogomamadou/financial-streaming-pipeline.git
cd financial-streaming-pipeline
```

### 2. Démarrer l'infrastructure complète
```bash
# Démarrer tous les services (Kafka, Flink, Trino, MLflow, Superset)
docker-compose -f infrastructure/docker-compose.yml up -d

# Ou utiliser le script de démarrage
./scripts/start-complete-pipeline.sh
```

### 3. Créer les topics Kafka
```bash
# PowerShell (Windows)
./scripts/create-kafka-topics.ps1

# Ou Linux/Mac
./scripts/create-kafka-topics.sh
```

### 4. Tester le pipeline
```bash
# Terminal 1 - Démarrer les producteurs de données
python data-producers/market_data_producer.py

# Terminal 2 - Consommer et vérifier les données
python data-producers/market_data_consumer.py --duration 30
```

## 📊 Services et Accès

| Service | URL | Description |
|---------|-----|-------------|
| **MLflow UI** | http://localhost:5000 | Suivi des expérimentations ML |
| **Superset** | http://localhost:8088 | Tableaux de bord et visualisations |
| **Trino** | http://localhost:8080 | Query Engine SQL |
| **Jupyter** | http://localhost:8888 | Notebooks ML (token: vide) |
| **MinIO** | http://localhost:9001 | Stockage objets S3 |
| **Kafka UI** | http://localhost:9090 | Interface Kafka |

## 🔧 Structure du Projet

```
financial-streaming-pipeline/
├── data-producers/           # Producteurs de données financières
│   ├── market_data_producer.py
│   ├── market_data_consumer.py
│   └── config/kafka_config.yaml
├── flink-jobs/              # Jobs de traitement Flink (Scala)
│   ├── src/main/scala/com/financial/streaming/
│   └── trino-config/
├── ml-models/               # Modèles de ML et prédictions
│   ├── train_volatility_model.py
│   ├── train_anomaly_model.py
│   ├── predict_realtime.py
│   └── models/              # Modèles sauvegardés
├── infrastructure/          # Configuration Docker
│   ├── docker-compose.yml
│   └── docker-compose-mlflow.yml
├── scripts/                 # Scripts utilitaires
│   ├── start-complete-pipeline.sh
│   └── create-kafka-topics.ps1
└── docs/                    # Documentation
```

## 🎯 Fonctionnalités

### 📈 Analyse Temps Réel
- **Indicateurs Techniques**: RSI, MACD, Bandes de Bollinger
- **Détection de Volatilité**: Modèles ML pour prédire la volatilité
- **Détection d'Anomalies**: Identifier les comportements inhabituels
- **Agrégation en Fenêtres**: Analyses par périodes (1min, 5min, 1h)

### 🤖 Machine Learning
- **Modèles de Volatilité**: Prédiction de la volatilité future
- **Détection d'Anomalies**: Isolation Forest et Autoencodeurs
- **Suivi MLflow**: Expérimentations, versioning des modèles
- **Prédictions Temps Réel**: Application des modèles sur le stream

### 📊 Visualisation
- **Tableaux de Bord Superset**: Métriques temps réel
- **Graphiques Interactifs**: Évolution des prix, indicateurs
- **Alertes**: Notifications sur seuils configurables

## 🛠️ Technologies Utilisées

| Composant | Technologie | Version |
|-----------|------------|---------|
| **Streaming** | Apache Kafka | 7.4.0 |
| **Processing** | Apache Flink | 1.17.1 |
| **Query Engine** | Trino | 435 |
| **Storage** | Apache Iceberg | 1.4.0 |
| **ML Tracking** | MLflow | 2.9.2 |
| **Visualization** | Apache Superset | 3.0.0 |
| **Object Storage** | MinIO | latest |
| **Database** | PostgreSQL | 14 |

## 📖 Utilisation

### Entraînement des Modèles ML
```bash
cd ml-models

# Installer les dépendances
pip install -r requirements-ml.txt

# Entraîner le modèle de volatilité
python train_volatility_model.py

# Entraîner le modèle d'anomalies
python train_anomaly_model.py
```

### Requêtes SQL avec Trino
```sql
-- Voir les tables disponibles
SHOW TABLES FROM hive.financial_db;

-- Analyser les données de marché récentes
SELECT
    symbol,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    COUNT(*) as total_ticks
FROM hive.financial_db.market_ticks
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY symbol;

-- Calculer la volatilité
SELECT
    symbol,
    STDDEV(price) as volatility,
    AVG(volume) as avg_volume
FROM hive.financial_db.market_ticks
WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
GROUP BY symbol;
```

## 🔍 Monitoring et Debugging

### Logs des Services
```bash
# Logs Flink
docker-compose -f infrastructure/docker-compose.yml logs flink-jobmanager

# Logs Kafka
docker-compose -f infrastructure/docker-compose.yml logs kafka

# Logs Trino
docker-compose -f infrastructure/docker-compose.yml logs trino
```

### Santé des Services
```bash
# Vérifier l'état de tous les services
docker-compose -f infrastructure/docker-compose.yml ps

# Redémarrer un service spécifique
docker-compose -f infrastructure/docker-compose.yml restart kafka
```

## 🚨 Dépannage

### Problèmes Courants

**Kafka ne démarre pas**:
```bash
# Nettoyer les volumes Kafka
docker-compose -f infrastructure/docker-compose.yml down --volumes
docker-compose -f infrastructure/docker-compose.yml up -d kafka zookeeper
```

**MLflow database migration error**:
```bash
# Supprimer et recréer la DB MLflow
docker-compose -f infrastructure/docker-compose-mlflow.yml down --volumes
docker-compose -f infrastructure/docker-compose-mlflow.yml up -d
```

**Trino connection refused**:
- Vérifier que Trino est démarré: `docker-compose logs trino`
- Attendre que le service soit complètement initialisé (2-3 minutes)

## 🤝 Contribution

Voir [CONTRIBUTING.md](CONTRIBUTING.md) pour les guidelines de contribution.

## 📄 Licence

Ce projet est sous licence MIT - voir le fichier [LICENSE](LICENSE) pour plus de détails.

## 👥 Équipe

- **Équipe KA** - Développement du pipeline de données financières

## 📞 Support

Pour toute question ou problème:
1. Vérifier la section [Dépannage](#-dépannage)
2. Consulter les logs des services
3. Ouvrir une issue sur GitHub

---

⭐ **Note**: Ce projet est en développement actif. Les fonctionnalités peuvent évoluer.