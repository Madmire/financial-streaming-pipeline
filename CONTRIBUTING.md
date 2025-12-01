# 🤝 Guide de Contribution

Bienvenue dans le projet **Financial Streaming Pipeline** ! Nous sommes heureux de votre intérêt pour contribuer à ce projet.

## 📋 Code de Conduite

Ce projet adhère à un code de conduite pour assurer un environnement inclusif et respectueux pour tous les contributeurs.

### Nos Standards
- **Respect** : Traiter tous les participants avec respect
- **Inclusivité** : Encourager la diversité et l'inclusion
- **Collaboration** : Travailler ensemble de manière constructive
- **Qualité** : Maintenir des standards élevés de code et documentation

## 🚀 Développement Local

### Prérequis
- Docker & Docker Compose
- Python 3.8+
- Scala 2.12+ (pour Flink)
- Git

### Configuration de l'Environnement

1. **Cloner le projet**:
```bash
git clone https://github.com/sanogomamadou/financial-streaming-pipeline.git
cd financial-streaming-pipeline
```

2. **Créer un environnement virtuel Python**:
```bash
# Pour les producteurs de données
cd data-producers
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou venv\Scripts\activate  # Windows
pip install -r requirements.txt

# Pour les modèles ML
cd ../ml-models
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou venv\Scripts\activate  # Windows
pip install -r requirements-ml.txt
```

3. **Démarrer l'infrastructure**:
```bash
# Infrastructure complète
docker-compose -f infrastructure/docker-compose.yml up -d

# Ou seulement MLflow pour le développement ML
docker-compose -f infrastructure/docker-compose-mlflow.yml up -d
```

## 💻 Standards de Code

### Python
- **Style** : PEP 8
- **Formatage** : Black (`black .`)
- **Linting** : Flake8
- **Imports** : Organisation avec `isort`

```bash
# Installation des outils
pip install black flake8 isort

# Formatage automatique
black .
isort .

# Vérification
flake8 .
```

### Scala (Flink)
- **Style** : Scala Style Guide officiel
- **Formatage** : Scalafmt
- Utiliser `sbt scalafmt` pour formatter

### Configuration
- **YAML** : Utiliser des guillemets pour les valeurs string
- **Docker Compose** : Version 3.8+, services nommés clairement
- **Scripts** : Headers bash avec droits d'exécution

## 🔄 Workflow de Développement

### 1. Créer une Branche
```bash
# Créer une branche descriptive
git checkout -b feature/nom-de-la-fonctionnalite
# ou
git checkout -b fix/nom-du-bug
# ou
git checkout -b docs/ajout-documentation
```

### 2. Commits Conventionnels
Suivre la convention [Conventional Commits](https://conventionalcommits.org/):

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**Types autorisés**:
- `feat`: Nouvelle fonctionnalité
- `fix`: Correction de bug
- `docs`: Changements de documentation
- `style`: Changements de style (formatage, etc.)
- `refactor`: Refactorisation du code
- `test`: Ajout ou modification de tests
- `chore`: Tâches de maintenance

**Exemples**:
```bash
git commit -m "feat: add real-time volatility prediction model"
git commit -m "fix: resolve kafka connection timeout issue"
git commit -m "docs: update API documentation for ML endpoints"
```

### 3. Tests
```bash
# Tests unitaires Python
python -m pytest

# Tests d'intégration Kafka
./scripts/test_kafka_pipeline.sh

# Tests MLflow
python ml-models/test_model_training.py
```

### 4. Pull Request
1. **Synchroniser** avec main:
```bash
git fetch origin
git rebase origin/main
```

2. **Push** votre branche:
```bash
git push origin feature/nom-de-la-fonctionnalite
```

3. **Créer** une Pull Request sur GitHub avec:
   - Description claire du changement
   - Références aux issues liées
   - Captures d'écran si applicable
   - Tests effectués

## 🧪 Tests

### Types de Tests
- **Unitaires** : Fonctions individuelles
- **Intégration** : Interaction entre composants
- **End-to-End** : Pipeline complet
- **Performance** : Charges élevées

### Exécution des Tests
```bash
# Tests Python
cd ml-models
python -m pytest tests/ -v

# Tests Kafka
./scripts/test_kafka_pipeline.sh

# Tests de charge
python data-producers/stress_test_producer.py
```

## 📁 Structure des Fichiers

### Nommage
- **Fichiers** : `snake_case.py` pour Python, `CamelCase.scala` pour Scala
- **Dossiers** : `kebab-case` ou `snake_case`
- **Variables** : `snake_case` en Python, `camelCase` en Scala
- **Classes** : `PascalCase` dans les deux langages

### Organisation
```
# Nouvelles fonctionnalités dans les dossiers appropriés
data-producers/     # Producteurs de données
ml-models/         # Modèles ML
flink-jobs/        # Jobs Flink
infrastructure/    # Configuration Docker
scripts/          # Scripts utilitaires
```

## 🔧 Outils Requis

### Outils de Développement
```bash
# Python
pip install black flake8 isort pytest mypy

# Scala (via sbt)
# scalafmt est configuré dans plugins.sbt

# Pre-commit hooks (recommandé)
pip install pre-commit
pre-commit install
```

### IDE Recommandés
- **Python** : VS Code avec extensions Python, Pylance
- **Scala** : IntelliJ IDEA avec plugin Scala
- **Docker** : Docker Desktop

## 🚨 Gestion des Issues

### Création d'Issues
- **Bug** : Template détaillé avec steps to reproduce
- **Feature** : Description claire avec use case
- **Question** : Section Discussions ou issues avec label `question`

### Labels Standards
- `bug` : Bug à corriger
- `enhancement` : Nouvelle fonctionnalité
- `documentation` : Amélioration docs
- `help wanted` : Besoin d'aide
- `good first issue` : Idéal pour débutants

## 📊 Métriques et Performance

### Suivi des Performances
- **Latence** : < 100ms pour le processing temps réel
- **Throughput** : > 1000 messages/seconde
- **Disponibilité** : 99.9% uptime des services

### Monitoring
```bash
# Métriques Flink
docker-compose logs flink-jobmanager | grep throughput

# Métriques Kafka
docker-compose exec kafka kafka-consumer-groups.sh --describe --group flink-consumer
```

## 🔒 Sécurité

### Bonnes Pratiques
- Ne pas committer de credentials
- Utiliser des variables d'environnement
- Scanner les dépendances pour vulnérabilités
- Chiffrement des données sensibles

### Secrets
- Utiliser `.env` files (ignorés par git)
- Variables d'environnement Docker
- Gestion centralisée des secrets

## 🎯 Revue de Code

### Checklist pour Reviewers
- [ ] Code respecte les standards
- [ ] Tests présents et passent
- [ ] Documentation mise à jour
- [ ] Performance acceptable
- [ ] Sécurité respectée

### Checklist pour Contributors
- [ ] Commits suivent la convention
- [ ] Tests locaux passent
- [ ] Linting OK
- [ ] Documentation à jour
- [ ] Changements testés manuellement

## 📚 Ressources

### Documentation
- [Architecture du projet](docs/architecture.md)
- [Guide de déploiement](docs/deployment.md)
- [API MLflow](https://mlflow.org/docs/latest/index.html)
- [Documentation Flink](https://flink.apache.org/documentation/)

### Liens Utiles
- [Conventional Commits](https://conventionalcommits.org/)
- [PEP 8 Style Guide](https://pep8.org/)
- [Scala Style Guide](https://docs.scala-lang.org/style/)

## 🙋 Support

Besoin d'aide ?
1. Vérifier la documentation existante
2. Chercher dans les issues GitHub
3. Ouvrir une nouvelle issue
4. Contacter l'équipe via Slack/Discord

---

**Merci de contribuer à Financial Streaming Pipeline ! 🎉**