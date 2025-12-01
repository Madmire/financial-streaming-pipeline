#!/bin/bash

set -e

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   🚀 FINANCIAL STREAMING PIPELINE - DÉMARRAGE COMPLET   ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}\n"

# ===================================================================
# Étape 1 : Vérification de l'environnement
# ===================================================================
echo -e "${BLUE}[1/9] 🔍 Vérification de l'environnement${NC}"

check_command() {
    if ! command -v $1 &> /dev/null; then
        echo -e "${RED}❌ $1 n'est pas installé${NC}"
        exit 1
    else
        echo -e "${GREEN}✅ $1 disponible${NC}"
    fi
}

check_command docker
check_command docker-compose
check_command python3
check_command sbt

echo ""

# ===================================================================
# Étape 2 : Nettoyage (optionnel)
# ===================================================================
echo -e "${BLUE}[2/9] 🧹 Nettoyage des anciens containers${NC}"
read -p "Voulez-vous nettoyer les anciens containers? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    docker-compose down -v 2>/dev/null || true
    echo -e "${GREEN}✅ Nettoyage effectué${NC}\n"
else
    echo -e "${YELLOW}⏭️  Nettoyage ignoré${NC}\n"
fi

# ===================================================================
# Étape 3 : Démarrage de l'infrastructure
# ===================================================================
echo -e "${BLUE}[3/9] 🏗️  Démarrage de l'infrastructure${NC}"

docker-compose up -d

echo -e "${YELLOW}⏳ Attente du démarrage des services (90 secondes)...${NC}"
sleep 90

# Vérifier les services
SERVICES=(
    "zookeeper"
    "kafka"
    "minio"
    "hive-metastore"
    "postgres"
    "flink-jobmanager"
    "flink-taskmanager"
    "trino"
    "superset"
    "grafana"
)

echo -e "\n${BLUE}Vérification des services:${NC}"
ALL_UP=true
for service in "${SERVICES[@]}"; do
    if docker-compose ps | grep "$service" | grep -q "Up"; then
        echo -e "${GREEN}✅ $service${NC}"
    else
        echo -e "${RED}❌ $service (non démarré)${NC}"
        ALL_UP=false
    fi
done

if [ "$ALL_UP" = false ]; then
    echo -e "\n${RED}⚠️  Certains services n'ont pas démarré correctement${NC}"
    read -p "Continuer quand même? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo ""

# ===================================================================
# Étape 4 : Démarrage MLflow
# ===================================================================
echo -e "${BLUE}[4/9] 🤖 Démarrage de MLflow${NC}"

if [ -f "docker-compose-mlflow.yml" ]; then
    docker-compose -f docker-compose-mlflow.yml up -d
    echo -e "${YELLOW}⏳ Attente de MLflow (30 secondes)...${NC}"
    sleep 30
    
    # Créer le bucket MLflow dans MinIO
    docker exec minio-init mc mb myminio/mlflow-artifacts --ignore-existing || true
    docker exec minio-init mc policy set public myminio/mlflow-artifacts || true
    
    echo -e "${GREEN}✅ MLflow démarré${NC}\n"
else
    echo -e "${YELLOW}⚠️  docker-compose-mlflow.yml non trouvé, MLflow ignoré${NC}\n"
fi

# ===================================================================
# Étape 5 : Initialisation d'Iceberg
# ===================================================================
echo -e "${BLUE}[5/9] 🧊 Initialisation des tables Iceberg${NC}"

echo -e "${YELLOW}Création du bucket warehouse...${NC}"
docker exec minio-init mc mb myminio/warehouse --ignore-existing || true
docker exec minio-init mc policy set public myminio/warehouse || true

echo -e "${YELLOW}Création des tables Iceberg via Trino...${NC}"
sleep 20  # Attendre que Trino soit vraiment prêt

docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.financial_db WITH (location = 's3://warehouse/financial_db')" 2>/dev/null || true

# Tables principales
for table in market_ticks market_aggregates_1min volatility_metrics market_anomalies technical_indicators; do
    echo -e "${YELLOW}  Création de $table...${NC}"
done

# Exécuter le script d'initialisation si disponible
if [ -f "iceberg/init-iceberg-tables.sql" ]; then
    docker exec trino trino --file /tmp/init-iceberg-tables.sql 2>/dev/null || true
fi

echo -e "${GREEN}✅ Tables Iceberg initialisées${NC}\n"

# ===================================================================
# Étape 6 : Compilation et soumission du job Flink
# ===================================================================
echo -e "${BLUE}[6/9] ⚙️  Compilation du job Flink${NC}"

if [ -d "flink-jobs" ]; then
    cd flink-jobs
    
    echo -e "${YELLOW}Compilation en cours (cela peut prendre quelques minutes)...${NC}"
    export SBT_OPTS="-Xmx2G -Xms512M"
    sbt clean assembly
    
    JAR_FILE="target/scala-2.12/financial-streaming-pipeline-assembly-1.0.jar"
    
    if [ -f "$JAR_FILE" ]; then
        JAR_SIZE=$(du -h "$JAR_FILE" | cut -f1)
        echo -e "${GREEN}✅ Compilation réussie (${JAR_SIZE})${NC}"
        
        # Copier vers Flink
        echo -e "${YELLOW}Copie du JAR vers Flink...${NC}"
        docker cp "$JAR_FILE" flink-jobmanager:/opt/flink/usrlib/
        docker cp "$JAR_FILE" flink-taskmanager:/opt/flink/usrlib/
        
        # Soumettre le job
        echo -e "${YELLOW}Soumission du job Flink...${NC}"
        JOB_OUTPUT=$(docker exec flink-jobmanager flink run \
            -d \
            -c com.finance.streaming.UnifiedMarketProcessor \
            /opt/flink/usrlib/financial-streaming-pipeline-assembly-1.0.jar 2>&1)
        
        if echo "$JOB_OUTPUT" | grep -q "Job has been submitted"; then
            JOB_ID=$(echo "$JOB_OUTPUT" | grep -oP 'JobID \K[a-f0-9]+')
            echo -e "${GREEN}✅ Job Flink soumis (ID: $JOB_ID)${NC}"
        else
            echo -e "${RED}❌ Erreur lors de la soumission${NC}"
            echo "$JOB_OUTPUT"
        fi
    else
        echo -e "${RED}❌ JAR non trouvé après compilation${NC}"
    fi
    
    cd ..
else
    echo -e "${YELLOW}⚠️  Répertoire flink-jobs non trouvé${NC}"
fi

echo ""

# ===================================================================
# Étape 7 : Configuration de Superset
# ===================================================================
echo -e "${BLUE}[7/9] 📊 Configuration de Superset${NC}"

if [ -f "init-superset-complete.sh" ]; then
    chmod +x init-superset-complete.sh
    ./init-superset-complete.sh
else
    echo -e "${YELLOW}⚠️  Script d'initialisation Superset non trouvé${NC}"
fi

echo ""

# ===================================================================
# Étape 8 : Démarrage du producteur de données
# ===================================================================
echo -e "${BLUE}[8/9] 📡 Démarrage du producteur de données${NC}"

read -p "Voulez-vous démarrer le producteur maintenant? (Y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Nn]$ ]]; then
    if [ -d "data-producers" ]; then
        cd data-producers
        
        # Installer les dépendances si nécessaire
        if [ ! -d "venv" ]; then
            echo -e "${YELLOW}Création de l'environnement virtuel...${NC}"
            python3 -m venv venv
            source venv/bin/activate
            pip install -r requirements.txt
        else
            source venv/bin/activate
        fi
        
        echo -e "${GREEN}✅ Démarrage du producteur en arrière-plan${NC}"
        nohup python market_data_producer.py > ../logs/producer.log 2>&1 &
        PRODUCER_PID=$!
        echo $PRODUCER_PID > ../logs/producer.pid
        echo -e "${YELLOW}PID du producteur: $PRODUCER_PID${NC}"
        
        cd ..
    else
        echo -e "${RED}❌ Répertoire data-producers non trouvé${NC}"
    fi
else
    echo -e "${YELLOW}⏭️  Producteur non démarré${NC}"
fi

echo ""

# ===================================================================
# Étape 9 : Résumé et accès
# ===================================================================
echo -e "${BLUE}[9/9] 📋 Résumé du déploiement${NC}\n"

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║                  🎉 DÉPLOIEMENT TERMINÉ !                ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}\n"

echo -e "${GREEN}✅ Le pipeline financier est opérationnel!${NC}\n"

echo -e "${BLUE}📊 Interfaces Web:${NC}"
echo -e "  ┌─────────────────────────────────────────────────────┐"
echo -e "  │ Kafka UI      : ${YELLOW}http://localhost:8080${NC}           │"
echo -e "  │ Flink         : ${YELLOW}http://localhost:8081${NC}           │"
echo -e "  │ Trino         : ${YELLOW}http://localhost:8082${NC}           │"
echo -e "  │ Superset      : ${YELLOW}http://localhost:8088${NC}  (admin/admin) │"
echo -e "  │ Grafana       : ${YELLOW}http://localhost:3000${NC}  (admin/admin123) │"
echo -e "  │ MinIO Console : ${YELLOW}http://localhost:9001${NC}  (admin/password123) │"
echo -e "  │ MLflow        : ${YELLOW}http://localhost:5000${NC}           │"
echo -e "  │ Jupyter       : ${YELLOW}http://localhost:8888${NC}           │"
echo -e "  └─────────────────────────────────────────────────────┘"

echo -e "\n${BLUE}🔍 Commandes utiles:${NC}"
echo -e "  ${YELLOW}# Voir les logs Flink${NC}"
echo -e "  docker-compose logs -f flink-taskmanager"
echo -e ""
echo -e "  ${YELLOW}# Requête Trino${NC}"
echo -e "  docker exec -it trino trino"
echo -e "  > SELECT COUNT(*) FROM iceberg.financial_db.market_ticks;"
echo -e ""
echo -e "  ${YELLOW}# Arrêter le producteur${NC}"
echo -e "  kill \$(cat logs/producer.pid)"
echo -e ""
echo -e "  ${YELLOW}# Voir les données dans Kafka${NC}"
echo -e "  docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic stocks.AAPL --max-messages 5"

echo -e "\n${BLUE}📚 Prochaines étapes:${NC}"
echo -e "  1. ${GREEN}Accéder à Superset et créer des dashboards${NC}"
echo -e "  2. ${GREEN}Entraîner les modèles ML${NC}"
echo -e "     cd ml-models && python train_volatility_model.py"
echo -e "  3. ${GREEN}Démarrer les prédictions temps réel${NC}"
echo -e "     python predict_realtime.py"
echo -e "  4. ${GREEN}Consulter les métriques dans Grafana${NC}"

echo -e "\n${BLUE}📖 Documentation:${NC}"
echo -e "  • README.md"
echo -e "  • docs/ARCHITECTURE_FINALE.md"
echo -e "  • docs/TROUBLESHOOTING.md"
echo -e "  • BUILD_GUIDE.md"

echo -e "\n${GREEN}🎊 Profitez de votre pipeline de streaming financier!${NC}\n"

# Proposer d'ouvrir les dashboards
read -p "Voulez-vous ouvrir les interfaces web? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    if command -v xdg-open &> /dev/null; then
        xdg-open http://localhost:8081 2>/dev/null &  # Flink
        xdg-open http://localhost:8088 2>/dev/null &  # Superset
        xdg-open http://localhost:8080 2>/dev/null &  # Kafka UI
    elif command -v open &> /dev/null; then
        open http://localhost:8081 &  # Flink
        open http://localhost:8088 &  # Superset
        open http://localhost:8080 &  # Kafka UI
    else
        echo -e "${YELLOW}Ouvrez manuellement les URLs ci-dessus${NC}"
    fi
fi