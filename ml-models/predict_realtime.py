"""
Service de prédiction temps réel
Consomme les données de Kafka, applique les modèles ML, et republie les prédictions
"""

import json
import time
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
import mlflow
import mlflow.sklearn
import joblib
from collections import deque
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RealtimePredictor:
    def __init__(
        self,
        kafka_bootstrap='localhost:29092',
        mlflow_tracking_uri='http://localhost:5000',
        model_name='volatility_predictor_AAPL',
        model_version='latest'
    ):
        """Initialiser le service de prédiction temps réel"""
        
        # MLflow
        mlflow.set_tracking_uri(mlflow_tracking_uri)
        self.load_model(model_name, model_version)
        
        # Kafka Consumer
        self.consumer = KafkaConsumer(
            'stocks.AAPL', 'stocks.GOOGL', 'stocks.MSFT',
            bootstrap_servers=kafka_bootstrap,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            group_id='ml-predictor'
        )
        
        # Kafka Producer pour les prédictions
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Buffer pour calculer les features (fenêtre glissante)
        self.buffers = {}  # {symbol: deque of prices}
        self.buffer_size = 20
        
        logger.info("✅ Service de prédiction initialisé")
    
    def load_model(self, model_name, model_version):
        """Charger le modèle depuis MLflow"""
        try:
            if model_version == 'latest':
                model_uri = f"models:/{model_name}/latest"
            else:
                model_uri = f"models:/{model_name}/{model_version}"
            
            self.model = mlflow.sklearn.load_model(model_uri)
            logger.info(f"✅ Modèle chargé: {model_uri}")
            
            # Charger le scaler (si disponible)
            try:
                self.scaler = joblib.load(f"scaler_{model_name.split('_')[-1]}.pkl")
            except:
                logger.warning("⚠️  Scaler non trouvé, utilisation sans normalisation")
                self.scaler = None
                
        except Exception as e:
            logger.error(f"❌ Erreur de chargement du modèle: {e}")
            self.model = None
            self.scaler = None
    
    def update_buffer(self, symbol, tick_data):
        """Mettre à jour le buffer de données pour un symbole"""
        if symbol not in self.buffers:
            self.buffers[symbol] = {
                'prices': deque(maxlen=self.buffer_size),
                'volumes': deque(maxlen=self.buffer_size)
            }
        
        self.buffers[symbol]['prices'].append(tick_data['price'])
        self.buffers[symbol]['volumes'].append(tick_data['volume'])
    
    def calculate_features(self, symbol, current_tick):
        """Calculer les features pour la prédiction"""
        buffer = self.buffers.get(symbol)
        
        if not buffer or len(buffer['prices']) < self.buffer_size:
            return None
        
        prices = list(buffer['prices'])
        volumes = list(buffer['volumes'])
        current_price = current_tick['price']
        
        # Calculer les returns
        return_1 = (current_price - prices[-2]) / prices[-2] if len(prices) > 1 else 0
        return_5 = (current_price - prices[-6]) / prices[-6] if len(prices) > 5 else 0
        return_10 = (current_price - prices[-11]) / prices[-11] if len(prices) > 10 else 0
        return_20 = (current_price - prices[0]) / prices[0] if len(prices) == 20 else 0
        
        # SMA
        sma_20 = np.mean(prices)
        price_to_sma = (current_price - sma_20) / sma_20
        
        # Volatilité relative
        volatility_20 = np.std(prices)
        relative_volatility = volatility_20 / current_price if current_price > 0 else 0
        
        # Volume ratio
        avg_volume_20 = np.mean(volumes)
        volume_ratio = current_tick['volume'] / avg_volume_20 if avg_volume_20 > 0 else 1
        
        features = {
            'return_1': return_1,
            'return_5': return_5,
            'return_10': return_10,
            'return_20': return_20,
            'price_to_sma': price_to_sma,
            'relative_volatility': relative_volatility,
            'volume_ratio': volume_ratio
        }
        
        return features
    
    def predict(self, features_dict):
        """Faire une prédiction"""
        if self.model is None:
            return None
        
        # Convertir en DataFrame
        features_df = pd.DataFrame([features_dict])
        
        # Normaliser si scaler disponible
        if self.scaler is not None:
            features_scaled = self.scaler.transform(features_df)
        else:
            features_scaled = features_df.values
        
        # Prédiction
        prediction = self.model.predict(features_scaled)[0]
        
        return float(prediction)
    
    def process_tick(self, tick_data, topic):
        """Traiter un tick de marché"""
        symbol = tick_data['symbol']
        
        # Mettre à jour le buffer
        self.update_buffer(symbol, tick_data)
        
        # Calculer les features
        features = self.calculate_features(symbol, tick_data)
        
        if features is None:
            return  # Pas assez de données
        
        # Prédiction
        predicted_volatility = self.predict(features)
        
        if predicted_volatility is None:
            return
        
        # Créer le message de prédiction
        prediction_message = {
            'symbol': symbol,
            'timestamp': tick_data.get('timestamp', int(time.time() * 1000)),
            'current_price': tick_data['price'],
            'predicted_volatility': predicted_volatility,
            'features': features,
            'model': 'volatility_predictor',
            'prediction_time': int(time.time() * 1000)
        }
        
        # Publier sur Kafka
        self.producer.send('ml-predictions', prediction_message)
        
        # Log
        logger.info(
            f"[PREDICTION] {symbol}: Price={tick_data['price']:.2f}, "
            f"Predicted Vol={predicted_volatility*100:.2f}%"
        )
    
    def start(self):
        """Démarrer le service de prédiction"""
        logger.info("🚀 Démarrage du service de prédiction temps réel")
        logger.info("📥 Écoute des topics Kafka...")
        
        try:
            for message in self.consumer:
                try:
                    tick_data = message.value
                    self.process_tick(tick_data, message.topic)
                    
                except Exception as e:
                    logger.error(f"❌ Erreur de traitement: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("\n⏹️  Arrêt du service...")
        finally:
            self.consumer.close()
            self.producer.close()
            logger.info("✅ Service arrêté proprement")

def main():
    """Script principal"""
    
    print("="*60)
    print("🤖 Service de Prédiction ML Temps Réel")
    print("="*60)
    
    # Configuration
    predictor = RealtimePredictor(
        kafka_bootstrap='localhost:29092',
        mlflow_tracking_uri='http://localhost:5000',
        model_name='volatility_predictor_AAPL',
        model_version='latest'
    )
    
    # Démarrer
    predictor.start()

if __name__ == "__main__":
    main()