"""
Entraînement d'un modèle de détection d'anomalies
Utilise Isolation Forest et AutoEncoder
"""

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix
import joblib
from trino.dbapi import connect
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class AnomalyModelTrainer:
    def __init__(self, mlflow_tracking_uri="http://mlflow:5000"):
        """Initialiser le trainer"""
        mlflow.set_tracking_uri(mlflow_tracking_uri)
        mlflow.set_experiment("anomaly_detection")
        
        self.trino_conn = connect(
            host='trino',
            port=8080,
            user='trino',
            catalog='hive',
            schema='financial_db'
        )
        
        self.scaler = StandardScaler()
    
    def fetch_training_data(self, symbol='AAPL', days_back=30):
        """Récupérer les données avec anomalies connues"""
        
        query = f"""
        WITH market_features AS (
            SELECT 
                t.symbol,
                t.timestamp,
                t.price,
                t.volume,
                t.bid,
                t.ask,
                (t.ask - t.bid) / t.price as spread_pct,
                LAG(t.price, 1) OVER (PARTITION BY t.symbol ORDER BY t.timestamp) as prev_price,
                AVG(t.price) OVER (
                    PARTITION BY t.symbol 
                    ORDER BY t.timestamp 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as sma_20,
                STDDEV(t.price) OVER (
                    PARTITION BY t.symbol 
                    ORDER BY t.timestamp 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as std_20,
                AVG(t.volume) OVER (
                    PARTITION BY t.symbol 
                    ORDER BY t.timestamp 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as avg_volume_20,
                STDDEV(CAST(t.volume AS DOUBLE)) OVER (
                    PARTITION BY t.symbol 
                    ORDER BY t.timestamp 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as std_volume_20
            FROM market_ticks t
            WHERE t.symbol = '{symbol}'
                AND t.ingestion_date >= CURRENT_DATE - INTERVAL '{days_back}' DAY
        ),
        features_with_labels AS (
            SELECT 
                f.*,
                (f.price - f.prev_price) / f.prev_price as price_change,
                (f.price - f.sma_20) / NULLIF(f.std_20, 0) as price_zscore,
                (CAST(f.volume AS DOUBLE) - f.avg_volume_20) / NULLIF(f.std_volume_20, 0) as volume_zscore,
                CASE WHEN a.symbol IS NOT NULL THEN 1 ELSE 0 END as is_anomaly,
                COALESCE(a.anomaly_type, 'normal') as anomaly_type
            FROM market_features f
            LEFT JOIN market_anomalies a 
                ON f.symbol = a.symbol 
                AND ABS(f.timestamp - CAST(TO_UNIXTIME(a.event_timestamp) AS BIGINT) * 1000) < 5000
        )
        SELECT *
        FROM features_with_labels
        WHERE std_20 IS NOT NULL 
            AND prev_price IS NOT NULL
        ORDER BY timestamp
        """
        
        cursor = self.trino_conn.cursor()
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        
        df = pd.DataFrame(data, columns=columns)
        print(f"✅ Récupéré {len(df)} enregistrements")
        print(f"   Anomalies: {df['is_anomaly'].sum()} ({df['is_anomaly'].mean()*100:.2f}%)")
        
        return df
    
    def prepare_features(self, df):
        """Préparer les features"""
        
        feature_columns = [
            'price', 'volume', 'spread_pct', 'price_change',
            'price_zscore', 'volume_zscore'
        ]
        
        X = df[feature_columns].fillna(0)
        X = X.replace([np.inf, -np.inf], 0)
        
        y = df['is_anomaly'].values
        
        return X, y
    
    def train_isolation_forest(self, symbol='AAPL'):
        """Entraîner Isolation Forest"""
        
        with mlflow.start_run(run_name=f"anomaly_{symbol}_isolation_forest"):
            
            mlflow.log_param("symbol", symbol)
            mlflow.log_param("model_type", "isolation_forest")
            mlflow.log_param("training_date", datetime.now().isoformat())
            
            # Données
            print("📥 Récupération des données...")
            df = self.fetch_training_data(symbol=symbol)
            
            if len(df) < 100:
                print("❌ Pas assez de données")
                return None
            
            X, y = self.prepare_features(df)
            
            # Normalisation
            X_scaled = self.scaler.fit_transform(X)
            
            # Modèle Isolation Forest
            # contamination = proportion d'anomalies attendue
            contamination = max(0.01, min(0.1, y.mean()))
            
            model = IsolationForest(
                n_estimators=100,
                contamination=contamination,
                random_state=42,
                n_jobs=-1
            )
            
            mlflow.log_param("contamination", contamination)
            mlflow.log_param("n_estimators", 100)
            
            print(f"🎓 Entraînement Isolation Forest...")
            model.fit(X_scaled)
            
            # Prédictions (-1 = anomalie, 1 = normal)
            predictions = model.predict(X_scaled)
            predictions_binary = (predictions == -1).astype(int)
            
            # Scores d'anomalie
            anomaly_scores = model.decision_function(X_scaled)
            
            # Métriques
            if y.sum() > 0:  # Si on a des labels
                from sklearn.metrics import precision_score, recall_score, f1_score
                
                precision = precision_score(y, predictions_binary)
                recall = recall_score(y, predictions_binary)
                f1 = f1_score(y, predictions_binary)
                
                mlflow.log_metric("precision", precision)
                mlflow.log_metric("recall", recall)
                mlflow.log_metric("f1_score", f1)
                
                print(f"\n📊 Résultats:")
                print(f"  Precision: {precision:.4f}")
                print(f"  Recall:    {recall:.4f}")
                print(f"  F1 Score:  {f1:.4f}")
                
                # Matrice de confusion
                cm = confusion_matrix(y, predictions_binary)
                print(f"\nMatrice de confusion:")
                print(cm)
            
            # Statistiques des anomalies détectées
            n_anomalies = predictions_binary.sum()
            pct_anomalies = n_anomalies / len(predictions_binary) * 100
            
            mlflow.log_metric("detected_anomalies", n_anomalies)
            mlflow.log_metric("anomaly_percentage", pct_anomalies)
            
            print(f"\n🚨 Anomalies détectées: {n_anomalies} ({pct_anomalies:.2f}%)")
            
            # Top anomalies
            top_indices = np.argsort(anomaly_scores)[:10]
            top_anomalies = df.iloc[top_indices][['timestamp', 'price', 'volume', 'anomaly_type']]
            print(f"\n🔝 Top 10 anomalies:")
            print(top_anomalies)
            
            # Sauvegarder
            mlflow.sklearn.log_model(
                model,
                "model",
                registered_model_name=f"anomaly_detector_{symbol}"
            )
            
            scaler_file = f"scaler_anomaly_{symbol}.pkl"
            joblib.dump(self.scaler, scaler_file)
            mlflow.log_artifact(scaler_file)
            
            print(f"\n✅ Modèle enregistré dans MLflow")
            
            return model, self.scaler
    
    def train_autoencoder(self, symbol='AAPL'):
        """Entraîner un AutoEncoder pour détection d'anomalies"""
        
        try:
            import tensorflow as tf
            from tensorflow import keras
            from tensorflow.keras import layers
        except ImportError:
            print("⚠️  TensorFlow non installé, skip AutoEncoder")
            return None
        
        with mlflow.start_run(run_name=f"anomaly_{symbol}_autoencoder"):
            
            mlflow.log_param("symbol", symbol)
            mlflow.log_param("model_type", "autoencoder")
            
            # Données
            df = self.fetch_training_data(symbol=symbol)
            X, y = self.prepare_features(df)
            X_scaled = self.scaler.fit_transform(X)
            
            # Séparer normal/anomalie
            X_normal = X_scaled[y == 0]
            X_anomaly = X_scaled[y == 1]
            
            if len(X_normal) < 50:
                print("❌ Pas assez de données normales")
                return None
            
            # Architecture AutoEncoder
            input_dim = X_scaled.shape[1]
            encoding_dim = max(2, input_dim // 2)
            
            # Encoder
            encoder = keras.Sequential([
                layers.Dense(encoding_dim * 2, activation='relu', input_shape=(input_dim,)),
                layers.Dense(encoding_dim, activation='relu')
            ])
            
            # Decoder
            decoder = keras.Sequential([
                layers.Dense(encoding_dim * 2, activation='relu', input_shape=(encoding_dim,)),
                layers.Dense(input_dim, activation='linear')
            ])
            
            # AutoEncoder complet
            autoencoder = keras.Sequential([encoder, decoder])
            autoencoder.compile(optimizer='adam', loss='mse')
            
            mlflow.log_param("encoding_dim", encoding_dim)
            mlflow.log_param("epochs", 50)
            
            # Entraînement sur données normales uniquement
            print("🎓 Entraînement AutoEncoder...")
            history = autoencoder.fit(
                X_normal, X_normal,
                epochs=50,
                batch_size=32,
                validation_split=0.2,
                verbose=0
            )
            
            # Reconstruction error
            reconstructions = autoencoder.predict(X_scaled)
            mse = np.mean(np.power(X_scaled - reconstructions, 2), axis=1)
            
            # Définir threshold (95e percentile des erreurs normales)
            mse_normal = mse[y == 0]
            threshold = np.percentile(mse_normal, 95)
            
            mlflow.log_metric("reconstruction_threshold", threshold)
            
            # Prédictions
            predictions = (mse > threshold).astype(int)
            
            if y.sum() > 0:
                from sklearn.metrics import precision_score, recall_score, f1_score
                
                precision = precision_score(y, predictions)
                recall = recall_score(y, predictions)
                f1 = f1_score(y, predictions)
                
                mlflow.log_metric("precision", precision)
                mlflow.log_metric("recall", recall)
                mlflow.log_metric("f1_score", f1)
                
                print(f"\n📊 Résultats AutoEncoder:")
                print(f"  Precision: {precision:.4f}")
                print(f"  Recall:    {recall:.4f}")
                print(f"  F1 Score:  {f1:.4f}")
            
            # Sauvegarder
            model_path = f"autoencoder_{symbol}"
            autoencoder.save(model_path)
            mlflow.log_artifacts(model_path)
            
            print(f"✅ AutoEncoder enregistré")
            
            return autoencoder, threshold

def main():
    """Script principal"""
    
    print("="*60)
    print("🚨 Entraînement des Modèles de Détection d'Anomalies")
    print("="*60)
    
    trainer = AnomalyModelTrainer()
    
    symbols = ['AAPL', 'GOOGL', 'MSFT']
    
    for symbol in symbols:
        print(f"\n{'='*60}")
        print(f"📈 Entraînement pour {symbol}")
        print('='*60)
        
        try:
            # Isolation Forest
            model_if, scaler = trainer.train_isolation_forest(symbol=symbol)
            
            # AutoEncoder (optionnel)
            # model_ae, threshold = trainer.train_autoencoder(symbol=symbol)
            
        except Exception as e:
            print(f"❌ Erreur pour {symbol}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    print("\n" + "="*60)
    print("✨ Entraînement terminé!")
    print("🌐 MLflow UI: http://localhost:5000")
    print("="*60)

if __name__ == "__main__":
    main()