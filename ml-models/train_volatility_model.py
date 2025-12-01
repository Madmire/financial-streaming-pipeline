"""
Entraînement d'un modèle de prédiction de volatilité
Utilise les données historiques d'Iceberg via Trino
"""

import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.preprocessing import StandardScaler
import joblib
from trino.dbapi import connect
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class VolatilityModelTrainer:
    def __init__(self, mlflow_tracking_uri="http://mlflow:5000"):
        """Initialiser le trainer avec connexion MLflow et Trino"""
        mlflow.set_tracking_uri(mlflow_tracking_uri)
        mlflow.set_experiment("volatility_prediction")
        
        # Connexion Trino
        self.trino_conn = connect(
            host='trino',
            port=8080,
            user='trino',
            catalog='hive',
            schema='financial_db'
        )
        
        self.scaler = StandardScaler()
    
    def fetch_training_data(self, symbol='AAPL', days_back=30):
        """Récupérer les données d'entraînement depuis Iceberg"""
        
        query = f"""
        WITH price_data AS (
            SELECT 
                symbol,
                timestamp,
                price,
                volume,
                LAG(price, 1) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_price_1,
                LAG(price, 5) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_price_5,
                LAG(price, 10) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_price_10,
                LAG(price, 20) OVER (PARTITION BY symbol ORDER BY timestamp) as prev_price_20,
                AVG(price) OVER (
                    PARTITION BY symbol 
                    ORDER BY timestamp 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as sma_20,
                STDDEV(price) OVER (
                    PARTITION BY symbol 
                    ORDER BY timestamp 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as volatility_20,
                AVG(volume) OVER (
                    PARTITION BY symbol 
                    ORDER BY timestamp 
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) as avg_volume_20
            FROM market_ticks
            WHERE symbol = '{symbol}'
                AND ingestion_date >= CURRENT_DATE - INTERVAL '{days_back}' DAY
        ),
        features AS (
            SELECT 
                symbol,
                timestamp,
                price,
                volume,
                -- Returns
                (price - prev_price_1) / prev_price_1 as return_1,
                (price - prev_price_5) / prev_price_5 as return_5,
                (price - prev_price_10) / prev_price_10 as return_10,
                (price - prev_price_20) / prev_price_20 as return_20,
                -- Technical indicators
                (price - sma_20) / sma_20 as price_to_sma,
                volatility_20 / price as relative_volatility,
                volume / avg_volume_20 as volume_ratio,
                -- Target: Future volatility (next 20 periods)
                LEAD(volatility_20, 10) OVER (PARTITION BY symbol ORDER BY timestamp) as future_volatility
            FROM price_data
        )
        SELECT * 
        FROM features
        WHERE future_volatility IS NOT NULL
            AND return_1 IS NOT NULL
            AND return_5 IS NOT NULL
        ORDER BY timestamp
        """
        
        cursor = self.trino_conn.cursor()
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        
        df = pd.DataFrame(data, columns=columns)
        print(f"✅ Récupéré {len(df)} enregistrements pour {symbol}")
        
        return df
    
    def prepare_features(self, df):
        """Préparer les features et la target"""
        
        feature_columns = [
            'return_1', 'return_5', 'return_10', 'return_20',
            'price_to_sma', 'relative_volatility', 'volume_ratio'
        ]
        
        X = df[feature_columns].fillna(0)
        y = df['future_volatility'].fillna(0)
        
        # Nettoyer les valeurs infinies
        X = X.replace([np.inf, -np.inf], 0)
        y = y.replace([np.inf, -np.inf], 0)
        
        return X, y
    
    def train_model(self, symbol='AAPL', model_type='random_forest'):
        """Entraîner le modèle avec MLflow tracking"""
        
        with mlflow.start_run(run_name=f"volatility_{symbol}_{model_type}"):
            
            # Log des paramètres
            mlflow.log_param("symbol", symbol)
            mlflow.log_param("model_type", model_type)
            mlflow.log_param("training_date", datetime.now().isoformat())
            
            # Récupérer les données
            print("📥 Récupération des données...")
            df = self.fetch_training_data(symbol=symbol)
            
            if len(df) < 100:
                print("❌ Pas assez de données pour l'entraînement")
                return None
            
            # Préparer les features
            print("🔧 Préparation des features...")
            X, y = self.prepare_features(df)
            
            # Split train/test
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, shuffle=False  # Time series: pas de shuffle
            )
            
            # Normalisation
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)
            
            # Sélection du modèle
            if model_type == 'random_forest':
                model = RandomForestRegressor(
                    n_estimators=100,
                    max_depth=10,
                    min_samples_split=5,
                    random_state=42,
                    n_jobs=-1
                )
            elif model_type == 'gradient_boosting':
                model = GradientBoostingRegressor(
                    n_estimators=100,
                    max_depth=5,
                    learning_rate=0.1,
                    random_state=42
                )
            else:
                raise ValueError(f"Type de modèle inconnu: {model_type}")
            
            mlflow.log_params(model.get_params())
            
            # Entraînement
            print(f"🎓 Entraînement du modèle {model_type}...")
            model.fit(X_train_scaled, y_train)
            
            # Prédictions
            y_pred_train = model.predict(X_train_scaled)
            y_pred_test = model.predict(X_test_scaled)
            
            # Métriques
            train_mse = mean_squared_error(y_train, y_pred_train)
            test_mse = mean_squared_error(y_test, y_pred_test)
            train_r2 = r2_score(y_train, y_pred_train)
            test_r2 = r2_score(y_test, y_pred_test)
            test_mae = mean_absolute_error(y_test, y_pred_test)
            
            # Log des métriques
            mlflow.log_metric("train_mse", train_mse)
            mlflow.log_metric("test_mse", test_mse)
            mlflow.log_metric("train_r2", train_r2)
            mlflow.log_metric("test_r2", test_r2)
            mlflow.log_metric("test_mae", test_mae)
            mlflow.log_metric("train_size", len(X_train))
            mlflow.log_metric("test_size", len(X_test))
            
            print(f"\n📊 Résultats:")
            print(f"  Train MSE: {train_mse:.6f}")
            print(f"  Test MSE:  {test_mse:.6f}")
            print(f"  Train R²:  {train_r2:.4f}")
            print(f"  Test R²:   {test_r2:.4f}")
            print(f"  Test MAE:  {test_mae:.6f}")
            
            # Feature importance (pour Random Forest)
            if hasattr(model, 'feature_importances_'):
                feature_importance = pd.DataFrame({
                    'feature': X.columns,
                    'importance': model.feature_importances_
                }).sort_values('importance', ascending=False)
                
                print(f"\n🎯 Feature Importance:")
                print(feature_importance)
                
                # Log comme artifact
                importance_file = f"feature_importance_{symbol}.csv"
                feature_importance.to_csv(importance_file, index=False)
                mlflow.log_artifact(importance_file)
            
            # Sauvegarder le modèle
            mlflow.sklearn.log_model(
                model, 
                "model",
                registered_model_name=f"volatility_predictor_{symbol}"
            )
            
            # Sauvegarder le scaler
            scaler_file = f"scaler_{symbol}.pkl"
            joblib.dump(self.scaler, scaler_file)
            mlflow.log_artifact(scaler_file)
            
            print(f"\n✅ Modèle entraîné et enregistré dans MLflow")
            
            return model, self.scaler

def main():
    """Script principal"""
    
    print("="*60)
    print("🤖 Entraînement du Modèle de Prédiction de Volatilité")
    print("="*60)
    
    trainer = VolatilityModelTrainer()
    
    # Entraîner pour plusieurs symboles
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']
    
    for symbol in symbols:
        print(f"\n{'='*60}")
        print(f"📈 Entraînement pour {symbol}")
        print('='*60)
        
        try:
            # Random Forest
            model_rf, scaler = trainer.train_model(
                symbol=symbol, 
                model_type='random_forest'
            )
            
            # Gradient Boosting
            model_gb, _ = trainer.train_model(
                symbol=symbol, 
                model_type='gradient_boosting'
            )
            
        except Exception as e:
            print(f"❌ Erreur pour {symbol}: {e}")
            continue
    
    print("\n" + "="*60)
    print("✨ Entraînement terminé!")
    print("🌐 Accédez à MLflow UI: http://localhost:5000")
    print("="*60)

if __name__ == "__main__":
    main()