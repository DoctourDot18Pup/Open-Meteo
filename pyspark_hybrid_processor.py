import pandas as pd
import numpy as np
import logging
from datetime import datetime
import json
import glob
import os
import sys

# Importaciones de ML
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
import warnings

# Configuración inicial
warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LocalMLWeatherProcessor:
    """
    Procesador ML sin PySpark - usando Pandas para evitar problemas HDFS
    """
    def __init__(self):
        # Configuración de cultivos
        self.crop_configurations = {
            "maiz": {
                "name": "Maíz",
                "frost_threshold": 0,
                "heat_threshold": 35,
                "drought_days": 15,
                "optimal_temp_min": 15,
                "optimal_temp_max": 30,
                "critical_growth_months": [5, 6, 7, 8],
                "ml_weight_frost": 3.0,
                "ml_weight_heat": 2.0,
                "ml_weight_drought": 2.5
            },
            "frijol": {
                "name": "Frijol",
                "frost_threshold": 2,
                "heat_threshold": 32,
                "drought_days": 10,
                "optimal_temp_min": 18,
                "optimal_temp_max": 28,
                "critical_growth_months": [6, 7, 8, 9],
                "ml_weight_frost": 4.0,
                "ml_weight_heat": 1.5,
                "ml_weight_drought": 3.0
            },
            "chile": {
                "name": "Chile",
                "frost_threshold": 4,
                "heat_threshold": 38,
                "drought_days": 7,
                "optimal_temp_min": 20,
                "optimal_temp_max": 35,
                "critical_growth_months": [4, 5, 6, 7, 8],
                "ml_weight_frost": 4.5,
                "ml_weight_heat": 1.0,
                "ml_weight_drought": 2.0
            },
            "trigo": {
                "name": "Trigo",
                "frost_threshold": -2,
                "heat_threshold": 30,
                "drought_days": 20,
                "optimal_temp_min": 10,
                "optimal_temp_max": 25,
                "critical_growth_months": [11, 12, 1, 2, 3],
                "ml_weight_frost": 2.0,
                "ml_weight_heat": 2.5,
                "ml_weight_drought": 2.0
            }
        }

    def load_hybrid_data(self, data_path):
        """Cargar datos híbridos usando Pandas"""
        try:
            logger.info("📊 Cargando datos híbridos...")
            
            if not os.path.exists(data_path):
                raise FileNotFoundError(f"No se encuentra el archivo: {data_path}")

            df = pd.read_csv(data_path)
            
            # Convertir columna date
            df['date'] = pd.to_datetime(df['date'])
            
            logger.info(f"📈 Datos cargados: {len(df)} registros")
            # logger.info(f"📋 Columnas: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error cargando datos: {e}")
            raise

    def clean_and_enhance_data(self, df):
        """Limpieza y enriquecimiento de datos"""
        try:
            logger.info("🧹 Limpiando y enriqueciendo datos...")
            
            # Limpiar valores nulos en columnas críticas
            critical_columns = [
                'temperature_2m_max', 'temperature_2m_min', 'precipitation_sum',
                'et0_fao_evapotranspiration', 'shortwave_radiation_sum'
            ]
            
            for col_name in critical_columns:
                if col_name in df.columns:
                    df[col_name] = df[col_name].fillna(df[col_name].mean())
            
            # Features temporales
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            df['day_of_month'] = df['date'].dt.day
            df['day_of_year'] = df['date'].dt.dayofyear
            df['quarter'] = df['date'].dt.quarter
            
            # Estaciones agrícolas
            def get_agricultural_season(month):
                if month in [12, 1, 2]:
                    return "invierno"
                elif month in [3, 4, 5]:
                    return "primavera"
                elif month in [6, 7, 8]:
                    return "verano"
                else:
                    return "otono"
            
            df['agricultural_season'] = df['month'].apply(get_agricultural_season)
            
            # Features derivados
            if 'temperature_2m_max' in df.columns and 'temperature_2m_min' in df.columns:
                df['thermal_amplitude'] = df['temperature_2m_max'] - df['temperature_2m_min']
                df['temp_mean'] = (df['temperature_2m_max'] + df['temperature_2m_min']) / 2
            
            # Balance hídrico
            if 'et0_fao_evapotranspiration' in df.columns and 'precipitation_sum' in df.columns:
                df['water_balance'] = df['precipitation_sum'] - df['et0_fao_evapotranspiration']
                df['water_deficit'] = np.where(df['water_balance'] < 0, -df['water_balance'], 0)
                df['water_surplus'] = np.where(df['water_balance'] > 0, df['water_balance'], 0)
            
            # Grados día acumulados
            if 'temp_mean' in df.columns:
                df['growing_degree_days'] = np.maximum(df['temp_mean'] - 10, 0)
            
            # Variables de estrés
            if 'temperature_2m_min' in df.columns:
                df['frost_risk'] = (df['temperature_2m_min'] <= 2).astype(int)
            if 'temperature_2m_max' in df.columns:
                df['heat_stress'] = (df['temperature_2m_max'] > 35).astype(int)
            if 'water_deficit' in df.columns:
                df['drought_indicator'] = (df['water_deficit'] > 3).astype(int)
            
            # UV processing
            if 'uv_index_max' in df.columns:
                df['uv_stress'] = (df['uv_index_max'] > 8).astype(int)
            elif 'uv_index_estimated' in df.columns:
                df['uv_stress'] = (df['uv_index_estimated'] > 8).astype(int)
            
            # Features de tendencia (rolling windows)
            # Optimizacion: vectorizar en lugar de iterar si es posible, pero mantenemos lógica original segura
            for state in df['state'].unique():
                state_mask = df['state'] == state
                state_data = df[state_mask].copy().sort_values('date')
                
                if 'temperature_2m_max' in df.columns:
                    df.loc[state_mask, 'temp_trend_7d'] = state_data['temperature_2m_max'].rolling(window=7, min_periods=1).mean().values
                if 'precipitation_sum' in df.columns:
                    df.loc[state_mask, 'rain_trend_7d'] = state_data['precipitation_sum'].rolling(window=7, min_periods=1).sum().values
            
            logger.info("✅ Datos limpiados y enriquecidos")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error en limpieza: {e}")
            raise

    def calculate_crop_risk_scores(self, df):
        """Calcula scores de riesgo por cultivo"""
        try:
            logger.info("🌾 Calculando scores de riesgo por cultivo...")
            
            for crop_key, config in self.crop_configurations.items():
                # logger.info(f"   Procesando {config['name']}...")
                
                # Score base por factores de riesgo
                risk_score = np.zeros(len(df))
                
                # Componente helada
                if 'temperature_2m_min' in df.columns:
                    frost_component = np.where(
                        df['temperature_2m_min'] <= config["frost_threshold"],
                        config["ml_weight_frost"], 0
                    )
                    risk_score += frost_component
                
                # Componente calor
                if 'temperature_2m_max' in df.columns:
                    heat_component = np.where(
                        df['temperature_2m_max'] >= config["heat_threshold"],
                        config["ml_weight_heat"], 0
                    )
                    risk_score += heat_component
                
                # Componente sequía
                if 'water_deficit' in df.columns:
                    drought_component = np.where(
                        df['water_deficit'] > 3,
                        config["ml_weight_drought"], 0
                    )
                    risk_score += drought_component
                
                # Multiplicador por mes crítico
                critical_months = config["critical_growth_months"]
                critical_multiplier = np.where(
                    df['month'].isin(critical_months), 1.5, 1.0
                )
                
                # Score final
                df[f'{crop_key}_risk_score'] = risk_score
                df[f'{crop_key}_critical_period'] = critical_multiplier
                df[f'{crop_key}_final_risk'] = risk_score * critical_multiplier
                
                # Clasificación de riesgo
                def classify_risk(score):
                    if score <= 1:
                        return "bajo"
                    elif score <= 3:
                        return "medio"
                    elif score <= 6:
                        return "alto"
                    else:
                        return "critico"
                
                df[f'{crop_key}_risk_level'] = df[f'{crop_key}_final_risk'].apply(classify_risk)
            
            logger.info("✅ Scores de riesgo calculados")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error calculando riesgos: {e}")
            raise

    def train_ml_models(self, df):
        """Entrena modelos ML para predicción"""
        try:
            logger.info("🤖 Entrenando modelos ML...")
            
            # Filtrar datos de entrenamiento
            training_data = df[df['dataset_type'] == 'training'].copy()
            
            if len(training_data) < 50:
                logger.warning("⚠️ Pocos datos de entrenamiento")
                return {}
            
            # Features para ML
            feature_columns = [
                'temperature_2m_max', 'temperature_2m_min', 'thermal_amplitude',
                'precipitation_sum', 'et0_fao_evapotranspiration', 
                'water_balance', 'water_deficit', 'shortwave_radiation_sum',
                'growing_degree_days', 'frost_risk', 'heat_stress', 'drought_indicator',
                'month', 'day_of_year', 'quarter'
            ]
            
            # Filtrar features disponibles
            available_features = [col for col in feature_columns if col in training_data.columns]
            
            if len(available_features) < 5:
                logger.warning(f"⚠️ Solo {len(available_features)} features disponibles")
                return {}
            
            # logger.info(f"📊 Features ML: {available_features}")
            
            models = {}
            
            for crop_key, config in self.crop_configurations.items():
                # logger.info(f"   Entrenando modelo {config['name']}...")
                
                target_col = f'{crop_key}_final_risk'
                if target_col not in training_data.columns:
                    logger.warning(f"   ⚠️ Target {target_col} no encontrado")
                    continue
                
                # Preparar datos
                X = training_data[available_features].fillna(0)
                y = training_data[target_col].fillna(0)
                
                # Dividir datos
                if len(X) > 20:
                    X_train, X_test, y_train, y_test = train_test_split(
                        X, y, test_size=0.2, random_state=42
                    )
                    
                    # Entrenar Random Forest
                    rf = RandomForestRegressor(
                        n_estimators=50,
                        max_depth=8,
                        random_state=42,
                        n_jobs=-1
                    )
                    
                    rf.fit(X_train, y_train)
                    
                    # Evaluar
                    y_pred = rf.predict(X_test)
                    mae = mean_absolute_error(y_test, y_pred)
                    
                else:
                    # Entrenar con todos los datos si son pocos
                    rf = RandomForestRegressor(
                        n_estimators=30,
                        max_depth=5,
                        random_state=42
                    )
                    rf.fit(X, y)
                    mae = 0.0
                
                logger.info(f"   ✅ {config['name']} - MAE: {mae:.3f}")
                
                models[crop_key] = {
                    'model': rf,
                    'features': available_features,
                    'mae': mae
                }
            
            logger.info(f"✅ {len(models)} modelos entrenados")
            return models
            
        except Exception as e:
            logger.error(f"❌ Error entrenando ML: {e}")
            return {}

    def apply_ml_predictions(self, df, models):
        """Aplica predicciones ML"""
        try:
            logger.info("🔮 Aplicando predicciones ML...")
            
            if not models:
                logger.warning("⚠️ No hay modelos disponibles")
                return df
            
            # Datos para predicción
            prediction_data = df[df['dataset_type'].isin(['current', 'prediction'])].copy()
            
            if len(prediction_data) == 0:
                logger.warning("⚠️ No hay datos para predicción")
                return df
            
            for crop_key, model_info in models.items():
                # logger.info(f"   Predicciones {self.crop_configurations[crop_key]['name']}...")
                
                # Preparar features
                X_pred = prediction_data[model_info['features']].fillna(0)
                
                # Generar predicciones
                predictions = model_info['model'].predict(X_pred)
                
                # Asignar predicciones
                df.loc[prediction_data.index, f'{crop_key}_ml_prediction'] = predictions
                
                # Clasificar predicciones
                def classify_ml_prediction(score):
                    if score <= 1:
                        return "bajo"
                    elif score <= 3:
                        return "medio"
                    elif score <= 6:
                        return "alto"
                    else:
                        return "critico"
                
                df.loc[prediction_data.index, f'{crop_key}_ml_risk_level'] = [
                    classify_ml_prediction(pred) for pred in predictions
                ]
            
            logger.info("✅ Predicciones ML aplicadas")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error aplicando predicciones: {e}")
            return df

    def create_aggregations(self, df):
        """Crea agregaciones para dashboard"""
        try:
            logger.info("📈 Creando agregaciones...")
            
            # 1. Métricas diarias por estado
            daily_metrics = df.groupby(['state', 'date', 'dataset_type']).agg({
                'temperature_2m_max': 'mean',
                'temperature_2m_min': 'mean', 
                'precipitation_sum': 'sum',
                'water_balance': 'mean',
                'frost_risk': 'max',
                'heat_stress': 'max',
                'growing_degree_days': 'mean'
            }).round(2).reset_index()
            
            # 2. Alertas por cultivo y estado
            prediction_data = df[df['dataset_type'].isin(['current', 'prediction'])]
            
            crop_alerts = {}
            for crop in self.crop_configurations.keys():
                ml_col = f'{crop}_ml_risk_level'
                if ml_col in prediction_data.columns:
                    high_risk = prediction_data[
                        prediction_data[ml_col].isin(['alto', 'critico'])
                    ].groupby('state').size().reset_index(name=f'{crop}_high_risk_days')
                    crop_alerts[crop] = high_risk
            
            # 3. Resumen por región climática
            if 'climate_zone' in df.columns:
                regional_summary = df.groupby(['climate_zone', 'agricultural_season']).agg({
                    'temperature_2m_max': 'mean',
                    'water_balance': 'mean',
                    'frost_risk': 'sum'
                }).round(2).reset_index()
            else:
                regional_summary = pd.DataFrame()
            
            logger.info("✅ Agregaciones creadas")
            
            return {
                'daily_metrics': daily_metrics,
                'crop_alerts': crop_alerts,
                'regional_summary': regional_summary
            }
            
        except Exception as e:
            logger.error(f"❌ Error en agregaciones: {e}")
            return {}

    def prepare_dynamodb_format(self, df, aggregations):
        """Prepara datos para DynamoDB"""
        try:
            logger.info("🗄️ Preparando formato DynamoDB...")
            
            # 1. Tabla principal meteorológica con ML
            weather_data = []
            
            for _, row in df.iterrows():
                record = {
                    'partition_key': row['state'],
                    'sort_key': f"{row['date'].strftime('%Y-%m-%d')}#{row['dataset_type']}",
                    'state': row['state'],
                    'date': row['date'].strftime('%Y-%m-%d'),
                    'dataset_type': row['dataset_type'],
                    'temp_max': round(float(row.get('temperature_2m_max', 0)), 2),
                    'temp_min': round(float(row.get('temperature_2m_min', 0)), 2),
                    'precipitation': round(float(row.get('precipitation_sum', 0)), 2),
                    'water_balance': round(float(row.get('water_balance', 0)), 2),
                    'frost_risk': int(row.get('frost_risk', 0)),
                    'heat_stress': int(row.get('heat_stress', 0)),
                    'drought_indicator': int(row.get('drought_indicator', 0)),
                    'updated_at': datetime.now().isoformat()
                }
                
                # Agregar predicciones ML por cultivo
                for crop in self.crop_configurations.keys():
                    ml_pred_col = f'{crop}_ml_prediction'
                    ml_level_col = f'{crop}_ml_risk_level'
                    
                    if ml_pred_col in row:
                        record[f'{crop}_ml_risk'] = round(float(row.get(ml_pred_col, 0)), 3)
                    if ml_level_col in row:
                        record[f'{crop}_risk_level'] = row.get(ml_level_col, 'bajo')
                
                weather_data.append(record)
            
            # 2. Alertas ML
            alerts_data = []
            if 'crop_alerts' in aggregations:
                for crop, alerts_df in aggregations['crop_alerts'].items():
                    for _, row in alerts_df.iterrows():
                        alerts_data.append({
                            'partition_key': row['state'],
                            'sort_key': f'ML_ALERTS#{crop.upper()}',
                            'state': row['state'],
                            'crop': crop,
                            'high_risk_days': int(row[f'{crop}_high_risk_days']),
                            'generated_at': datetime.now().isoformat()
                        })
            
            # 3. Métricas diarias
            metrics_data = []
            if 'daily_metrics' in aggregations:
                for _, row in aggregations['daily_metrics'].iterrows():
                    metrics_data.append({
                        'partition_key': f"{row['state']}#{row['date'].strftime('%Y-%m-%d')}",
                        'sort_key': row['dataset_type'],
                        'state': row['state'],
                        'date': row['date'].strftime('%Y-%m-%d'),
                        'avg_temp_max': round(float(row['temperature_2m_max']), 2),
                        'avg_temp_min': round(float(row['temperature_2m_min']), 2),
                        'total_precip': round(float(row['precipitation_sum']), 2),
                        'frost_events': int(row['frost_risk']),
                        'heat_events': int(row['heat_stress']),
                        'computed_at': datetime.now().isoformat()
                    })
            
            logger.info("✅ Formato DynamoDB preparado")
            
            return {
                'weather_data': weather_data,
                'ml_alerts': alerts_data,
                'daily_metrics': metrics_data
            }
            
        except Exception as e:
            logger.error(f"❌ Error preparando DynamoDB: {e}")
            return {}

    def save_processed_data(self, df, aggregations, dynamo_tables):
        """Guarda todos los datos procesados"""
        try:
            logger.info("💾 Guardando datos procesados...")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M')
            
            # 1. Dataset principal con ML
            main_filename = f"enhanced_weather_ml_{timestamp}.csv"
            df.to_csv(main_filename, index=False)
            
            # 2. Datos para DynamoDB (JSON)
            dynamo_base = f"dynamo_ml_ready_{timestamp}"
            
            if dynamo_tables.get('weather_data'):
                with open(f"{dynamo_base}_weather.json", 'w') as f:
                    json.dump(dynamo_tables['weather_data'], f, indent=2)
                    
            if dynamo_tables.get('ml_alerts'):
                with open(f"{dynamo_base}_alerts.json", 'w') as f:
                    json.dump(dynamo_tables['ml_alerts'], f, indent=2)
                    
            if dynamo_tables.get('daily_metrics'):
                with open(f"{dynamo_base}_metrics.json", 'w') as f:
                    json.dump(dynamo_tables['daily_metrics'], f, indent=2)
            
            # 3. Agregaciones
            if 'daily_metrics' in aggregations:
                aggregations['daily_metrics'].to_csv(f"aggregations_{timestamp}_daily.csv", index=False)
            
            logger.info(f"✅ Archivos guardados con timestamp {timestamp}")
            
            return {
                'main_dataset': main_filename,
                'dynamo_weather': f"{dynamo_base}_weather.json",
                'dynamo_alerts': f"{dynamo_base}_alerts.json",
                'dynamo_metrics': f"{dynamo_base}_metrics.json"
            }
            
        except Exception as e:
            logger.error(f"❌ Error guardando: {e}")
            return {}

    def generate_ml_report(self, df, models, aggregations):
        """Genera reporte ML"""
        try:
            print("\n" + "="*80)
            print("🤖 REPORTE MACHINE LEARNING - PROCESAMIENTO LOCAL")
            print("="*80)
            
            # Estadísticas generales
            total_records = len(df)
            training_records = len(df[df['dataset_type'] == 'training'])
            prediction_records = len(df[df['dataset_type'].isin(['current', 'prediction'])])
            
            print(f"\n📊 DATOS PROCESADOS:")
            print(f"   Total registros: {total_records:,}")
            print(f"   Datos entrenamiento: {training_records:,}")
            print(f"   Datos predicción: {prediction_records:,}")
            print(f"   Estados: {df['state'].nunique()}")
            
            # Rendimiento de modelos
            if models:
                print(f"\n🎯 MODELOS ML ENTRENADOS:")
                for crop_key, model_info in models.items():
                    crop_name = self.crop_configurations[crop_key]['name']
                    mae = model_info.get('mae', 0)
                    features_count = len(model_info.get('features', []))
                    status = '✅ Operativo' if mae < 2.0 else '⚠️ Revisar'
                    print(f"   {crop_name}: MAE={mae:.3f}, Features={features_count}, {status}")
            
            # Distribución de alertas
            prediction_data = df[df['dataset_type'].isin(['current', 'prediction'])]
            if len(prediction_data) > 0:
                print(f"\n⚠️ DISTRIBUCIÓN ALERTAS ML:")
                for crop_key in self.crop_configurations.keys():
                    ml_col = f'{crop_key}_ml_risk_level'
                    if ml_col in prediction_data.columns:
                        risk_counts = prediction_data[ml_col].value_counts()
                        crop_name = self.crop_configurations[crop_key]['name']
                        print(f"   {crop_name}:")
                        for level, count in risk_counts.items():
                            if level:
                                print(f"      - {level.capitalize()}: {count} días")
            
            # Por estado
            print(f"\n🗺️ RESUMEN POR ESTADO:")
            for state in df['state'].unique():
                state_data = prediction_data[prediction_data['state'] == state]
                if len(state_data) > 0:
                    frost_days = state_data.get('frost_risk', pd.Series([0])).sum()
                    heat_days = state_data.get('heat_stress', pd.Series([0])).sum()
                    print(f"   {state}: {len(state_data)} días, "
                          f"Heladas: {frost_days}, Calor: {heat_days}")
            
            print("="*80)
            
        except Exception as e:
            logger.error(f"❌ Error en reporte: {e}")

    def run_complete_pipeline(self, data_path):
        """Pipeline completo sin PySpark"""
        try:
            logger.info("🚀 PIPELINE ML LOCAL - SIN PYSPARK")
            logger.info("="*50)
            
            # 1. Cargar datos
            logger.info("📊 FASE 1: CARGA")
            df = self.load_hybrid_data(data_path)
            
            # 2. Limpiar y enriquecer
            logger.info("🧹 FASE 2: LIMPIEZA")
            df = self.clean_and_enhance_data(df)
            
            # 3. Calcular riesgos
            logger.info("🌾 FASE 3: RIESGOS")
            df = self.calculate_crop_risk_scores(df)
            
            # 4. Entrenar ML
            logger.info("🤖 FASE 4: MACHINE LEARNING")
            models = self.train_ml_models(df)
            
            # 5. Predicciones
            if models:
                logger.info("🔮 FASE 5: PREDICCIONES")
                df = self.apply_ml_predictions(df, models)
            
            # 6. Agregaciones
            logger.info("📈 FASE 6: AGREGACIONES")
            aggregations = self.create_aggregations(df)
            
            # 7. DynamoDB
            logger.info("🗄️ FASE 7: DYNAMODB")
            dynamo_tables = self.prepare_dynamodb_format(df, aggregations)
            
            # 8. Guardar
            logger.info("💾 FASE 8: GUARDADO")
            saved_files = self.save_processed_data(df, aggregations, dynamo_tables)
            
            # 9. Reporte
            self.generate_ml_report(df, models, aggregations)
            
            logger.info(f"\n🎉 PIPELINE LOCAL COMPLETADO!")
            
            return {
                'processed_data': df,
                'ml_models': models,
                'aggregations': aggregations,
                'dynamo_tables': dynamo_tables,
                'saved_files': saved_files
            }
            
        except Exception as e:
            logger.error(f"❌ Error en pipeline: {e}")
            raise

def main():
    """Función principal del procesador local"""
    print("🚀 PROCESADOR ML LOCAL - SIN PROBLEMAS PYSPARK")
    print("="*60)
    print("🎯 VENTAJAS:")
    print("   • Sin dependencias de Hadoop/HDFS")
    print("   • Funciona en cualquier máquina")
    print("   • Pandas + Scikit-learn = Rápido y confiable")
    print("   • Genera JSONs listos para AWS DynamoDB")
    print("="*60)
    
    # Buscar archivos híbridos en el directorio actual
    hybrid_files = glob.glob("corrected_hybrid_full_*.csv")
    
    if not hybrid_files:
        print("\n❌ ERROR: No se encontraron archivos CSV de entrada.")
        print("🔧 Asegúrate de tener un archivo llamado 'corrected_hybrid_full_YYYYMMDD.csv' en esta carpeta.")
        return
    
    # Usar el archivo más reciente
    # Asume formato corrected_hybrid_full_YYYYMMDD.csv o similar
    try:
        latest_file = max(hybrid_files, key=os.path.getctime)
        print(f"\n📊 Archivo de datos detectado: {latest_file}")
    except Exception as e:
        print(f"Error seleccionando archivo: {e}")
        latest_file = hybrid_files[0]
    
    try:
        # Inicializar procesador local
        processor = LocalMLWeatherProcessor()
        
        # Ejecutar pipeline completo
        results = processor.run_complete_pipeline(latest_file)
        
        if results:
            print(f"\n✅ PROCESAMIENTO ML LOCAL EXITOSO!")
            
            files = results.get('saved_files', {})
            print(f"💾 Archivos generados: {len(files)}")
            for key, path in files.items():
                print(f"   - {key}: {path}")
                
    except Exception as e:
        print(f"\n❌ ERROR FATAL EN PROCESAMIENTO: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
