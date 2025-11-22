import os
import sys
import logging
from datetime import datetime, timedelta
import json

# Configurar variables de entorno para modo local
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

try:
    import findspark
    findspark.init()
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, lit, when, round as spark_round, date_format, 
        year, month, dayofmonth, avg, max as spark_max, 
        min as spark_min, sum as spark_sum, count, desc,
        regexp_replace, split, concat_ws, current_timestamp
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, 
        IntegerType, BooleanType, TimestampType
    )
except ImportError as e:
    print(f"Error importando PySpark: {e}")
    print("Ejecuta: pip install pyspark findspark")
    sys.exit(1)

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CompleteWeatherProcessor:
    def __init__(self):
        self.spark = self._initialize_local_spark()
        
        # Cultivos regionales aplicables a todo México
        self.regional_crops = {
            "maiz": {
                "name": "Maíz",
                "frost_threshold": 0,      # °C - Crítico
                "heat_threshold": 35,      # °C - Estrés térmico
                "drought_days": 15,        # días sin lluvia
                "min_radiation": 15,       # MJ/m² - mínimo para crecimiento
                "optimal_temp_min": 15,    # °C
                "optimal_temp_max": 30     # °C
            },
            "frijol": {
                "name": "Frijol",
                "frost_threshold": 2,      # °C - Más sensible que maíz
                "heat_threshold": 32,      # °C
                "drought_days": 10,        # días sin lluvia
                "min_radiation": 12,       # MJ/m²
                "optimal_temp_min": 18,
                "optimal_temp_max": 28
            },
            "chile": {
                "name": "Chile",
                "frost_threshold": 4,      # °C - Muy sensible
                "heat_threshold": 38,      # °C - Resistente al calor
                "drought_days": 7,         # días sin lluvia
                "min_radiation": 18,       # MJ/m² - Necesita mucho sol
                "optimal_temp_min": 20,
                "optimal_temp_max": 35
            },
            "trigo": {
                "name": "Trigo",
                "frost_threshold": -2,     # °C - Resistente al frío
                "heat_threshold": 30,      # °C
                "drought_days": 20,        # días sin lluvia
                "min_radiation": 10,       # MJ/m²
                "optimal_temp_min": 10,
                "optimal_temp_max": 25
            }
        }

    def get_all_mexican_states(self):
        """Retornar todos los 32 estados mexicanos"""
        return {
            "Aguascalientes": {"lat": 21.8853, "lon": -102.2916, "region": "Bajio"},
            "Baja California": {"lat": 32.6519, "lon": -115.4683, "region": "Norte_Arido"},
            "Baja California Sur": {"lat": 24.1444, "lon": -110.3128, "region": "Norte_Arido"},
            "Campeche": {"lat": 19.8414, "lon": -90.5328, "region": "Sureste_Tropical"},
            "Chiapas": {"lat": 16.7569, "lon": -93.1292, "region": "Sur_Tropical"},
            "Chihuahua": {"lat": 28.6353, "lon": -106.0889, "region": "Norte_Desertico"},
            "Ciudad de Mexico": {"lat": 19.4326, "lon": -99.1332, "region": "Centro_Templado"},
            "Coahuila": {"lat": 25.4232, "lon": -101.0053, "region": "Norte"},
            "Colima": {"lat": 19.2433, "lon": -103.7244, "region": "Pacifico"},
            "Durango": {"lat": 24.0277, "lon": -104.6532, "region": "Norte"},
            "Estado de Mexico": {"lat": 19.3569, "lon": -99.6561, "region": "Centro_Templado"},
            "Guanajuato": {"lat": 21.0190, "lon": -101.2574, "region": "Bajio_Agricola"},
            "Guerrero": {"lat": 17.4392, "lon": -99.5451, "region": "Sur_Calido"},
            "Hidalgo": {"lat": 20.0910, "lon": -98.7624, "region": "Centro"},
            "Jalisco": {"lat": 20.6597, "lon": -103.3496, "region": "Occidente"},
            "Michoacan": {"lat": 19.5665, "lon": -101.7068, "region": "Occidente"},
            "Morelos": {"lat": 18.6813, "lon": -99.1013, "region": "Centro"},
            "Nayarit": {"lat": 21.7514, "lon": -104.8455, "region": "Pacifico"},
            "Nuevo Leon": {"lat": 25.5922, "lon": -99.9962, "region": "Noreste"},
            "Oaxaca": {"lat": 17.0732, "lon": -96.7266, "region": "Sur_Montanoso"},
            "Puebla": {"lat": 19.0414, "lon": -98.2063, "region": "Centro"},
            "Queretaro": {"lat": 20.5888, "lon": -100.3899, "region": "Bajio_Templado"},
            "Quintana Roo": {"lat": 19.1817, "lon": -88.4791, "region": "Caribe"},
            "San Luis Potosi": {"lat": 22.1565, "lon": -100.9855, "region": "Centro_Norte"},
            "Sinaloa": {"lat": 24.8069, "lon": -107.3940, "region": "Pacifico_Intensivo"},
            "Sonora": {"lat": 29.0729, "lon": -110.9559, "region": "Noroeste_Arido"},
            "Tabasco": {"lat": 17.8409, "lon": -92.6189, "region": "Golfo_Humedo"},
            "Tamaulipas": {"lat": 24.2669, "lon": -98.8363, "region": "Golfo_Norte"},
            "Tlaxcala": {"lat": 19.3139, "lon": -98.2404, "region": "Centro"},
            "Veracruz": {"lat": 19.1738, "lon": -96.1342, "region": "Golfo_Centro"},
            "Yucatan": {"lat": 20.7099, "lon": -89.0943, "region": "Peninsula"},
            "Zacatecas": {"lat": 22.7709, "lon": -102.5832, "region": "Norte_Centro"}
        }

    def _initialize_local_spark(self):
        """Inicializar Spark en modo local sin Hadoop"""
        try:
            # Configuración específica para modo local
            spark = SparkSession.builder \
                .appName("CompleteWeatherProcessor") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
                .config("spark.hadoop.fs.defaultFS", "file:///") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            logger.info("✅ Spark Local Session inicializada correctamente")
            return spark
            
        except Exception as e:
            logger.error(f"❌ Error inicializando Spark Local: {e}")
            raise

    def load_weather_data_local(self):
        """Cargar datos meteorológicos desde archivos locales (32 estados)"""
        try:
            logger.info("📊 Cargando datos meteorológicos para 32 estados...")
            
            # Usar rutas absolutas para evitar problemas
            import os
            current_dir = os.getcwd()
            
            # Cargar datos de TODOS los estados (archivo generado por full_states_extractor.py)
            daily_path = os.path.join(current_dir, "mexico_all_states_weather_20251119.csv")
            if not os.path.exists(daily_path):
                # Fallback al archivo de 6 estados si el de 32 no existe
                daily_path = os.path.join(current_dir, "mexico_reliable_daily_20251119.csv")
                logger.warning("⚠️ Usando archivo de 6 estados como fallback")
                
            if not os.path.exists(daily_path):
                raise FileNotFoundError(f"No se encuentra archivo de datos: {daily_path}")
                
            daily_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(f"file://{daily_path}")
            
            # Cargar datos horarios (opcional)
            hourly_path = os.path.join(current_dir, "mexico_reliable_hourly_20251119.csv")
            hourly_df = None
            if os.path.exists(hourly_path):
                hourly_df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(f"file://{hourly_path}")
                logger.info(f"🕐 Datos horarios: {hourly_df.count()} registros")
            
            # Cargar datos históricos si existe
            historical_path = os.path.join(current_dir, "mexico_historical_weather_2020_2024_20251119.csv")
            historical_df = None
            if os.path.exists(historical_path):
                historical_df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(f"file://{historical_path}")
                logger.info(f"📅 Datos históricos: {historical_df.count()} registros")
            
            logger.info(f"📊 Datos diarios cargados: {daily_df.count()} registros")
            logger.info(f"🏛️  Estados únicos: {daily_df.select('state').distinct().count()}")
            logger.info("✅ Archivos cargados exitosamente")
            
            return daily_df, hourly_df, historical_df
            
        except Exception as e:
            logger.error(f"❌ Error cargando datos: {e}")
            raise

    def process_temporal_data(self, df):
        """Procesar y enriquecer datos temporales"""
        try:
            logger.info("🕐 Procesando datos temporales...")
            
            # Procesar duración de luz solar
            processed_df = df.withColumn(
                "daylight_hours",
                when(col("daylight_duration").isNotNull(),
                     spark_round(col("daylight_duration") / 3600, 2)
                ).otherwise(0)
            )
            
            # Agregar variables temporales usando columna 'date'
            if "date" in df.columns:
                processed_df = processed_df.withColumn("year", year(col("date"))) \
                    .withColumn("month", month(col("date"))) \
                    .withColumn("day", dayofmonth(col("date"))) \
                    .withColumn("season",
                        when(col("month").isin([12, 1, 2]), "invierno")
                        .when(col("month").isin([3, 4, 5]), "primavera")  
                        .when(col("month").isin([6, 7, 8]), "verano")
                        .otherwise("otono")
                    )
            
            logger.info("✅ Datos temporales procesados")
            return processed_df
            
        except Exception as e:
            logger.error(f"❌ Error procesando datos temporales: {e}")
            # Devolver DataFrame original si hay error
            return df

    def generate_crop_alerts(self, df):
        """Generar alertas específicas por cultivo para todos los estados"""
        try:
            logger.info("⚠️ Generando alertas por cultivo para todos los estados...")
            
            alerts_list = []
            
            # Obtener estados únicos
            states = [row["state"] for row in df.select("state").distinct().collect()]
            logger.info(f"📍 Procesando alertas para {len(states)} estados")
            
            for state in states:
                state_data = df.filter(col("state") == state)
                
                for crop_key, crop_config in self.regional_crops.items():
                    
                    # Contar días con diferentes tipos de riesgo
                    frost_count = state_data.filter(
                        col("temperature_2m_min") <= crop_config["frost_threshold"]
                    ).count()
                    
                    heat_count = state_data.filter(
                        col("temperature_2m_max") >= crop_config["heat_threshold"]
                    ).count()
                    
                    low_radiation_count = state_data.filter(
                        col("shortwave_radiation_sum") < crop_config["min_radiation"]
                    ).count()
                    
                    # Generar alertas si hay riesgos
                    if frost_count > 0:
                        alerts_list.append({
                            "alert_id": f"{state}_{crop_key}_frost_{datetime.now().strftime('%Y%m%d')}",
                            "state": state,
                            "crop": crop_config["name"],
                            "alert_type": "frost_risk",
                            "severity": "critical",
                            "message": f"Riesgo de helada para {crop_config['name']} en {state}",
                            "affected_days": frost_count,
                            "threshold_value": crop_config["frost_threshold"],
                            "created_at": datetime.now().isoformat()
                        })
                    
                    if heat_count > 0:
                        alerts_list.append({
                            "alert_id": f"{state}_{crop_key}_heat_{datetime.now().strftime('%Y%m%d')}",
                            "state": state,
                            "crop": crop_config["name"],
                            "alert_type": "heat_stress",
                            "severity": "warning",
                            "message": f"Estrés térmico para {crop_config['name']} en {state}",
                            "affected_days": heat_count,
                            "threshold_value": crop_config["heat_threshold"],
                            "created_at": datetime.now().isoformat()
                        })
                    
                    if low_radiation_count > 0:
                        alerts_list.append({
                            "alert_id": f"{state}_{crop_key}_radiation_{datetime.now().strftime('%Y%m%d')}",
                            "state": state,
                            "crop": crop_config["name"],
                            "alert_type": "low_radiation",
                            "severity": "warning",
                            "message": f"Radiación solar baja para {crop_config['name']} en {state}",
                            "affected_days": low_radiation_count,
                            "threshold_value": crop_config["min_radiation"],
                            "created_at": datetime.now().isoformat()
                        })
            
            # Convertir a DataFrame si hay alertas
            if alerts_list:
                alerts_df = self.spark.createDataFrame(alerts_list)
                logger.info(f"⚠️ Generadas {len(alerts_list)} alertas para {len(states)} estados")
                return alerts_df
            else:
                logger.info("ℹ️ No se generaron alertas críticas")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error generando alertas: {e}")
            return None

    def create_aggregated_metrics(self, df):
        """Crear métricas agregadas para todos los estados"""
        try:
            logger.info("📈 Creando métricas agregadas para todos los estados...")
            
            # Verificar que tenemos las columnas necesarias
            required_cols = ["state", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"]
            available_cols = df.columns
            
            for col_name in required_cols:
                if col_name not in available_cols:
                    logger.warning(f"Columna faltante: {col_name}")
                    return None, None
            
            # Métricas por estado
            monthly_metrics = df.groupBy("state") \
                .agg(
                    avg("temperature_2m_max").alias("avg_temp_max"),
                    avg("temperature_2m_min").alias("avg_temp_min"),
                    spark_sum("precipitation_sum").alias("total_precipitation"),
                    avg("shortwave_radiation_sum").alias("avg_radiation"),
                    count("*").alias("total_days")
                ) \
                .withColumn("processed_at", current_timestamp())
            
            # Métricas por región si existe la columna
            regional_metrics = None
            if "region" in available_cols:
                regional_metrics = df.groupBy("region") \
                    .agg(
                        avg("temperature_2m_max").alias("regional_avg_temp_max"),
                        avg("temperature_2m_min").alias("regional_avg_temp_min"),
                        spark_sum("precipitation_sum").alias("regional_precipitation"),
                        count("*").alias("days_count")
                    )
            
            logger.info("✅ Métricas agregadas creadas")
            return monthly_metrics, regional_metrics
            
        except Exception as e:
            logger.error(f"❌ Error creando métricas: {e}")
            return None, None

    def prepare_dynamodb_format(self, df, table_type):
        """Preparar datos para formato DynamoDB"""
        try:
            logger.info(f"📦 Preparando datos para DynamoDB - {table_type}...")
            
            if table_type == "weather_data":
                # Formato para tabla principal
                base_cols = ["state", "date", "temperature_2m_max", "temperature_2m_min", "precipitation_sum"]
                available_cols = [col for col in base_cols if col in df.columns]
                
                dynamodb_df = df.select(*available_cols) \
                    .withColumn("partition_key", concat_ws("-", col("state"), col("date"))) \
                    .withColumn("sort_key", col("date")) \
                    .withColumn("updated_at", current_timestamp())
                
            elif table_type == "monthly_metrics":
                # Formato para métricas agregadas
                dynamodb_df = df.withColumn("partition_key", col("state")) \
                    .withColumn("sort_key", lit("monthly_summary")) \
                    .withColumn("updated_at", current_timestamp())
            
            logger.info(f"✅ Datos preparados para DynamoDB - {table_type}")
            return dynamodb_df
            
        except Exception as e:
            logger.error(f"❌ Error preparando datos DynamoDB: {e}")
            return None

    def save_processed_data(self, df, filename):
        """Guardar datos procesados como JSON"""
        try:
            if df is not None:
                output_dir = f"processed_{filename}_32states"
                df.coalesce(1).write.mode("overwrite") \
                    .option("header", "true") \
                    .json(output_dir)
                logger.info(f"💾 Datos guardados: {output_dir}")
                return True
            return False
                
        except Exception as e:
            logger.error(f"❌ Error guardando {filename}: {e}")
            return False

    def run_complete_pipeline(self):
        """Ejecutar pipeline completo para 32 estados"""
        try:
            logger.info("🚀 Iniciando pipeline COMPLETO para 32 estados mexicanos...")
            
            # 1. Cargar datos
            daily_df, hourly_df, historical_df = self.load_weather_data_local()
            
            # 2. Mostrar información de cobertura
            states_count = daily_df.select("state").distinct().count()
            total_records = daily_df.count()
            
            logger.info("\n📊 COBERTURA DE DATOS:")
            logger.info(f"   Estados procesados: {states_count}")
            logger.info(f"   Total registros: {total_records}")
            logger.info(f"   Promedio por estado: {total_records // states_count if states_count > 0 else 0}")
            
            # Mostrar lista de estados
            states_list = [row["state"] for row in daily_df.select("state").distinct().collect()]
            logger.info(f"   Estados incluidos: {', '.join(sorted(states_list))}")
            
            # 3. Procesar datos temporales
            processed_daily = self.process_temporal_data(daily_df)
            
            # 4. Generar alertas por cultivo
            alerts_df = self.generate_crop_alerts(processed_daily)
            if alerts_df:
                alert_count = alerts_df.count()
                logger.info(f"\n⚠️ SISTEMA DE ALERTAS:")
                logger.info(f"   Total alertas generadas: {alert_count}")
                
                # Mostrar distribución de alertas
                alert_summary = alerts_df.groupBy("alert_type", "severity").count().collect()
                for row in alert_summary:
                    logger.info(f"   {row['alert_type']} ({row['severity']}): {row['count']}")
            
            # 5. Crear métricas agregadas
            monthly_metrics, regional_metrics = self.create_aggregated_metrics(processed_daily)
            
            if monthly_metrics:
                metrics_count = monthly_metrics.count()
                logger.info(f"\n📈 MÉTRICAS AGREGADAS:")
                logger.info(f"   Métricas por estado: {metrics_count}")
                
                # Mostrar top 5 estados más calientes
                logger.info("\n🌡️ TOP 5 ESTADOS MÁS CALIENTES:")
                hot_states = monthly_metrics.orderBy(col("avg_temp_max").desc()).limit(5)
                for row in hot_states.collect():
                    logger.info(f"   {row['state']}: {row['avg_temp_max']:.1f}°C promedio")
            
            # 6. Preparar datos para DynamoDB
            weather_dynamo = self.prepare_dynamodb_format(processed_daily, "weather_data")
            
            if monthly_metrics:
                metrics_dynamo = self.prepare_dynamodb_format(monthly_metrics, "monthly_metrics")
            else:
                metrics_dynamo = None
            
            # 7. Guardar datos procesados
            saved_files = []
            if weather_dynamo and self.save_processed_data(weather_dynamo, "weather_data_dynamo"):
                saved_files.append("weather_data_dynamo_32states")
            
            if metrics_dynamo and self.save_processed_data(metrics_dynamo, "monthly_metrics_dynamo"):
                saved_files.append("monthly_metrics_dynamo_32states")
            
            if alerts_df and self.save_processed_data(alerts_df, "alerts_dynamo"):
                saved_files.append("alerts_dynamo_32states")
            
            # 8. Estadísticas finales
            logger.info("\n📊 RESUMEN FINAL DEL PROCESAMIENTO:")
            logger.info(f"   🏛️  Estados procesados: {states_count}")
            logger.info(f"   📅 Registros totales: {total_records}")
            if monthly_metrics:
                logger.info(f"   📈 Métricas por estado: {monthly_metrics.count()}")
            if alerts_df:
                logger.info(f"   ⚠️ Alertas generadas: {alerts_df.count()}")
            logger.info(f"   💾 Archivos guardados: {len(saved_files)}")
            
            logger.info("🎉 Pipeline COMPLETO para 32 estados completado exitosamente!")
            
            return {
                "processed_daily": processed_daily,
                "monthly_metrics": monthly_metrics,
                "alerts": alerts_df,
                "saved_files": saved_files,
                "states_processed": states_count,
                "total_records": total_records
            }
            
        except Exception as e:
            logger.error(f"❌ Error en pipeline: {e}")
            raise
        finally:
            self.spark.stop()

def main():
    """Función principal"""
    print("🔥 PROCESADOR PYSPARK COMPLETO - 32 ESTADOS MEXICANOS")
    print("="*70)
    print("🇲🇽 Cobertura: TODOS los estados de México")
    print("🌽 Cultivos: Maíz, Frijol, Chile, Trigo")
    print("⚠️ Alertas: Sistema automático por cultivo/estado")
    print("📦 Output: Datos listos para DynamoDB")
    print("="*70)
    
    try:
        processor = CompleteWeatherProcessor()
        results = processor.run_complete_pipeline()
        
        print("\n🎉 PROCESAMIENTO COMPLETO EXITOSO!")
        print(f"🏛️  Estados procesados: {results['states_processed']}")
        print(f"📊 Registros totales: {results['total_records']}")
        print(f"📁 Archivos generados: {results['saved_files']}")
        print(f"📦 Estado: Listos para integración DynamoDB")
        
        if results['states_processed'] == 32:
            print("\n✅ COBERTURA COMPLETA DE MÉXICO ALCANZADA!")
        else:
            print(f"\n⚠️ Usando {results['states_processed']} estados (archivo base disponible)")
        
    except Exception as e:
        print(f"\n❌ ERROR EN PROCESAMIENTO: {e}")
        logger.error(f"Error crítico: {e}")

if __name__ == "__main__":
    main()
