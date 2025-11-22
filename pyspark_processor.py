import os
import sys
import logging
from datetime import datetime, timedelta
import json

# Configurar PySpark
os.environ['JAVA_HOME'] = '/usr/lib/jvm/oracle_jdk11'
os.environ['SPARK_HOME'] = '/opt/spark'  # Se configurará automáticamente

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

class WeatherDataProcessor:
    def __init__(self):
        self.spark = self._initialize_spark()
        
        # Cultivos regionales (Querétaro/Guanajuato)
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

    def _initialize_spark(self):
        """Inicializar Spark Session con configuración optimizada"""
        try:
            spark = SparkSession.builder \
                .appName("WeatherDataProcessor") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            logger.info("✅ Spark Session inicializada correctamente")
            return spark
            
        except Exception as e:
            logger.error(f"❌ Error inicializando Spark: {e}")
            raise

    def load_weather_data(self):
        """Cargar datos meteorológicos desde CSVs"""
        try:
            logger.info("📊 Cargando datos meteorológicos...")
            
            # Cargar datos diarios
            daily_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv("mexico_reliable_daily_20251119.csv")
            
            # Cargar datos horarios
            hourly_df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv("mexico_reliable_hourly_20251119.csv")
            
            # Cargar datos históricos (si existe)
            historical_df = None
            if os.path.exists("mexico_historical_weather_2020_2024_20251119.csv"):
                historical_df = self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv("mexico_historical_weather_2020_2024_20251119.csv")
                logger.info(f"📅 Datos históricos cargados: {historical_df.count()} registros")
            
            logger.info(f"📊 Datos diarios: {daily_df.count()} registros")
            logger.info(f"🕐 Datos horarios: {hourly_df.count()} registros")
            
            return daily_df, hourly_df, historical_df
            
        except Exception as e:
            logger.error(f"❌ Error cargando datos: {e}")
            raise

    def process_temporal_data(self, df):
        """Procesar y enriquecer datos temporales"""
        try:
            logger.info("🕐 Procesando datos temporales...")
            
            # Convertir formato de tiempo
            processed_df = df.withColumn(
                "sunrise_time", 
                date_format(
                    (col("sunrise_hour") * 3600).cast("timestamp"), 
                    "HH:mm"
                )
            ).withColumn(
                "sunset_time",
                date_format(
                    (col("sunset_hour") * 3600).cast("timestamp"),
                    "HH:mm"  
                )
            ).withColumn(
                "daylight_hours",
                spark_round(col("daylight_duration") / 3600, 2)
            )
            
            # Agregar variables temporales
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
            raise

    def generate_crop_alerts(self, df):
        """Generar alertas específicas por cultivo"""
        try:
            logger.info("⚠️ Generando alertas por cultivo...")
            
            alerts_data = []
            
            # Procesar alertas para cada estado y cultivo
            for state_row in df.select("state").distinct().collect():
                state = state_row["state"]
                state_data = df.filter(col("state") == state)
                
                for crop_key, crop_config in self.regional_crops.items():
                    
                    # Alertas de helada
                    frost_alerts = state_data.filter(
                        col("temperature_2m_min") <= crop_config["frost_threshold"]
                    )
                    
                    # Alertas de calor extremo
                    heat_alerts = state_data.filter(
                        col("temperature_2m_max") >= crop_config["heat_threshold"]
                    )
                    
                    # Alertas de sequía (días consecutivos sin lluvia)
                    drought_alerts = state_data.filter(
                        col("is_dry_day") == 1
                    )
                    
                    # Alertas de radiación baja
                    radiation_alerts = state_data.filter(
                        col("shortwave_radiation_sum") < crop_config["min_radiation"]
                    )
                    
                    # Crear registros de alertas
                    if frost_alerts.count() > 0:
                        alerts_data.append({
                            "alert_id": f"{state}_{crop_key}_frost_{datetime.now().strftime('%Y%m%d')}",
                            "state": state,
                            "crop": crop_config["name"],
                            "alert_type": "frost_risk",
                            "severity": "critical",
                            "message": f"Riesgo de helada para {crop_config['name']} en {state}",
                            "affected_days": frost_alerts.count(),
                            "threshold_value": crop_config["frost_threshold"],
                            "created_at": datetime.now().isoformat()
                        })
                    
                    if heat_alerts.count() > 0:
                        alerts_data.append({
                            "alert_id": f"{state}_{crop_key}_heat_{datetime.now().strftime('%Y%m%d')}",
                            "state": state,
                            "crop": crop_config["name"],
                            "alert_type": "heat_stress",
                            "severity": "warning",
                            "message": f"Estrés térmico para {crop_config['name']} en {state}",
                            "affected_days": heat_alerts.count(),
                            "threshold_value": crop_config["heat_threshold"],
                            "created_at": datetime.now().isoformat()
                        })
            
            # Convertir a DataFrame
            if alerts_data:
                alerts_df = self.spark.createDataFrame(alerts_data)
                logger.info(f"⚠️ Generadas {len(alerts_data)} alertas")
                return alerts_df
            else:
                logger.info("ℹ️ No se generaron alertas críticas")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error generando alertas: {e}")
            raise

    def create_aggregated_metrics(self, df):
        """Crear métricas agregadas para dashboard"""
        try:
            logger.info("📈 Creando métricas agregadas...")
            
            # Métricas por estado y mes
            monthly_metrics = df.groupBy("state", "year", "month", "region") \
                .agg(
                    avg("temperature_2m_max").alias("avg_temp_max"),
                    avg("temperature_2m_min").alias("avg_temp_min"),
                    spark_sum("precipitation_sum").alias("total_precipitation"),
                    avg("shortwave_radiation_sum").alias("avg_radiation"),
                    spark_sum("is_dry_day").alias("dry_days_count"),
                    spark_sum("heat_stress_risk").alias("heat_stress_days"),
                    avg("daylight_hours").alias("avg_daylight_hours")
                ) \
                .withColumn("processed_at", current_timestamp())
            
            # Métricas por región climática
            regional_metrics = df.groupBy("region", "season") \
                .agg(
                    avg("temperature_2m_max").alias("seasonal_avg_temp_max"),
                    avg("temperature_2m_min").alias("seasonal_avg_temp_min"),
                    spark_sum("precipitation_sum").alias("seasonal_precipitation"),
                    count("*").alias("days_count")
                )
            
            logger.info("✅ Métricas agregadas creadas")
            return monthly_metrics, regional_metrics
            
        except Exception as e:
            logger.error(f"❌ Error creando métricas: {e}")
            raise

    def prepare_dynamodb_format(self, df, table_type):
        """Preparar datos para DynamoDB"""
        try:
            logger.info(f"📦 Preparando datos para DynamoDB - {table_type}...")
            
            if table_type == "weather_data":
                # Formato para tabla principal de datos meteorológicos
                dynamodb_df = df.select(
                    concat_ws("-", col("state"), col("date")).alias("partition_key"),
                    col("date").alias("sort_key"),
                    col("state"),
                    col("region"),
                    spark_round(col("temperature_2m_max"), 2).alias("temp_max"),
                    spark_round(col("temperature_2m_min"), 2).alias("temp_min"),
                    spark_round(col("precipitation_sum"), 2).alias("precipitation"),
                    spark_round(col("shortwave_radiation_sum"), 2).alias("radiation"),
                    col("daylight_hours"),
                    col("sunrise_time"),
                    col("sunset_time"),
                    col("season"),
                    col("agricultural_season"),
                    current_timestamp().alias("updated_at")
                )
                
            elif table_type == "monthly_metrics":
                # Formato para métricas agregadas mensuales
                dynamodb_df = df.select(
                    concat_ws("-", col("state"), col("year"), col("month")).alias("partition_key"),
                    col("month").alias("sort_key"),
                    col("state"),
                    col("region"),
                    col("year"),
                    spark_round(col("avg_temp_max"), 2).alias("avg_temp_max"),
                    spark_round(col("avg_temp_min"), 2).alias("avg_temp_min"), 
                    spark_round(col("total_precipitation"), 2).alias("total_precipitation"),
                    col("dry_days_count"),
                    col("heat_stress_days"),
                    col("processed_at").alias("updated_at")
                )
            
            logger.info(f"✅ Datos preparados para DynamoDB - {table_type}")
            return dynamodb_df
            
        except Exception as e:
            logger.error(f"❌ Error preparando datos DynamoDB: {e}")
            raise

    def save_processed_data(self, df, filename, format_type="csv"):
        """Guardar datos procesados"""
        try:
            if format_type == "csv":
                df.coalesce(1).write.mode("overwrite") \
                    .option("header", "true") \
                    .csv(f"processed_{filename}")
                logger.info(f"💾 Datos guardados: processed_{filename}")
                
            elif format_type == "json":
                df.coalesce(1).write.mode("overwrite") \
                    .json(f"processed_{filename}")
                logger.info(f"💾 Datos JSON guardados: processed_{filename}")
                
        except Exception as e:
            logger.error(f"❌ Error guardando datos: {e}")

    def run_complete_pipeline(self):
        """Ejecutar pipeline completo de procesamiento"""
        try:
            logger.info("🚀 Iniciando pipeline completo de procesamiento...")
            
            # 1. Cargar datos
            daily_df, hourly_df, historical_df = self.load_weather_data()
            
            # 2. Procesar datos temporales
            processed_daily = self.process_temporal_data(daily_df)
            
            # 3. Mostrar muestra de datos procesados
            logger.info("\n📊 MUESTRA DE DATOS PROCESADOS:")
            processed_daily.select(
                "state", "date", "temp_max", "temp_min", 
                "precipitation_sum", "daylight_hours", "sunrise_time", "sunset_time"
            ).show(10, truncate=False)
            
            # 4. Generar alertas por cultivo
            alerts_df = self.generate_crop_alerts(processed_daily)
            if alerts_df:
                logger.info("\n⚠️ ALERTAS GENERADAS:")
                alerts_df.show(truncate=False)
            
            # 5. Crear métricas agregadas
            monthly_metrics, regional_metrics = self.create_aggregated_metrics(processed_daily)
            
            logger.info("\n📈 MÉTRICAS MENSUALES POR ESTADO:")
            monthly_metrics.show(10, truncate=False)
            
            # 6. Preparar datos para DynamoDB
            weather_dynamo = self.prepare_dynamodb_format(processed_daily, "weather_data")
            metrics_dynamo = self.prepare_dynamodb_format(monthly_metrics, "monthly_metrics")
            
            # 7. Guardar datos procesados
            self.save_processed_data(weather_dynamo, "weather_data_dynamo", "json")
            self.save_processed_data(metrics_dynamo, "monthly_metrics_dynamo", "json")
            
            if alerts_df:
                self.save_processed_data(alerts_df, "alerts_dynamo", "json")
            
            # 8. Estadísticas finales
            logger.info("\n📊 RESUMEN FINAL DEL PROCESAMIENTO:")
            logger.info(f"   📅 Días procesados: {processed_daily.count()}")
            logger.info(f"   🏛️  Estados: {processed_daily.select('state').distinct().count()}")
            logger.info(f"   📈 Métricas mensuales: {monthly_metrics.count()}")
            if alerts_df:
                logger.info(f"   ⚠️ Alertas generadas: {alerts_df.count()}")
            
            logger.info("🎉 Pipeline completado exitosamente!")
            
            return {
                "processed_daily": processed_daily,
                "monthly_metrics": monthly_metrics,
                "regional_metrics": regional_metrics,
                "alerts": alerts_df,
                "dynamo_ready": {
                    "weather_data": weather_dynamo,
                    "monthly_metrics": metrics_dynamo
                }
            }
            
        except Exception as e:
            logger.error(f"❌ Error en pipeline: {e}")
            raise
        finally:
            self.spark.stop()

def main():
    """Función principal"""
    print("🔥 PROCESADOR PYSPARK - DATOS METEOROLÓGICOS")
    print("="*60)
    print("🎯 Cultivos regionales: Maíz, Frijol, Chile, Trigo")
    print("⚠️ Alertas básicas por umbrales")
    print("📦 Datos preparados para DynamoDB")
    print("="*60)
    
    try:
        processor = WeatherDataProcessor()
        results = processor.run_complete_pipeline()
        
        print("\n✅ PROCESAMIENTO COMPLETADO!")
        print(f"📁 Archivos generados en carpeta 'processed_*'")
        print(f"📦 Datos listos para integración DynamoDB")
        
    except Exception as e:
        print(f"\n❌ ERROR EN PROCESAMIENTO: {e}")
        logger.error(f"Error crítico: {e}")

if __name__ == "__main__":
    main()
