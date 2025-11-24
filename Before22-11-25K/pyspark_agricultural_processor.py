import os
import sys
import logging
from datetime import datetime, timedelta
import json

# Configurar PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Fix para distutils en Python 3.12+
try:
    import distutils
except ImportError:
    import setuptools
    import pkg_resources

try:
    import findspark
    findspark.init()
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, lit, when, round as spark_round, date_format, 
        year, month, dayofmonth, avg, max as spark_max, 
        min as spark_min, sum as spark_sum, count, desc,
        regexp_replace, split, concat_ws, current_timestamp,
        datediff, lag, lead, stddev, first, last, collect_list
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, 
        IntegerType, BooleanType, TimestampType
    )
    from pyspark.sql.window import Window
    
    print("✅ PySpark importado correctamente")
    
except ImportError as e:
    print(f"❌ Error importando PySpark: {e}")
    print("Soluciones:")
    print("1. pip install setuptools")
    print("2. Usar Python 3.11 en lugar de 3.12+")
    sys.exit(1)

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AgriculturalWeatherProcessor:
    def __init__(self):
        self.spark = self._initialize_spark()
        
        # Cultivos principales de México con umbrales específicos
        self.crops_config = {
            "maiz": {
                "name": "Maíz",
                "frost_critical": 0,       # °C - Temperatura crítica de helada
                "frost_warning": 2,        # °C - Advertencia de helada
                "heat_stress": 35,         # °C - Estrés térmico
                "heat_extreme": 40,        # °C - Calor extremo
                "drought_days": 15,        # Días sin lluvia = sequía
                "min_radiation": 15,       # MJ/m² - Radiación mínima diaria
                "optimal_temp_min": 18,    # °C - Temperatura óptima mínima
                "optimal_temp_max": 30     # °C - Temperatura óptima máxima
            },
            "frijol": {
                "name": "Frijol",
                "frost_critical": 0,
                "frost_warning": 3,
                "heat_stress": 32,
                "heat_extreme": 38,
                "drought_days": 10,
                "min_radiation": 12,
                "optimal_temp_min": 15,
                "optimal_temp_max": 28
            },
            "chile": {
                "name": "Chile",
                "frost_critical": 2,
                "frost_warning": 5,
                "heat_stress": 38,
                "heat_extreme": 42,
                "drought_days": 12,
                "min_radiation": 18,
                "optimal_temp_min": 20,
                "optimal_temp_max": 32
            },
            "trigo": {
                "name": "Trigo",
                "frost_critical": -2,
                "frost_warning": 0,
                "heat_stress": 30,
                "heat_extreme": 35,
                "drought_days": 20,
                "min_radiation": 10,
                "optimal_temp_min": 10,
                "optimal_temp_max": 25
            }
        }
        
    def _initialize_spark(self):
        """Inicializar SparkSession en modo local"""
        try:
            spark = SparkSession.builder \
                .appName("MexicanAgriculturalWeatherAnalysis") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.driver.memory", "2g") \
                .config("spark.driver.maxResultSize", "1g") \
                .config("spark.hadoop.fs.defaultFS", "file:///") \
                .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
                .config("spark.sql.streaming.checkpointLocation", "file:///tmp/spark-checkpoint") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            logger.info("✅ SparkSession inicializado correctamente en modo local")
            return spark
            
        except Exception as e:
            logger.error(f"❌ Error inicializando Spark: {e}")
            sys.exit(1)
    
    def load_weather_data(self, file_path):
        """Cargar datos meteorológicos desde CSV"""
        try:
            logger.info(f"📊 Cargando datos desde: {file_path}")
            
            # Cargar CSV con schema inferido
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(file_path)
            
            logger.info(f"✅ Datos cargados: {df.count():,} registros, {len(df.columns)} columnas")
            
            # Mostrar schema para verificación
            print("\n📋 SCHEMA DE DATOS:")
            df.printSchema()
            
            # Convertir fecha si es necesario
            if 'date' in df.columns:
                df = df.withColumn("date", col("date").cast("date"))
            
            # Agregar columnas temporales
            df = df.withColumn("year", year("date")) \
                   .withColumn("month", month("date")) \
                   .withColumn("day_of_year", dayofmonth("date"))
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error cargando datos: {e}")
            return None
    
    def clean_and_enrich_data(self, df):
        """Limpiar y enriquecer datos meteorológicos"""
        logger.info("🧹 Limpiando y enriqueciendo datos...")
        
        # Filtrar valores nulos en variables críticas
        initial_count = df.count()
        df_clean = df.filter(
            col("temperature_2m_max").isNotNull() &
            col("temperature_2m_min").isNotNull() &
            col("precipitation_sum").isNotNull()
        )
        
        cleaned_count = df_clean.count()
        logger.info(f"🧹 Limpieza completada: {cleaned_count:,}/{initial_count:,} registros válidos")
        
        # Enriquecer con variables derivadas
        df_enriched = df_clean \
            .withColumn("thermal_amplitude", col("temperature_2m_max") - col("temperature_2m_min")) \
            .withColumn("is_dry_day", when(col("precipitation_sum") < 1, 1).otherwise(0)) \
            .withColumn("is_rainy_day", when(col("precipitation_sum") > 10, 1).otherwise(0)) \
            .withColumn("is_hot_day", when(col("temperature_2m_max") > 35, 1).otherwise(0)) \
            .withColumn("is_cold_day", when(col("temperature_2m_min") < 5, 1).otherwise(0)) \
            .withColumn("growing_degree_days", 
                       when(((col("temperature_2m_max") + col("temperature_2m_min")) / 2 - 10) > 0,
                            (col("temperature_2m_max") + col("temperature_2m_min")) / 2 - 10)
                       .otherwise(0))
        
        # Agregar estación del año
        df_enriched = df_enriched.withColumn("season",
            when((col("month") >= 3) & (col("month") <= 5), "primavera")
            .when((col("month") >= 6) & (col("month") <= 8), "verano")
            .when((col("month") >= 9) & (col("month") <= 11), "otoño")
            .otherwise("invierno")
        )
        
        return df_enriched
    
    def generate_agricultural_alerts(self, df):
        """Generar alertas agrícolas por cultivo y estado"""
        logger.info("⚠️ Generando alertas agrícolas...")
        
        alerts_list = []
        
        # Procesar cada estado
        states = df.select("state").distinct().collect()
        
        for state_row in states:
            state_name = state_row['state']
            state_data = df.filter(col("state") == state_name)
            
            logger.info(f"🌾 Procesando alertas para: {state_name}")
            
            # Generar alertas por cultivo
            for crop_code, crop_config in self.crops_config.items():
                crop_alerts = self._generate_crop_alerts(state_data, state_name, crop_code, crop_config)
                alerts_list.extend(crop_alerts)
        
        logger.info(f"⚠️ Total de alertas generadas: {len(alerts_list)}")
        
        # Convertir lista a DataFrame de Spark
        if alerts_list:
            alerts_df = self.spark.createDataFrame(alerts_list)
            return alerts_df
        else:
            return None
    
    def _generate_crop_alerts(self, state_data, state_name, crop_code, crop_config):
        """Generar alertas específicas por cultivo"""
        alerts = []
        
        # Convertir a Pandas para análisis detallado
        state_pd = state_data.toPandas()
        
        if len(state_pd) == 0:
            return alerts
        
        crop_name = crop_config['name']
        
        # 1. ALERTAS DE HELADA
        frost_critical_days = state_pd[state_pd['temperature_2m_min'] <= crop_config['frost_critical']]
        if len(frost_critical_days) > 0:
            alerts.append({
                "alert_id": f"{state_name}_{crop_code}_frost_critical_{datetime.now().strftime('%Y%m%d')}",
                "state": state_name,
                "crop": crop_code,
                "crop_name": crop_name,
                "alert_type": "frost_critical",
                "severity": "CRÍTICO",
                "message": f"Helada crítica detectada para {crop_name}. Temp mínima: {frost_critical_days['temperature_2m_min'].min():.1f}°C",
                "affected_days": len(frost_critical_days),
                "min_temperature": float(frost_critical_days['temperature_2m_min'].min()),
                "date_first": str(frost_critical_days['date'].min()),
                "date_last": str(frost_critical_days['date'].max()),
                "recommendations": [
                    "Implementar sistemas de protección contra heladas",
                    "Considerar cosecha anticipada si el cultivo lo permite",
                    "Monitorear pronósticos nocturnos"
                ],
                "timestamp": datetime.now().isoformat()
            })
        
        # 2. ALERTAS DE CALOR EXTREMO
        heat_extreme_days = state_pd[state_pd['temperature_2m_max'] >= crop_config['heat_extreme']]
        if len(heat_extreme_days) > 0:
            alerts.append({
                "alert_id": f"{state_name}_{crop_code}_heat_extreme_{datetime.now().strftime('%Y%m%d')}",
                "state": state_name,
                "crop": crop_code,
                "crop_name": crop_name,
                "alert_type": "heat_extreme",
                "severity": "ALTO",
                "message": f"Calor extremo para {crop_name}. Temp máxima: {heat_extreme_days['temperature_2m_max'].max():.1f}°C",
                "affected_days": len(heat_extreme_days),
                "max_temperature": float(heat_extreme_days['temperature_2m_max'].max()),
                "date_first": str(heat_extreme_days['date'].min()),
                "date_last": str(heat_extreme_days['date'].max()),
                "recommendations": [
                    "Aumentar frecuencia de riego",
                    "Implementar sombreado si es posible",
                    "Monitorear estrés hídrico del cultivo"
                ],
                "timestamp": datetime.now().isoformat()
            })
        
        # 3. ALERTAS DE SEQUÍA
        consecutive_dry_days = self._calculate_consecutive_dry_days(state_pd)
        if consecutive_dry_days >= crop_config['drought_days']:
            alerts.append({
                "alert_id": f"{state_name}_{crop_code}_drought_{datetime.now().strftime('%Y%m%d')}",
                "state": state_name,
                "crop": crop_code,
                "crop_name": crop_name,
                "alert_type": "drought",
                "severity": "MEDIO" if consecutive_dry_days < crop_config['drought_days'] * 1.5 else "ALTO",
                "message": f"Sequía prolongada para {crop_name}. {consecutive_dry_days} días consecutivos sin lluvia significativa",
                "affected_days": consecutive_dry_days,
                "total_precipitation": float(state_pd['precipitation_sum'].sum()),
                "avg_precipitation": float(state_pd['precipitation_sum'].mean()),
                "recommendations": [
                    "Implementar riego suplementario",
                    "Evaluar reservas de agua",
                    "Considerar mulching para conservar humedad"
                ],
                "timestamp": datetime.now().isoformat()
            })
        
        # 4. ALERTAS DE RADIACIÓN INSUFICIENTE
        if 'shortwave_radiation_sum' in state_pd.columns:
            low_radiation_days = state_pd[state_pd['shortwave_radiation_sum'] < crop_config['min_radiation']]
            if len(low_radiation_days) > len(state_pd) * 0.3:  # Más del 30% de días
                alerts.append({
                    "alert_id": f"{state_name}_{crop_code}_low_radiation_{datetime.now().strftime('%Y%m%d')}",
                    "state": state_name,
                    "crop": crop_code,
                    "crop_name": crop_name,
                    "alert_type": "low_radiation",
                    "severity": "MEDIO",
                    "message": f"Radiación insuficiente para {crop_name}. {len(low_radiation_days)} días con baja radiación",
                    "affected_days": len(low_radiation_days),
                    "avg_radiation": float(state_pd['shortwave_radiation_sum'].mean()),
                    "min_radiation_required": crop_config['min_radiation'],
                    "recommendations": [
                        "Monitorear desarrollo vegetativo",
                        "Considerar suplementos nutricionales",
                        "Evaluar espaciamiento entre plantas"
                    ],
                    "timestamp": datetime.now().isoformat()
                })
        
        return alerts
    
    def _calculate_consecutive_dry_days(self, df_pd):
        """Calcular días consecutivos sin lluvia"""
        if len(df_pd) == 0:
            return 0
        
        # Ordenar por fecha
        df_sorted = df_pd.sort_values('date')
        
        max_consecutive = 0
        current_consecutive = 0
        
        for _, row in df_sorted.iterrows():
            if row['precipitation_sum'] < 1:  # Día seco
                current_consecutive += 1
                max_consecutive = max(max_consecutive, current_consecutive)
            else:
                current_consecutive = 0
        
        return max_consecutive
    
    def generate_monthly_metrics(self, df):
        """Generar métricas agregadas por estado y mes"""
        logger.info("📈 Generando métricas mensuales por estado...")
        
        # Verificar qué columnas están disponibles
        available_columns = df.columns
        logger.info(f"Columnas disponibles: {available_columns}")
        
        # Agregaciones básicas
        monthly_metrics = df.groupBy("state", "year", "month") \
            .agg(
                count("*").alias("total_days"),
                avg("temperature_2m_max").alias("avg_temp_max"),
                avg("temperature_2m_min").alias("avg_temp_min"),
                spark_max("temperature_2m_max").alias("max_temp_absolute"),
                spark_min("temperature_2m_min").alias("min_temp_absolute"),
                spark_sum("precipitation_sum").alias("total_precipitation"),
                avg("precipitation_sum").alias("avg_daily_precipitation"),
                spark_sum("is_dry_day").alias("total_dry_days"),
                spark_sum("is_rainy_day").alias("total_rainy_days"),
                spark_sum("is_hot_day").alias("total_hot_days"),
                spark_sum("is_cold_day").alias("total_cold_days"),
                avg("growing_degree_days").alias("avg_growing_degree_days"),
                spark_sum("growing_degree_days").alias("total_growing_degree_days")
            )
        
        # Agregar radiación solo si la columna existe
        if 'shortwave_radiation_sum' in available_columns:
            monthly_metrics = monthly_metrics.withColumn("avg_radiation", 
                lit(0))  # Placeholder, se calculará después si es necesario
            logger.info("✅ Columna de radiación agregada como placeholder")
        else:
            logger.info("⚠️ Columna shortwave_radiation_sum no disponible, omitiendo del agregado")
        
        # Agregar columnas derivadas
        monthly_metrics = monthly_metrics \
            .withColumn("drought_risk_percentage", 
                       (col("total_dry_days") / col("total_days") * 100)) \
            .withColumn("thermal_stress_percentage",
                       (col("total_hot_days") / col("total_days") * 100)) \
            .withColumn("period_id", 
                       concat_ws("-", col("state"), col("year"), col("month"))) \
            .withColumn("analysis_timestamp", current_timestamp())
        
        logger.info(f"📊 Métricas mensuales generadas para {monthly_metrics.count()} períodos")
        return monthly_metrics
    
    def save_to_dynamo_format(self, df, output_dir, file_prefix):
        """Guardar DataFrame en formato compatible con DynamoDB"""
        try:
            # Crear directorio si no existe
            os.makedirs(output_dir, exist_ok=True)
            
            # Convertir a Pandas para mejor manejo de JSON
            df_pd = df.toPandas()
            
            # Guardar como JSON Lines (un JSON por línea)
            output_file = os.path.join(output_dir, f"{file_prefix}_{datetime.now().strftime('%Y%m%d_%H%M')}.json")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for _, row in df_pd.iterrows():
                    # Convertir row a dict y limpiar valores NaN
                    record = row.to_dict()
                    
                    # Limpiar valores NaN/None para DynamoDB
                    cleaned_record = {}
                    for key, value in record.items():
                        if value is not None and str(value) != 'nan':
                            if isinstance(value, float):
                                cleaned_record[key] = round(value, 3)
                            else:
                                cleaned_record[key] = str(value)
                    
                    # Escribir JSON
                    f.write(json.dumps(cleaned_record, ensure_ascii=False) + '\n')
            
            logger.info(f"💾 Datos guardados: {output_file}")
            logger.info(f"📊 Registros exportados: {len(df_pd):,}")
            
            return output_file
            
        except Exception as e:
            logger.error(f"❌ Error guardando datos: {e}")
            return None
    
    def process_complete_pipeline(self, input_file):
        """Ejecutar pipeline completo de procesamiento"""
        logger.info("🚀 INICIANDO PIPELINE COMPLETO DE PROCESAMIENTO AGRÍCOLA")
        logger.info("="*80)
        
        start_time = datetime.now()
        
        # 1. Cargar datos
        df_raw = self.load_weather_data(input_file)
        if df_raw is None:
            logger.error("❌ No se pudieron cargar los datos")
            return
        
        # 2. Limpiar y enriquecer
        df_clean = self.clean_and_enrich_data(df_raw)
        
        # 3. Generar alertas agrícolas
        alerts_df = self.generate_agricultural_alerts(df_clean)
        
        # 4. Generar métricas mensuales
        metrics_df = self.generate_monthly_metrics(df_clean)
        
        # 5. Preparar datos principales para DynamoDB
        df_dynamo = df_clean.withColumn("sort_key", date_format("date", "yyyy-MM-dd")) \
                           .withColumn("temp_max", spark_round("temperature_2m_max", 2)) \
                           .withColumn("temp_min", spark_round("temperature_2m_min", 2)) \
                           .withColumn("precipitation", spark_round("precipitation_sum", 2)) \
                           .withColumn("processed_timestamp", current_timestamp()) \
                           .select("state", "sort_key", "temp_max", "temp_min", "precipitation", 
                                  "thermal_amplitude", "growing_degree_days", "season", 
                                  "is_dry_day", "is_rainy_day", "processed_timestamp")
        
        # 6. Guardar todos los datasets
        outputs = {}
        
        # Datos meteorológicos procesados
        outputs['weather_data'] = self.save_to_dynamo_format(
            df_dynamo, 'processed_weather_data_dynamo', 'weather_data')
        
        # Métricas agregadas
        outputs['metrics'] = self.save_to_dynamo_format(
            metrics_df, 'processed_monthly_metrics_dynamo', 'monthly_metrics')
        
        # Alertas agrícolas
        if alerts_df is not None:
            outputs['alerts'] = self.save_to_dynamo_format(
                alerts_df, 'processed_alerts_dynamo', 'agricultural_alerts')
        
        # 7. Estadísticas finales
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        
        print("\n" + "="*80)
        print("🎉 PIPELINE COMPLETADO EXITOSAMENTE")
        print("="*80)
        print(f"⏱️ Tiempo de procesamiento: {processing_time:.1f} segundos")
        print(f"📊 Registros procesados: {df_clean.count():,}")
        print(f"🌎 Estados analizados: {df_clean.select('state').distinct().count()}")
        
        if alerts_df is not None:
            print(f"⚠️ Alertas generadas: {alerts_df.count()}")
        
        print(f"📈 Métricas mensuales: {metrics_df.count()}")
        
        print("\n📁 ARCHIVOS GENERADOS:")
        for output_type, file_path in outputs.items():
            if file_path:
                print(f"   ✅ {output_type}: {file_path}")
        
        print("\n🚀 PRÓXIMOS PASOS:")
        print("   1. ✅ Datos procesados y listos")
        print("   2. 📦 Configurar tablas DynamoDB")
        print("   3. 🚀 Crear Lambda functions")
        print("   4. 🌐 Setup API Gateway")
        print("   5. 📱 Desarrollar Dashboard Flutter")
        
        return outputs

def main():
    """Función principal"""
    print("🌾 PROCESADOR AGRÍCOLA MEXICANO - PYSPARK")
    print("="*60)
    print(f"🕐 Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Archivo principal a procesar
    input_file = "mexico_32_estados_daily_20251119_1717.csv"
    
    if not os.path.exists(input_file):
        print(f"❌ Error: No se encontró el archivo {input_file}")
        print("📋 Archivos disponibles:")
        for file in os.listdir('.'):
            if file.endswith('.csv'):
                print(f"   📄 {file}")
        return
    
    # Inicializar procesador
    processor = AgriculturalWeatherProcessor()
    
    try:
        # Ejecutar pipeline completo
        outputs = processor.process_complete_pipeline(input_file)
        
        if outputs:
            print(f"\n🎉 PROCESAMIENTO EXITOSO!")
            print(f"📊 {len(outputs)} tipos de datos generados")
        else:
            print(f"\n❌ Error durante el procesamiento")
            
    except Exception as e:
        logger.error(f"❌ Error en el pipeline: {e}")
    
    finally:
        # Limpiar recursos
        processor.spark.stop()
        print("🔌 SparkSession cerrada")

if __name__ == "__main__":
    main()
