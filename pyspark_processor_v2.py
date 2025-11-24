import os
import sys
from datetime import datetime

# --- CONFIGURACION JAVA (Vital para tu entorno) ---
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
# --------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, avg, max as max_, min as min_, sum as sum_, 
    concat_ws, to_date, year, month, abs as abs_
)
from pyspark.sql.types import FloatType

# Imports para Machine Learning
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

class AgroAnalyticsEngine:
    def __init__(self):
        print("[INFO] Iniciando Motor de Analisis Agricola (Spark Local + ML)...")
        
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        
        self.spark = SparkSession.builder \
            .appName("AgroClima_Analytics_Backend") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.ui.showConsoleProgress", "false") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")

        # Reglas de Cultivo
        self.crop_rules = {
            "maiz":  {"min_temp": 10, "max_temp": 30, "frost_kill": 0},
            "trigo": {"min_temp": 5,  "max_temp": 25, "frost_kill": -2},
            "frijol": {"min_temp": 12, "max_temp": 28, "frost_kill": 2},
            "sorgo": {"min_temp": 15, "max_temp": 32, "frost_kill": 5}
        }

    # ==========================================================================
    # FASE 1: INGESTA
    # ==========================================================================
    def ingest_data(self):
        print("\n[FASE 1] Ingesta de Datos...")
        
        path_hist = f"file://{os.path.join(self.base_dir, 'datos_meteorologicos', 'historico', '*.csv')}"
        path_fore = f"file://{os.path.join(self.base_dir, 'datos_meteorologicos', 'pronostico', '*.csv')}"

        try:
            df_hist = self.spark.read.option("header", "true").option("inferSchema", "true").csv(path_hist) \
                .withColumn("origen_dato", lit("historico"))
            
            df_fore = self.spark.read.option("header", "true").option("inferSchema", "true").csv(path_fore) \
                .withColumn("origen_dato", lit("pronostico"))

            self.full_df = df_hist.unionByName(df_fore, allowMissingColumns=True)
            
            print(f"   [OK] Datos Unificados: {self.full_df.count()} registros.")
            return self.full_df

        except Exception as e:
            print(f"   [ERROR] Fallo en ingesta: {e}")
            sys.exit(1)

    # ==========================================================================
    # FASE 2: LIMPIEZA
    # ==========================================================================
    def clean_and_enrich(self, df):
        print("\n[FASE 2] Limpieza y Indices Agricolas...")
        
        df = df.withColumn("temp_max", col("temperature_2m_max").cast(FloatType())) \
               .withColumn("temp_min", col("temperature_2m_min").cast(FloatType())) \
               .withColumn("precip", col("precipitation_sum").cast(FloatType())) \
               .withColumn("viento", col("wind_speed_10m_max").cast(FloatType())) \
               .withColumn("radiacion", col("shortwave_radiation_sum").cast(FloatType())) \
               .withColumn("evapotranspiracion", col("et0_fao_evapotranspiration").cast(FloatType()))

        # Indices
        df = df.withColumn("rango_termico", col("temp_max") - col("temp_min"))
        
        t_prom = (col("temp_max") + col("temp_min")) / 2
        df = df.withColumn("gdd_maiz", 
                           when((t_prom - 10) > 0, t_prom - 10).otherwise(0))

        df = df.withColumn("deficit_hidrico", col("evapotranspiracion") - col("precip"))

        df = df.withColumn("alerta_helada", when(col("temp_min") <= 2, 1).otherwise(0)) \
               .withColumn("alerta_calor", when(col("temp_max") >= 35, 1).otherwise(0)) \
               .withColumn("alerta_viento", when(col("viento") >= 40, 1).otherwise(0))

        # Rellenar nulos para ML (Importante)
        df = df.fillna(0, subset=["precip", "viento", "radiacion"])
        
        return df

    # ==========================================================================
    # FASE 3: LOGICA DE NEGOCIO
    # ==========================================================================
    def apply_crop_logic(self, df):
        print("\n[FASE 3] Evaluando Riesgos por Cultivo...")
        
        for cultivo, reglas in self.crop_rules.items():
            col_name = f"riesgo_{cultivo}"
            df = df.withColumn(col_name, 
                when(col("temp_min") <= reglas['frost_kill'], lit(3)) 
                .when((col("temp_min") < reglas['min_temp']) | (col("temp_max") > reglas['max_temp']), lit(2))
                .otherwise(lit(0))
            )
        return df

    # ==========================================================================
    # FASE 3.5: MACHINE LEARNING (K-MEANS)
    # ==========================================================================
    def apply_machine_learning(self, df):
        print("\n[FASE 3.5] Ejecutando Machine Learning (K-Means)...")
        
        # 1. Definir Features
        feature_cols = ["temp_max", "temp_min", "precip", "viento", "radiacion"]
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = MinMaxScaler(inputCol="features_raw", outputCol="features")
        
        # k=4 Climas tipicos (ej. Seco, Humedo, Frio, Extremo)
        kmeans = KMeans(k=4, seed=1, featuresCol="features", predictionCol="cluster_clima")
        
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        print("   [INFO] Entrenando modelo con datos historicos...")
        
        # Entrenar solo con historico
        df_historico = df.filter(col("origen_dato") == "historico")
        
        if df_historico.count() > 0:
            model = pipeline.fit(df_historico)
            
            print("   [INFO] Generando predicciones (Cluster IDs)...")
            # Predecir sobre TODO el dataset (incluyendo pronostico)
            predictions = model.transform(df)
            
            # Limpiar columnas vectoriales intermedias (no soportadas por JSON/Dynamo)
            return predictions.drop("features_raw", "features")
        else:
            print("   [WARN] No hay historico suficiente. Saltando ML.")
            return df

    # ==========================================================================
    # FASE 4: EXPORTACION
    # ==========================================================================
    def export_results(self, df):
        print("\n[FASE 4] Generando JSONs para DynamoDB...")
        
        output_dir = os.path.join(self.base_dir, "datos_procesados")
        
        # A. WeatherDetails (Incluye campo nuevo 'cluster_clima')
        print("   -> WeatherDetails...")
        final_df = df.select(
            col("state").alias("PK_State"),
            col("date").alias("SK_Date"),
            col("region"),
            col("temp_max"), col("temp_min"), col("precip"), 
            col("radiacion"), col("gdd_maiz"),
            col("riesgo_maiz"), col("riesgo_trigo"), col("riesgo_frijol"),
            col("alerta_helada"), col("alerta_calor"),
            col("origen_dato"),
            col("cluster_clima") # CAMPO ML NUEVO
        )
        path_details = f"file://{os.path.join(output_dir, 'dynamo_weather_details')}"
        final_df.coalesce(1).write.mode("overwrite").json(path_details)

        # B. ActiveAlerts
        print("   -> ActiveAlerts...")
        alerts_df = final_df.filter(
            (col("origen_dato") == "pronostico") & 
            ((col("riesgo_maiz") >= 2) | (col("riesgo_trigo") >= 2) | (col("alerta_helada") == 1))
        ).select(
            concat_ws("#", lit("ALERT"), col("PK_State")).alias("PK_Alert"),
            col("SK_Date"),
            col("riesgo_maiz"),
            col("riesgo_trigo"),
            col("alerta_helada"),
            lit("WARNING").alias("Level"),
            col("cluster_clima") # Agregamos contexto ML a la alerta
        )
        path_alerts = f"file://{os.path.join(output_dir, 'dynamo_active_alerts')}"
        alerts_df.coalesce(1).write.mode("overwrite").json(path_alerts)

        # C. MonthlyStats
        print("   -> MonthlyStats...")
        stats_df = df.withColumn("Year", year("date")).withColumn("Month", month("date")) \
            .groupBy("state", "Year", "Month") \
            .agg(
                avg("temp_max").alias("Avg_Temp_Max"),
                sum_("precip").alias("Total_Precip"),
                sum_("gdd_maiz").alias("Total_GDD")
            )
        path_stats = f"file://{os.path.join(output_dir, 'dashboard_monthly_stats')}"
        stats_df.coalesce(1).write.mode("overwrite").json(path_stats)

        print(f"\n[FIN] Procesamiento terminado. Archivos en: {output_dir}")

    def run(self):
        raw_data = self.ingest_data()
        clean_data = self.clean_and_enrich(raw_data)
        analyzed_data = self.apply_crop_logic(clean_data)
        
        # Integracion ML
        ml_data = self.apply_machine_learning(analyzed_data)
        
        self.export_results(ml_data)
        self.spark.stop()

if __name__ == "__main__":
    engine = AgroAnalyticsEngine()
    engine.run()
