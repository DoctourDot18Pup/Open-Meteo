import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import os
import time
from datetime import datetime, timedelta

# ==============================================================================
# CONFIGURACIÓN INICIAL
# ==============================================================================

# 1. Configuración de Cliente API con Cache y Retry
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# 2. Definición de Directorios de Salida (Deben coincidir con tu PySpark)
BASE_DIR = "datos_meteorologicos_agricola"
DIR_HIST = os.path.join(BASE_DIR, "historico")
DIR_FORE = os.path.join(BASE_DIR, "pronostico")

os.makedirs(DIR_HIST, exist_ok=True)
os.makedirs(DIR_FORE, exist_ok=True)

# 3. Coordenadas de los 32 Estados (Lat/Lon aproximado del centro agrícola)
ESTADOS_MEXICO = {
    "Aguascalientes": {"lat": 21.88, "lon": -102.29, "region": "Centro-Norte"},
    "Baja California": {"lat": 30.84, "lon": -115.28, "region": "Noroeste"},
    "Baja California Sur": {"lat": 26.04, "lon": -111.67, "region": "Noroeste"},
    "Campeche": {"lat": 19.83, "lon": -90.53, "region": "Sureste"},
    "Coahuila": {"lat": 27.05, "lon": -101.70, "region": "Noreste"},
    "Colima": {"lat": 19.24, "lon": -103.72, "region": "Occidente"},
    "Chiapas": {"lat": 16.75, "lon": -93.12, "region": "Sureste"},
    "Chihuahua": {"lat": 28.63, "lon": -106.08, "region": "Norte"},
    "Ciudad de Mexico": {"lat": 19.43, "lon": -99.13, "region": "Centro"},
    "Durango": {"lat": 24.02, "lon": -104.65, "region": "Norte"},
    "Guanajuato": {"lat": 21.01, "lon": -101.25, "region": "Bajio"},
    "Guerrero": {"lat": 17.43, "lon": -99.54, "region": "Sur"},
    "Hidalgo": {"lat": 20.09, "lon": -98.76, "region": "Centro"},
    "Jalisco": {"lat": 20.65, "lon": -103.34, "region": "Occidente"},
    "Mexico": {"lat": 19.35, "lon": -99.63, "region": "Centro"},
    "Michoacan": {"lat": 19.56, "lon": -101.70, "region": "Occidente"},
    "Morelos": {"lat": 18.68, "lon": -99.10, "region": "Centro"},
    "Nayarit": {"lat": 21.75, "lon": -104.84, "region": "Occidente"},
    "Nuevo Leon": {"lat": 25.59, "lon": -99.99, "region": "Noreste"},
    "Oaxaca": {"lat": 17.07, "lon": -96.72, "region": "Sur"},
    "Puebla": {"lat": 19.04, "lon": -98.20, "region": "Centro"},
    "Queretaro": {"lat": 20.58, "lon": -100.38, "region": "Bajio"},
    "Quintana Roo": {"lat": 19.18, "lon": -88.47, "region": "Sureste"},
    "San Luis Potosi": {"lat": 22.15, "lon": -100.98, "region": "Centro-Norte"},
    "Sinaloa": {"lat": 25.17, "lon": -107.47, "region": "Noroeste"},
    "Sonora": {"lat": 29.29, "lon": -110.33, "region": "Noroeste"},
    "Tabasco": {"lat": 17.84, "lon": -92.61, "region": "Sureste"},
    "Tamaulipas": {"lat": 24.26, "lon": -98.83, "region": "Noreste"},
    "Tlaxcala": {"lat": 19.31, "lon": -98.24, "region": "Centro"},
    "Veracruz": {"lat": 19.17, "lon": -96.13, "region": "Golfo"},
    "Yucatan": {"lat": 20.70, "lon": -89.09, "region": "Sureste"},
    "Zacatecas": {"lat": 22.77, "lon": -102.57, "region": "Centro-Norte"}
}

# 4. Variables EXACTAS requeridas por PySpark (clean_and_enrich)
# Nota: Usamos 'daily' para obtener sumas y promedios directamente y facilitar la carga en Spark
VARIABLES_DAILY = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
    "shortwave_radiation_sum",
    "et0_fao_evapotranspiration",
    "soil_moisture_0_to_7cm_mean",
    "soil_temperature_0_to_7cm_mean"
]

# ==============================================================================
# FUNCIONES DE EXTRACCIÓN
# ==============================================================================

def fetch_and_save(url, params, folder, state_name, region, file_prefix):
    try:
        print(f"   Solicitando datos para {state_name} ({file_prefix})...")
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]

        # Procesar datos diarios
        daily = response.Daily()
        
        # Diccionario dinámico para Pandas
        daily_data = {"date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        )}

        # Extraer variables dinámicamente según la lista
        for i, var_name in enumerate(VARIABLES_DAILY):
            daily_data[var_name] = daily.Variables(i).ValuesAsNumpy()

        # Crear DataFrame
        df = pd.DataFrame(data=daily_data)
        
        # Agregar columnas de metadatos para PySpark
        df["state"] = state_name
        df["region"] = region
        
        # Guardar CSV
        filename = f"{file_prefix}_{state_name.replace(' ', '_')}.csv"
        path = os.path.join(folder, filename)
        df.to_csv(path, index=False)
        print(f"      -> Guardado: {filename}")
        
    except Exception as e:
        print(f"      [ERROR] Fallo en {state_name}: {e}")

# ==============================================================================
# EJECUCIÓN PRINCIPAL
# ==============================================================================

def main():
    print("--- INICIANDO EXTRACCIÓN MASIVA DE DATOS METEOROLÓGICOS ---")
    
    # Rango de fechas para Histórico (Últimos 2 años para entrenamiento IA)
    end_date_hist = datetime.now() - timedelta(days=8) # Hasta hace una semana
    start_date_hist = end_date_hist - timedelta(days=730) # 2 años atrás

    # 1. EXTRACCIÓN HISTÓRICA (Archive API)
    print(f"\n[1/2] Extrayendo HISTÓRICO ({start_date_hist.date()} a {end_date_hist.date()})")
    url_hist = "https://archive-api.open-meteo.com/v1/archive"
    
    for state, data in ESTADOS_MEXICO.items():
        params = {
            "latitude": data["lat"],
            "longitude": data["lon"],
            "start_date": start_date_hist.strftime("%Y-%m-%d"),
            "end_date": end_date_hist.strftime("%Y-%m-%d"),
            "daily": VARIABLES_DAILY,
            "timezone": "America/Mexico_City"
        }
        fetch_and_save(url_hist, params, DIR_HIST, state, data["region"], "HIST")
        
        # PAUSA para evitar Rate Limit (Importante para 32 estados seguidos)
        time.sleep(1.5) 

    # 2. EXTRACCIÓN PRONÓSTICO (Forecast API)
    print("\n[2/2] Extrayendo PRONÓSTICO (Próximos 16 días)")
    url_fore = "https://api.open-meteo.com/v1/forecast"
    
    for state, data in ESTADOS_MEXICO.items():
        params = {
            "latitude": data["lat"],
            "longitude": data["lon"],
            "daily": VARIABLES_DAILY,
            "forecast_days": 16, # Máximo gratuito aproximado
            "timezone": "America/Mexico_City"
        }
        fetch_and_save(url_fore, params, DIR_FORE, state, data["region"], "FORE")
        
        # Pausa leve
        time.sleep(1.5)

    print("\n--- PROCESO TERMINADO ---")
    print(f"Archivos listos en: {os.path.abspath(BASE_DIR)}")

if __name__ == "__main__":
    main()
