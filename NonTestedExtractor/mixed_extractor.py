import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import logging
from datetime import datetime, timedelta
import time
import numpy as np
import os

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class StructuredWeatherExtractor:
    def __init__(self):
        # Configuración de cliente con Cache y Retry (Esencial para 32 estados)
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=retry_session)
        
        self.archive_url = "https://archive-api.open-meteo.com/v1/archive"
        self.forecast_url = "https://api.open-meteo.com/v1/forecast"
        
        # DEFINICIÓN DE DIRECTORIOS DE SALIDA
        self.base_dir = "datos_meteorologicos"
        self.hist_dir = os.path.join(self.base_dir, "historico")
        self.fore_dir = os.path.join(self.base_dir, "pronostico")
        
        self._setup_directories()

        # Variables validadas (basadas en tu script 'reliable')
        self.daily_vars = [
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "precipitation_sum", "rain_sum", "showers_sum", "snowfall_sum",
            "wind_speed_10m_max", "wind_gusts_10m_max",
            "shortwave_radiation_sum", "et0_fao_evapotranspiration"
        ]

        # Lista completa de estados (simplificada para el ejemplo, usa tu diccionario completo)
        self.states = {
            # NORTE
            "Baja California": {"lat": 32.6519, "lon": -115.4683, "region": "Norte"},
            "Sonora": {"lat": 29.0729, "lon": -110.9559, "region": "Norte"},
            "Chihuahua": {"lat": 28.6353, "lon": -106.0889, "region": "Norte"},
            "Nuevo Leon": {"lat": 25.5922, "lon": -99.9962, "region": "Norte"},
            "Tamaulipas": {"lat": 24.2669, "lon": -98.8363, "region": "Norte"},
            "Coahuila": {"lat": 27.0587, "lon": -101.7068, "region": "Norte"},
            "Baja California Sur": {"lat": 26.0444, "lon": -111.6661, "region": "Norte"},
            "Sinaloa": {"lat": 24.8069, "lon": -107.3940, "region": "Norte"},
            "Durango": {"lat": 24.0277, "lon": -104.6532, "region": "Norte"},
            "Zacatecas": {"lat": 22.7709, "lon": -102.5832, "region": "Norte"},
            "San Luis Potosi": {"lat": 22.1565, "lon": -100.9855, "region": "Centro-Norte"},
            
            # CENTRO / BAJÍO
            "Guanajuato": {"lat": 21.0190, "lon": -101.2574, "region": "Bajio"},
            "Queretaro": {"lat": 20.5888, "lon": -100.3899, "region": "Bajio"},
            "Aguascalientes": {"lat": 21.8853, "lon": -102.2916, "region": "Bajio"},
            "Jalisco": {"lat": 20.6597, "lon": -103.3496, "region": "Occidente"},
            "Michoacan": {"lat": 19.5665, "lon": -101.7068, "region": "Occidente"},
            "Colima": {"lat": 19.2433, "lon": -103.7244, "region": "Occidente"},
            "Nayarit": {"lat": 21.7514, "lon": -104.8455, "region": "Occidente"},
            "Hidalgo": {"lat": 20.0910, "lon": -98.7624, "region": "Centro"},
            "Estado de Mexico": {"lat": 19.3569, "lon": -99.6561, "region": "Centro"},
            "Ciudad de Mexico": {"lat": 19.4326, "lon": -99.1332, "region": "Centro"},
            "Morelos": {"lat": 18.6813, "lon": -99.1013, "region": "Centro"},
            "Puebla": {"lat": 19.0414, "lon": -98.2063, "region": "Centro"},
            "Tlaxcala": {"lat": 19.3139, "lon": -98.2404, "region": "Centro"},

            # SUR / SURESTE
            "Veracruz": {"lat": 19.1738, "lon": -96.1342, "region": "Golfo"},
            "Tabasco": {"lat": 17.8409, "lon": -92.6189, "region": "Golfo"},
            "Guerrero": {"lat": 17.4392, "lon": -99.5451, "region": "Sur"},
            "Oaxaca": {"lat": 17.0732, "lon": -96.7266, "region": "Sur"},
            "Chiapas": {"lat": 16.7569, "lon": -93.1292, "region": "Sur"},
            "Campeche": {"lat": 19.8414, "lon": -90.5328, "region": "Peninsula"},
            "Yucatan": {"lat": 20.7099, "lon": -89.0943, "region": "Peninsula"},
            "Quintana Roo": {"lat": 19.1817, "lon": -88.4791, "region": "Peninsula"}
        }

    def _setup_directories(self):
        """Crea la estructura de carpetas si no existe"""
        for directory in [self.base_dir, self.hist_dir, self.fore_dir]:
            os.makedirs(directory, exist_ok=True)
            print(f"📁 Directorio verificado: {directory}")

    def fetch_data(self, url, params, state_name, context_tag):
        """Método genérico para fetching y formateo"""
        try:
            responses = self.openmeteo.weather_api(url, params=params)
            response = responses[0]
            
            # Procesar respuesta diaria
            daily = response.Daily()
            daily_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                    end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=daily.Interval()),
                    inclusive="left"
                )
            }
            
            for i, var in enumerate(self.daily_vars):
                # Verificar disponibilidad de variable en la respuesta
                try:
                    daily_data[var] = daily.Variables(i).ValuesAsNumpy()
                except Exception:
                    daily_data[var] = np.nan # Rellenar con Nan si falla

            df = pd.DataFrame(data=daily_data)
            
            # Añadir metadatos
            df['state'] = state_name
            df['region'] = self.states[state_name]['region']
            df['latitude'] = params['latitude']
            df['longitude'] = params['longitude']
            df['extraction_date'] = datetime.now().strftime("%Y-%m-%d")
            df['type'] = context_tag # 'historical' o 'forecast'

            # Formato de fecha limpio para CSV (YYYY-MM-DD)
            df['date'] = df['date'].dt.date
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error en {context_tag} para {state_name}: {e}")
            return None

    def process_all_states(self):
        print("\n" + "="*60)
        print("🚀 INICIANDO EXTRACCIÓN ESTRUCTURADA (Modo Seguro: Resume & Retry)")
        print("="*60)

        # Definir fechas
        today = datetime.now().date()
        
        # HISTÓRICO: 2021-01-01 hasta Ayer
        start_hist = "2021-01-01"
        end_hist = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # PRONÓSTICO: Hoy hasta +14 días
        start_fore = today.strftime("%Y-%m-%d")
        end_fore = (today + timedelta(days=14)).strftime("%Y-%m-%d")

        total_states = len(self.states)
        count = 0

        for state, coords in self.states.items():
            count += 1
            print(f"\n📍 Procesando {count}/{total_states}: {state}...")
            
            # --- 1. EXTRACCIÓN HISTÓRICA ---
            filename_hist = f"{state.replace(' ', '_')}_2021_2025.csv"
            path_hist = os.path.join(self.hist_dir, filename_hist)

            # Verificar si ya existe para no gastar API calls
            if os.path.exists(path_hist):
                print(f"   ⏩ Histórico ya existe (Saltando descarga)")
            else:
                hist_params = {
                    "latitude": coords["lat"], "longitude": coords["lon"],
                    "start_date": start_hist, "end_date": end_hist,
                    "daily": self.daily_vars, "timezone": "America/Mexico_City"
                }
                
                df_hist = self.fetch_data(self.archive_url, hist_params, state, "historical")
                
                if df_hist is not None:
                    df_hist.to_csv(path_hist, index=False)
                    print(f"   ✅ Histórico guardado: {len(df_hist)} registros")
                    # PAUSA LARGA DE SEGURIDAD DESPUÉS DE DESCARGA PESADA
                    time.sleep(5) 
                else:
                    print(f"   ❌ Falló descarga histórica (Reintentar más tarde)")

            # --- 2. EXTRACCIÓN PRONÓSTICO ---
            filename_fore = f"{state.replace(' ', '_')}_forecast_14d.csv"
            path_fore = os.path.join(self.fore_dir, filename_fore)

            if os.path.exists(path_fore):
                print(f"   ⏩ Pronóstico ya existe (Saltando descarga)")
            else:
                fore_params = {
                    "latitude": coords["lat"], "longitude": coords["lon"],
                    "start_date": start_fore, "end_date": end_fore,
                    "daily": self.daily_vars, "timezone": "America/Mexico_City"
                }

                df_fore = self.fetch_data(self.forecast_url, fore_params, state, "forecast")
                
                if df_fore is not None:
                    df_fore.to_csv(path_fore, index=False)
                    print(f"   ✅ Pronóstico guardado: {len(df_fore)} registros")
                    time.sleep(2) # Pausa leve para forecast

        print("\n" + "="*60)
        print("🎉 PROCESO COMPLETADO")
        print(f"📂 Verifica que tengas 32 archivos en: {self.hist_dir}")

if __name__ == "__main__":
    extractor = StructuredWeatherExtractor()
    extractor.process_all_states()
