import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import logging
from datetime import datetime, timedelta
import time
import numpy as np

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OpenMeteoExtractor:
    def __init__(self):
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)  # 1 hour cache
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=retry_session)
        
        # URLs para diferentes tipos de datos
        self.archive_url = "https://archive-api.open-meteo.com/v1/archive"
        self.forecast_url = "https://api.open-meteo.com/v1/forecast"
        
        # Coordenadas de las capitales de estados mexicanos
        self.mexican_states = {
            "Aguascalientes": {"lat": 21.8853, "lon": -102.2916},
            "Baja California": {"lat": 32.6519, "lon": -115.4683},
            "Baja California Sur": {"lat": 24.1444, "lon": -110.3128},
            "Campeche": {"lat": 19.8414, "lon": -90.5328},
            "Chiapas": {"lat": 16.7569, "lon": -93.1292},
            "Chihuahua": {"lat": 28.6353, "lon": -106.0889},
            "Ciudad de México": {"lat": 19.4326, "lon": -99.1332},
            "Coahuila": {"lat": 25.4232, "lon": -101.0053},
            "Colima": {"lat": 19.2433, "lon": -103.7244},
            "Durango": {"lat": 24.0277, "lon": -104.6532},
            "Guanajuato": {"lat": 21.0190, "lon": -101.2574},
            "Guerrero": {"lat": 17.4392, "lon": -99.5451},
            "Hidalgo": {"lat": 20.0910, "lon": -98.7624},
            "Jalisco": {"lat": 20.6597, "lon": -103.3496},
            "Estado de México": {"lat": 19.3569, "lon": -99.6561},
            "Michoacán": {"lat": 19.5665, "lon": -101.7068},
            "Morelos": {"lat": 18.6813, "lon": -99.1013},
            "Nayarit": {"lat": 21.7514, "lon": -104.8455},
            "Nuevo León": {"lat": 25.5922, "lon": -99.9962},
            "Oaxaca": {"lat": 17.0732, "lon": -96.7266},
            "Puebla": {"lat": 19.0414, "lon": -98.2063},
            "Querétaro": {"lat": 20.5888, "lon": -100.3899},
            "Quintana Roo": {"lat": 19.1817, "lon": -88.4791},
            "San Luis Potosí": {"lat": 22.1565, "lon": -100.9855},
            "Sinaloa": {"lat": 24.8069, "lon": -107.3940},
            "Sonora": {"lat": 29.0729, "lon": -110.9559},
            "Tabasco": {"lat": 17.8409, "lon": -92.6189},
            "Tamaulipas": {"lat": 24.2669, "lon": -98.8363},
            "Tlaxcala": {"lat": 19.3139, "lon": -98.2404},
            "Veracruz": {"lat": 19.1738, "lon": -96.1342},
            "Yucatán": {"lat": 20.7099, "lon": -89.0943},
            "Zacatecas": {"lat": 22.7709, "lon": -102.5832}
        }

    def get_historical_weather(self, state_name, lat, lon, start_date, end_date, data_type="daily"):
        """
        Obtiene datos meteorológicos históricos usando la librería oficial de OpenMeteo
        """
        if data_type == "daily":
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": start_date,
                "end_date": end_date,
                "daily": [
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "precipitation_sum",
                    "wind_speed_10m_max",
                    "shortwave_radiation_sum"
                ],
                "timezone": "America/Mexico_City"
            }
        else:  # hourly
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": start_date,
                "end_date": end_date,
                "hourly": [
                    "temperature_2m",
                    "relative_humidity_2m",
                    "precipitation",
                    "wind_speed_10m",
                    "shortwave_radiation"
                ],
                "timezone": "America/Mexico_City"
            }
        
        try:
            logger.info(f"Obteniendo datos históricos ({data_type}) para {state_name}...")
            
            responses = self.openmeteo.weather_api(self.archive_url, params=params)
            response = responses[0]
            
            logger.info(f"Coordenadas: {response.Latitude()}°N {response.Longitude()}°E")
            logger.info(f"Elevación: {response.Elevation()} m")
            
            if data_type == "daily":
                return self._process_daily_data(response, state_name, lat, lon)
            else:
                return self._process_hourly_data(response, state_name, lat, lon)
                
        except Exception as e:
            logger.error(f"Error obteniendo datos históricos para {state_name}: {e}")
            return None

    def _process_daily_data(self, response, state_name, lat, lon):
        """Procesa datos diarios de la respuesta"""
        daily = response.Daily()
        
        # Crear el DataFrame con fechas
        daily_data = {
            "date": pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left"
            )
        }
        
        # Extraer variables meteorológicas
        variables = [
            "temperature_2m_max",
            "temperature_2m_min", 
            "precipitation_sum",
            "wind_speed_10m_max",
            "shortwave_radiation_sum"
        ]
        
        for i, var in enumerate(variables):
            try:
                daily_data[var] = daily.Variables(i).ValuesAsNumpy()
            except:
                daily_data[var] = np.full(len(daily_data["date"]), np.nan)
        
        # Crear DataFrame
        df = pd.DataFrame(data=daily_data)
        df['state'] = state_name
        df['latitude'] = lat
        df['longitude'] = lon
        df['data_type'] = 'daily'
        
        return df

    def _process_hourly_data(self, response, state_name, lat, lon):
        """Procesa datos horarios de la respuesta"""
        hourly = response.Hourly()
        
        # Crear el DataFrame con fechas
        hourly_data = {
            "datetime": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )
        }
        
        # Extraer variables meteorológicas
        variables = [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "wind_speed_10m",
            "shortwave_radiation"
        ]
        
        for i, var in enumerate(variables):
            try:
                hourly_data[var] = hourly.Variables(i).ValuesAsNumpy()
            except:
                hourly_data[var] = np.full(len(hourly_data["datetime"]), np.nan)
        
        # Crear DataFrame
        df = pd.DataFrame(data=hourly_data)
        df['state'] = state_name
        df['latitude'] = lat
        df['longitude'] = lon
        df['data_type'] = 'hourly'
        
        return df

    def get_current_weather(self, state_name, lat, lon):
        """
        Obtiene datos meteorológicos actuales y pronóstico
        """
        params = {
            "latitude": lat,
            "longitude": lon,
            "current": ["temperature_2m", "relative_humidity_2m", "precipitation", "weather_code"],
            "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"],
            "timezone": "America/Mexico_City",
            "forecast_days": 7
        }
        
        try:
            logger.info(f"Obteniendo pronóstico para {state_name}...")
            
            responses = self.openmeteo.weather_api(self.forecast_url, params=params)
            response = responses[0]
            
            # Datos actuales
            current = response.Current()
            current_data = {
                "datetime": [pd.to_datetime(current.Time(), unit="s", utc=True)],
                "temperature_2m": [current.Variables(0).Value()],
                "relative_humidity_2m": [current.Variables(1).Value()],
                "precipitation": [current.Variables(2).Value()],
                "weather_code": [current.Variables(3).Value()],
                "state": [state_name],
                "latitude": [lat],
                "longitude": [lon],
                "data_type": ["current"]
            }
            
            current_df = pd.DataFrame(current_data)
            
            # Pronóstico diario
            daily = response.Daily()
            daily_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                    end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=daily.Interval()),
                    inclusive="left"
                ),
                "temperature_2m_max": daily.Variables(0).ValuesAsNumpy(),
                "temperature_2m_min": daily.Variables(1).ValuesAsNumpy(),
                "precipitation_sum": daily.Variables(2).ValuesAsNumpy()
            }
            
            forecast_df = pd.DataFrame(daily_data)
            forecast_df['state'] = state_name
            forecast_df['latitude'] = lat
            forecast_df['longitude'] = lon
            forecast_df['data_type'] = 'forecast'
            
            return current_df, forecast_df
            
        except Exception as e:
            logger.error(f"Error obteniendo pronóstico para {state_name}: {e}")
            return None, None

    def extract_sample_states_data(self, sample_states=5, days_back=30, include_hourly=False):
        """
        Extrae datos para una muestra de estados (para pruebas iniciales)
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        sample_state_names = list(self.mexican_states.keys())[:sample_states]
        all_daily_data = []
        all_hourly_data = []
        all_current_data = []
        all_forecast_data = []
        
        for state in sample_state_names:
            coords = self.mexican_states[state]
            
            # Datos históricos diarios
            daily_df = self.get_historical_weather(
                state,
                coords["lat"],
                coords["lon"],
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
                "daily"
            )
            
            if daily_df is not None:
                all_daily_data.append(daily_df)
            
            # Datos históricos horarios (solo últimos 7 días para no sobrecargar)
            if include_hourly:
                hourly_start = end_date - timedelta(days=7)
                hourly_df = self.get_historical_weather(
                    state,
                    coords["lat"],
                    coords["lon"],
                    hourly_start.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                    "hourly"
                )
                
                if hourly_df is not None:
                    all_hourly_data.append(hourly_df)
            
            # Datos actuales y pronóstico
            current_df, forecast_df = self.get_current_weather(
                state,
                coords["lat"],
                coords["lon"]
            )
            
            if current_df is not None:
                all_current_data.append(current_df)
            if forecast_df is not None:
                all_forecast_data.append(forecast_df)
            
            # Pausa para evitar rate limiting
            time.sleep(1)
        
        # Combinar datos
        results = {}
        
        if all_daily_data:
            results['daily'] = pd.concat(all_daily_data, ignore_index=True)
            logger.info(f"Datos diarios extraídos para {len(all_daily_data)} estados")
        
        if all_hourly_data:
            results['hourly'] = pd.concat(all_hourly_data, ignore_index=True)
            logger.info(f"Datos horarios extraídos para {len(all_hourly_data)} estados")
        
        if all_current_data:
            results['current'] = pd.concat(all_current_data, ignore_index=True)
            logger.info(f"Datos actuales extraídos para {len(all_current_data)} estados")
        
        if all_forecast_data:
            results['forecast'] = pd.concat(all_forecast_data, ignore_index=True)
            logger.info(f"Pronósticos extraídos para {len(all_forecast_data)} estados")
        
        return results

    def validate_data_quality(self, df, data_name="datos"):
        """
        Valida la calidad e integridad de los datos extraídos
        """
        logger.info(f"=== VALIDACIÓN DE CALIDAD: {data_name.upper()} ===")
        
        if df is None or df.empty:
            print(f"No hay datos para validar en {data_name}")
            return False
        
        # Información básica
        print(f"Total de registros: {len(df)}")
        print(f"Estados únicos: {df['state'].nunique()}")
        
        # Rango de fechas
        date_col = 'date' if 'date' in df.columns else 'datetime'
        if date_col in df.columns:
            print(f"Rango de fechas: {df[date_col].min()} a {df[date_col].max()}")
        
        # Tipos de datos
        if 'data_type' in df.columns:
            print(f"Tipos de datos: {df['data_type'].unique()}")
        
        # Verificar valores nulos
        print(f"\n--- Valores Nulos por Columna ---")
        null_counts = df.isnull().sum()
        for col, count in null_counts.items():
            if count > 0:
                percentage = (count / len(df)) * 100
                print(f"{col}: {count} ({percentage:.2f}%)")
        
        # Estadísticas descriptivas para variables numéricas
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        if len(numeric_cols) > 0:
            print(f"\n--- Estadísticas Descriptivas ---")
            print(df[numeric_cols].describe())
        
        # Verificar rangos de datos sospechosos
        print(f"\n--- Verificación de Rangos ---")
        
        # Temperaturas
        temp_cols = [col for col in df.columns if 'temperature' in col.lower()]
        for col in temp_cols:
            if col in df.columns:
                temp_max = df[col].max()
                temp_min = df[col].min()
                print(f"{col} - Max: {temp_max:.2f}°C, Min: {temp_min:.2f}°C")
                
                if temp_max > 60 or temp_min < -50:
                    logger.warning(f"Temperaturas fuera de rango en {col}")
        
        # Precipitación
        precip_cols = [col for col in df.columns if 'precipitation' in col.lower()]
        for col in precip_cols:
            if col in df.columns:
                precip_max = df[col].max()
                print(f"{col} - Max: {precip_max:.2f} mm")
                
                if precip_max > 500:
                    logger.warning(f"Precipitación excesiva en {col}")
        
        print(f"\n✅ Validación completada para {data_name}")
        return True

def main():
    """
    Función principal para probar la extracción de datos
    """
    extractor = OpenMeteoExtractor()
    
    print("=== EXTRACTOR DE DATOS OPEN-METEO (Versión Oficial) ===")
    print("Iniciando extracción de datos meteorológicos...\n")
    
    # Extraer datos para muestra de estados
    print("Extrayendo datos para muestra de estados...")
    results = extractor.extract_sample_states_data(
        sample_states=5,
        days_back=30,
        include_hourly=True
    )
    
    if results:
        print("✅ Datos extraídos exitosamente\n")
        
        # Validar y guardar cada tipo de datos
        for data_type, df in results.items():
            if df is not None and not df.empty:
                print(f"\n=== PROCESANDO {data_type.upper()} ===")
                
                # Validar datos
                extractor.validate_data_quality(df, data_type)
                
                # Guardar datos
                filename = f"openmeteo_{data_type}_mexico.csv"
                df.to_csv(filename, index=False)
                print(f"📁 Datos guardados en: {filename}")
                
                # Mostrar muestra
                print(f"\n--- Muestra de {data_type} ---")
                print(df.head(3))
                print(f"Columnas: {list(df.columns)}")
        
        print(f"\n🎉 Proceso completado exitosamente!")
        print(f"Archivos generados: {len(results)} tipos de datos")
        
    else:
        print("❌ Error extrayendo datos")

if __name__ == "__main__":
    main()
