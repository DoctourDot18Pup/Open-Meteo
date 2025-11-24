import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import logging
from datetime import datetime, timedelta
import time
import numpy as np

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EnhancedWeatherExtractor:
    def __init__(self):
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=retry_session)
        
        self.archive_url = "https://archive-api.open-meteo.com/v1/archive"
        self.forecast_url = "https://api.open-meteo.com/v1/forecast"
        
        # Variables diarias (solo las que funcionan en archive API)
        self.daily_variables = [
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "precipitation_sum", "wind_speed_10m_max", "shortwave_radiation_sum",
            "apparent_temperature_max", "apparent_temperature_min", 
            "sunrise", "sunset", "daylight_duration", "uv_index_max",
            "et0_fao_evapotranspiration"
        ]
        
        self.hourly_variables = [
            # Variables que SÍ funcionan en horarios
            "temperature_2m", "relative_humidity_2m", "dewpoint_2m", "precipitation", 
            "wind_speed_10m", "apparent_temperature", "pressure_msl", "cloud_cover",
            "soil_temperature_0cm", "evapotranspiration", "visibility", "is_day"
        ]
        
        # Estados clave normalizados
        self.key_states = {
            "Guanajuato": {"lat": 21.0190, "lon": -101.2574, "region": "Bajio_Agricola"},
            "Sinaloa": {"lat": 24.8069, "lon": -107.3940, "region": "Pacifico_Intensivo"},
            "Chiapas": {"lat": 16.7569, "lon": -93.1292, "region": "Sur_Tropical"},
            "Sonora": {"lat": 29.0729, "lon": -110.9559, "region": "Norte_Arido"},
            "Veracruz": {"lat": 19.1738, "lon": -96.1342, "region": "Golfo_Humedo"},
            "Queretaro": {"lat": 20.5888, "lon": -100.3899, "region": "Bajio_Templado"}
        }

    def get_enhanced_daily_data(self, state_name, coords, start_date, end_date):
        """
        Extrae datos diarios ampliados con todas las variables agrícolas
        """
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": start_date,
            "end_date": end_date,
            "daily": self.daily_variables,
            "timezone": "America/Mexico_City"
        }
        
        try:
            logger.info(f"📊 {state_name} - Datos diarios ampliados...")
            responses = self.openmeteo.weather_api(self.archive_url, params=params)
            response = responses[0]
            
            daily = response.Daily()
            daily_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                    end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=daily.Interval()),
                    inclusive="left"
                )
            }
            
            # Extraer todas las variables con manejo de errores
            for i, var in enumerate(self.daily_variables):
                try:
                    daily_data[var] = daily.Variables(i).ValuesAsNumpy()
                except:
                    daily_data[var] = np.full(len(daily_data["date"]), np.nan)
                    logger.warning(f"⚠️  Variable {var} no disponible para {state_name}")
            
            df = pd.DataFrame(data=daily_data)
            df['state'] = state_name
            df['latitude'] = coords["lat"]
            df['longitude'] = coords["lon"]
            df['region'] = coords["region"]
            df['elevation'] = response.Elevation()
            
            # Variables derivadas para agricultura
            df = self._add_agricultural_indices(df)
            
            logger.info(f"✅ {state_name}: {len(df)} registros con {len(self.daily_variables)} variables")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error {state_name}: {e}")
            return None

    def get_enhanced_hourly_data(self, state_name, coords, start_date, end_date):
        """
        Extrae datos horarios ampliados para análisis detallado
        """
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": start_date,
            "end_date": end_date,
            "hourly": self.hourly_variables,
            "timezone": "America/Mexico_City"
        }
        
        try:
            logger.info(f"🕐 {state_name} - Datos horarios ampliados...")
            responses = self.openmeteo.weather_api(self.archive_url, params=params)
            response = responses[0]
            
            hourly = response.Hourly()
            hourly_data = {
                "datetime": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left"
                )
            }
            
            for i, var in enumerate(self.hourly_variables):
                try:
                    hourly_data[var] = hourly.Variables(i).ValuesAsNumpy()
                except:
                    hourly_data[var] = np.full(len(hourly_data["datetime"]), np.nan)
                    logger.warning(f"⚠️  Variable horaria {var} no disponible para {state_name}")
            
            df = pd.DataFrame(data=hourly_data)
            df['state'] = state_name
            df['latitude'] = coords["lat"]
            df['longitude'] = coords["lon"]
            df['region'] = coords["region"]
            df['data_type'] = 'hourly'
            
            logger.info(f"✅ {state_name}: {len(df)} registros horarios")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error horarios {state_name}: {e}")
            return None

    def _add_agricultural_indices(self, df):
        """
        Calcula índices derivados importantes para agricultura
        """
        # Rango térmico diario
        if 'temperature_2m_max' in df.columns and 'temperature_2m_min' in df.columns:
            df['thermal_range'] = df['temperature_2m_max'] - df['temperature_2m_min']
        
        # Unidades de calor (base 10°C para maíz)
        if 'temperature_2m_max' in df.columns and 'temperature_2m_min' in df.columns:
            df['growing_degree_days'] = np.maximum(
                ((df['temperature_2m_max'] + df['temperature_2m_min']) / 2) - 10, 0
            )
        
        # Índice de estrés térmico (usando temp aparente si está disponible)
        if 'apparent_temperature_max' in df.columns:
            df['heat_stress_index'] = np.where(
                df['apparent_temperature_max'] > 35, 
                (df['apparent_temperature_max'] - 35) * 2,
                0
            )
        elif 'temperature_2m_max' in df.columns:
            df['heat_stress_index'] = np.where(
                df['temperature_2m_max'] > 35, 
                (df['temperature_2m_max'] - 35),
                0
            )
        
        # Índice de sequía simple
        if 'precipitation_sum' in df.columns:
            df['drought_days'] = (df['precipitation_sum'] < 1).astype(int)
            df['drought_streak'] = df.groupby(
                (df['drought_days'] != df['drought_days'].shift()).cumsum()
            )['drought_days'].cumsum()
        
        # Horas de luz útil para fotosíntesis
        if 'daylight_duration' in df.columns:
            df['useful_light_hours'] = df['daylight_duration'] / 3600  # convertir a horas
        
        # Eficiencia solar (radiación vs horas de luz)
        if 'shortwave_radiation_sum' in df.columns and 'daylight_duration' in df.columns:
            df['solar_efficiency'] = df['shortwave_radiation_sum'] / (df['daylight_duration'] / 3600 + 0.1)
        
        # Estación agrícola
        if 'date' in df.columns:
            df['month'] = df['date'].dt.month
            df['agricultural_season'] = df['month'].map({
                12: 'invierno', 1: 'invierno', 2: 'invierno',      # Diciembre-Febrero
                3: 'siembra', 4: 'siembra', 5: 'siembra',         # Marzo-Mayo
                6: 'desarrollo', 7: 'desarrollo', 8: 'desarrollo', # Junio-Agosto  
                9: 'cosecha', 10: 'cosecha', 11: 'cosecha'        # Septiembre-Noviembre
            })
        
        return df

    def analyze_agricultural_risks(self, df):
        """
        Genera análisis específico de riesgos agrícolas
        """
        print("\n" + "="*70)
        print("🌾 ANÁLISIS DE RIESGOS AGRÍCOLAS - DATOS AMPLIFICADOS")
        print("="*70)
        
        # Análisis por estado
        for state in df['state'].unique():
            state_data = df[df['state'] == state]
            print(f"\n🏛️  {state} ({state_data['region'].iloc[0]}):")
            
            # Riesgos de heladas
            if 'temperature_2m_min' in state_data.columns:
                frost_risk = (state_data['temperature_2m_min'] <= 0).sum()
                print(f"   ❄️  Días con riesgo de heladas: {frost_risk}")
            
            # Estrés térmico
            if 'heat_stress_index' in state_data.columns:
                heat_stress_days = (state_data['heat_stress_index'] > 5).sum()
                print(f"   🔥 Días con estrés térmico alto: {heat_stress_days}")
            
            # Sequías prolongadas
            if 'drought_streak' in state_data.columns:
                max_drought = state_data['drought_streak'].max()
                print(f"   🏜️  Máxima racha sin lluvia: {max_drought} días")
            
            # Condiciones ideales (solo con variables disponibles)
            conditions = []
            if 'temperature_2m_max' in state_data.columns:
                conditions.append(state_data['temperature_2m_max'].between(20, 30))
            if 'precipitation_sum' in state_data.columns:
                conditions.append(state_data['precipitation_sum'].between(2, 15))
            
            if conditions:
                optimal_days = pd.concat(conditions, axis=1).all(axis=1).sum()
                print(f"   ✅ Días con condiciones óptimas: {optimal_days}")
        
        print("="*70)

    def extract_comprehensive_dataset(self, days_back=30, include_hourly=False):
        """
        Extrae dataset completo con todas las variables ampliadas
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"🚀 EXTRACCIÓN COMPLETA AMPLIFICADA")
        logger.info(f"📅 Período: {start_date} to {end_date}")
        logger.info(f"📊 Variables diarias: {len(self.daily_variables)}")
        if include_hourly:
            logger.info(f"🕐 Variables horarias: {len(self.hourly_variables)}")
        
        daily_datasets = []
        hourly_datasets = []
        
        for state_name, coords in self.key_states.items():
            # Datos diarios ampliados
            daily_df = self.get_enhanced_daily_data(
                state_name, coords,
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d")
            )
            
            if daily_df is not None:
                daily_datasets.append(daily_df)
            
            # Datos horarios si se solicitan
            if include_hourly:
                hourly_df = self.get_enhanced_hourly_data(
                    state_name, coords,
                    (end_date - timedelta(days=7)).strftime("%Y-%m-%d"),  # Solo últimos 7 días
                    end_date.strftime("%Y-%m-%d")
                )
                
                if hourly_df is not None:
                    hourly_datasets.append(hourly_df)
            
            time.sleep(1)  # Rate limiting
        
        results = {}
        
        if daily_datasets:
            daily_combined = pd.concat(daily_datasets, ignore_index=True)
            results['daily_enhanced'] = daily_combined
            
            # Guardar datos diarios ampliados
            filename = f"mexico_weather_enhanced_daily_{datetime.now().strftime('%Y%m%d')}.csv"
            daily_combined.to_csv(filename, index=False)
            logger.info(f"💾 Datos diarios guardados: {filename}")
            
            # Análisis de riesgos
            self.analyze_agricultural_risks(daily_combined)
        
        if hourly_datasets:
            hourly_combined = pd.concat(hourly_datasets, ignore_index=True)
            results['hourly_enhanced'] = hourly_combined
            
            filename_hourly = f"mexico_weather_enhanced_hourly_{datetime.now().strftime('%Y%m%d')}.csv"
            hourly_combined.to_csv(filename_hourly, index=False)
            logger.info(f"💾 Datos horarios guardados: {filename_hourly}")
        
        return results

def main():
    extractor = EnhancedWeatherExtractor()
    
    print("🌾 EXTRACTOR METEOROLÓGICO AMPLIFICADO PARA AGRICULTURA")
    print("="*60)
    print(f"📊 Variables diarias: {len(extractor.daily_variables)}")
    print(f"🕐 Variables horarias: {len(extractor.hourly_variables)}")
    print("="*60)
    
    # Extracción completa
    results = extractor.extract_comprehensive_dataset(
        days_back=30, 
        include_hourly=True
    )
    
    if results:
        print(f"\n✅ EXTRACCIÓN COMPLETADA:")
        for data_type, df in results.items():
            print(f"   📄 {data_type}: {len(df)} registros, {len(df.columns)} columnas")
            
            # Mostrar nuevas variables
            new_vars = [col for col in df.columns if col not in 
                       ['date', 'datetime', 'state', 'latitude', 'longitude', 'region', 'elevation']]
            print(f"      Variables: {new_vars[:10]}...")  # Mostrar las primeras 10
        
        print(f"\n🎉 Dataset amplificado listo para PySpark y análisis avanzado!")

if __name__ == "__main__":
    main()
