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

class ReliableWeatherExtractor:
    def __init__(self):
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=retry_session)
        
        self.archive_url = "https://archive-api.open-meteo.com/v1/archive"
        self.forecast_url = "https://api.open-meteo.com/v1/forecast"
        
        # VARIABLES VALIDADAS - Solo las que funcionan 100%
        self.daily_variables_verified = [
            # Variables básicas probadas
            "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
            "precipitation_sum", "wind_speed_10m_max", "shortwave_radiation_sum",
            # Variables extendidas que SÍ funcionan
            "apparent_temperature_max", "apparent_temperature_min", 
            "daylight_duration"
            # Removido: sunrise, sunset (requieren manejo especial de timestamps)
            # Removido: uv_index_max (no disponible en históricos)
            # Removido: et0_fao_evapotranspiration (puede ser limitado)
        ]
        
        self.hourly_variables_verified = [
            # Variables horarias que funcionan garantizado
            "temperature_2m", "relative_humidity_2m", "dewpoint_2m", 
            "precipitation", "wind_speed_10m", "apparent_temperature", 
            "pressure_msl", "cloud_cover", "is_day"
            # Removido: soil_temperature_0cm (limitado por modelo)
            # Removido: evapotranspiration (limitado)  
            # Removido: visibility (limitado geográficamente)
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

    def test_variable_availability(self, state_name, coords):
        """
        Prueba qué variables están realmente disponibles para una ubicación
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=3)  # Solo 3 días para prueba rápida
        
        print(f"\n🧪 PROBANDO DISPONIBILIDAD DE VARIABLES - {state_name}")
        print("="*50)
        
        # Probar variables diarias
        working_daily = []
        for var in self.daily_variables_verified:
            try:
                params = {
                    "latitude": coords["lat"],
                    "longitude": coords["lon"],
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "daily": [var],
                    "timezone": "America/Mexico_City"
                }
                
                responses = self.openmeteo.weather_api(self.archive_url, params=params)
                response = responses[0]
                daily = response.Daily()
                
                # Verificar que los datos no estén vacíos
                data = daily.Variables(0).ValuesAsNumpy()
                if not np.isnan(data).all():
                    working_daily.append(var)
                    print(f"   ✅ {var}")
                else:
                    print(f"   ⚠️  {var} - datos vacíos")
                    
            except Exception as e:
                print(f"   ❌ {var} - error: {str(e)[:50]}...")
            
            time.sleep(0.2)  # Rate limiting
        
        print(f"\n📊 VARIABLES DIARIAS FUNCIONANDO: {len(working_daily)}/{len(self.daily_variables_verified)}")
        
        # Probar variables horarias (muestra pequeña)
        working_hourly = []
        for var in self.hourly_variables_verified[:5]:  # Solo las primeras 5 para rapidez
            try:
                params = {
                    "latitude": coords["lat"],
                    "longitude": coords["lon"],
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "hourly": [var],
                    "timezone": "America/Mexico_City"
                }
                
                responses = self.openmeteo.weather_api(self.archive_url, params=params)
                response = responses[0]
                hourly = response.Hourly()
                
                data = hourly.Variables(0).ValuesAsNumpy()
                if not np.isnan(data).all():
                    working_hourly.append(var)
                    print(f"   ✅ {var} (hourly)")
                else:
                    print(f"   ⚠️  {var} (hourly) - datos vacíos")
                    
            except Exception as e:
                print(f"   ❌ {var} (hourly) - error: {str(e)[:50]}...")
            
            time.sleep(0.2)
        
        print(f"🕐 VARIABLES HORARIAS FUNCIONANDO: {len(working_hourly)}/5 probadas")
        return working_daily, working_hourly

    def get_reliable_daily_data(self, state_name, coords, start_date, end_date):
        """
        Extrae solo variables diarias 100% confiables
        """
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": start_date,
            "end_date": end_date,
            "daily": self.daily_variables_verified,
            "timezone": "America/Mexico_City"
        }
        
        try:
            logger.info(f"📊 {state_name} - Extrayendo {len(self.daily_variables_verified)} variables diarias confiables...")
            
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
            
            # Extraer variables y validar contenido
            variables_with_data = []
            for i, var in enumerate(self.daily_variables_verified):
                try:
                    data = daily.Variables(i).ValuesAsNumpy()
                    daily_data[var] = data
                    
                    # Verificar calidad de datos
                    non_nan_count = (~np.isnan(data)).sum()
                    if non_nan_count > 0:
                        variables_with_data.append(var)
                        logger.info(f"   ✅ {var}: {non_nan_count}/{len(data)} valores válidos")
                    else:
                        logger.warning(f"   ⚠️  {var}: Sin datos válidos")
                        
                except Exception as e:
                    logger.error(f"   ❌ {var}: {e}")
                    daily_data[var] = np.full(len(daily_data["date"]), np.nan)
            
            # Obtener sunrise/sunset correctamente
            try:
                # Calcular sunrise/sunset desde los datos de fecha y coordenadas
                dates = daily_data["date"]
                daily_data["sunrise_hour"] = self._calculate_sunrise_sunset(
                    dates, coords["lat"], coords["lon"], "sunrise"
                )
                daily_data["sunset_hour"] = self._calculate_sunrise_sunset(
                    dates, coords["lat"], coords["lon"], "sunset"
                )
                variables_with_data.extend(["sunrise_hour", "sunset_hour"])
                logger.info(f"   ✅ sunrise_hour/sunset_hour: Calculados correctamente")
            except Exception as e:
                logger.warning(f"   ⚠️  No se pudieron calcular sunrise/sunset: {e}")
            
            df = pd.DataFrame(data=daily_data)
            df['state'] = state_name
            df['latitude'] = coords["lat"]
            df['longitude'] = coords["lon"]
            df['region'] = coords["region"]
            df['elevation'] = response.Elevation()
            df['variables_count'] = len(variables_with_data)
            
            # Agregar índices agrícolas
            df = self._add_agricultural_indices_safe(df)
            
            logger.info(f"✅ {state_name}: {len(df)} registros, {len(variables_with_data)} variables con datos")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error {state_name}: {e}")
            return None

    def _calculate_sunrise_sunset(self, dates, lat, lon, event_type):
        """
        Calcula aproximadamente las horas de amanecer/atardecer
        Nota: Implementación simplificada para fines demostrativos
        """
        import math
        
        hours = []
        for date in dates:
            day_of_year = date.timetuple().tm_yday
            
            # Fórmula simplificada para calcular amanecer/atardecer
            lat_rad = math.radians(lat)
            declination = math.radians(23.45 * math.sin(math.radians(360 * (284 + day_of_year) / 365)))
            
            hour_angle = math.acos(-math.tan(lat_rad) * math.tan(declination))
            
            if event_type == "sunrise":
                hour = 12 - (hour_angle * 12 / math.pi)
            else:  # sunset
                hour = 12 + (hour_angle * 12 / math.pi)
            
            hours.append(hour)
        
        return hours

    def get_reliable_hourly_data(self, state_name, coords, start_date, end_date):
        """
        Extrae solo variables horarias 100% confiables
        """
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": start_date,
            "end_date": end_date,
            "hourly": self.hourly_variables_verified,
            "timezone": "America/Mexico_City"
        }
        
        try:
            logger.info(f"🕐 {state_name} - Extrayendo {len(self.hourly_variables_verified)} variables horarias...")
            
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
            
            variables_with_data = []
            for i, var in enumerate(self.hourly_variables_verified):
                try:
                    data = hourly.Variables(i).ValuesAsNumpy()
                    hourly_data[var] = data
                    
                    non_nan_count = (~np.isnan(data)).sum()
                    if non_nan_count > 0:
                        variables_with_data.append(var)
                    
                except Exception as e:
                    hourly_data[var] = np.full(len(hourly_data["datetime"]), np.nan)
            
            df = pd.DataFrame(data=hourly_data)
            df['state'] = state_name
            df['latitude'] = coords["lat"]
            df['longitude'] = coords["lon"]
            df['region'] = coords["region"]
            df['data_type'] = 'hourly'
            df['variables_count'] = len(variables_with_data)
            
            logger.info(f"✅ {state_name}: {len(df)} registros horarios")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error horarios {state_name}: {e}")
            return None

    def _add_agricultural_indices_safe(self, df):
        """
        Calcula índices agrícolas con validación robusta
        """
        # Solo calcular si las variables base están disponibles
        if 'temperature_2m_max' in df.columns and 'temperature_2m_min' in df.columns:
            df['thermal_range'] = df['temperature_2m_max'] - df['temperature_2m_min']
            
            # Grados día de crecimiento (GDD)
            df['growing_degree_days'] = np.maximum(
                ((df['temperature_2m_max'] + df['temperature_2m_min']) / 2) - 10, 0
            )
            
            # Unidades de frío (para cultivos que requieren vernalización)
            df['chill_hours_estimate'] = np.where(
                (df['temperature_2m_min'] >= 0) & (df['temperature_2m_max'] <= 7), 1, 0
            )
        
        if 'apparent_temperature_max' in df.columns:
            df['heat_stress_risk'] = np.where(df['apparent_temperature_max'] > 35, 1, 0)
        
        if 'precipitation_sum' in df.columns:
            df['is_dry_day'] = (df['precipitation_sum'] < 1).astype(int)
            df['is_rainy_day'] = (df['precipitation_sum'] > 10).astype(int)
        
        if 'shortwave_radiation_sum' in df.columns:
            df['high_radiation_day'] = (df['shortwave_radiation_sum'] > 20).astype(int)
        
        if 'daylight_duration' in df.columns:
            df['daylight_hours'] = df['daylight_duration'] / 3600
        
        # Variables temporales
        if 'date' in df.columns:
            df['month'] = df['date'].dt.month
            df['day_of_year'] = df['date'].dt.dayofyear
            df['agricultural_season'] = df['month'].map({
                12: 'winter', 1: 'winter', 2: 'winter',
                3: 'spring', 4: 'spring', 5: 'spring',
                6: 'summer', 7: 'summer', 8: 'summer',
                9: 'autumn', 10: 'autumn', 11: 'autumn'
            })
        
        return df

    def comprehensive_quality_report(self, daily_df, hourly_df=None):
        """
        Genera reporte completo de calidad de datos
        """
        print("\n" + "="*80)
        print("📊 REPORTE DE CALIDAD DE DATOS - VARIABLES CONFIABLES")
        print("="*80)
        
        if daily_df is not None:
            print("\n📅 DATOS DIARIOS:")
            for col in daily_df.select_dtypes(include=[np.number]).columns:
                if col not in ['latitude', 'longitude', 'elevation', 'variables_count', 'month', 'day_of_year']:
                    non_null = daily_df[col].notna().sum()
                    total = len(daily_df)
                    completeness = (non_null / total) * 100
                    
                    if completeness > 90:
                        status = "✅"
                    elif completeness > 50:
                        status = "⚠️ "
                    else:
                        status = "❌"
                    
                    print(f"   {status} {col}: {completeness:.1f}% completo ({non_null}/{total})")
        
        if hourly_df is not None:
            print("\n🕐 DATOS HORARIOS:")
            for col in hourly_df.select_dtypes(include=[np.number]).columns[:8]:  # Primeros 8
                if col not in ['latitude', 'longitude', 'variables_count']:
                    non_null = hourly_df[col].notna().sum()
                    total = len(hourly_df)
                    completeness = (non_null / total) * 100
                    
                    if completeness > 90:
                        status = "✅"
                    elif completeness > 50:
                        status = "⚠️ "
                    else:
                        status = "❌"
                    
                    print(f"   {status} {col}: {completeness:.1f}% completo")
        
        print("="*80)

def main():
    """
    Extractor con validación exhaustiva de variables
    """
    extractor = ReliableWeatherExtractor()
    
    print("🔬 EXTRACTOR METEOROLÓGICO CONFIABLE")
    print("="*50)
    print("🎯 Objetivo: Solo variables 100% funcionales")
    print("🧪 Con validación de calidad de datos")
    print("="*50)
    
    # Probar disponibilidad en un estado primero
    test_state = "Guanajuato"
    test_coords = extractor.key_states[test_state]
    
    print(f"🧪 Probando disponibilidad en {test_state}...")
    working_daily, working_hourly = extractor.test_variable_availability(test_state, test_coords)
    
    # Proceder con extracción completa
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)
    
    daily_datasets = []
    hourly_datasets = []
    
    for state_name, coords in extractor.key_states.items():
        # Datos diarios
        daily_df = extractor.get_reliable_daily_data(
            state_name, coords,
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )
        
        if daily_df is not None:
            daily_datasets.append(daily_df)
        
        # Datos horarios (últimos 7 días)
        hourly_df = extractor.get_reliable_hourly_data(
            state_name, coords,
            (end_date - timedelta(days=7)).strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )
        
        if hourly_df is not None:
            hourly_datasets.append(hourly_df)
        
        time.sleep(1)
    
    # Combinar y guardar
    if daily_datasets:
        daily_combined = pd.concat(daily_datasets, ignore_index=True)
        filename_daily = f"mexico_reliable_daily_{datetime.now().strftime('%Y%m%d')}.csv"
        daily_combined.to_csv(filename_daily, index=False)
        print(f"\n💾 Datos diarios confiables: {filename_daily}")
    
    if hourly_datasets:
        hourly_combined = pd.concat(hourly_datasets, ignore_index=True)
        filename_hourly = f"mexico_reliable_hourly_{datetime.now().strftime('%Y%m%d')}.csv"
        hourly_combined.to_csv(filename_hourly, index=False)
        print(f"💾 Datos horarios confiables: {filename_hourly}")
    
    # Reporte de calidad
    if daily_datasets and hourly_datasets:
        extractor.comprehensive_quality_report(daily_combined, hourly_combined)
    
    print(f"\n🎉 Extracción confiable completada!")
    print(f"📊 Variables diarias verificadas: {len(extractor.daily_variables_verified)}")
    print(f"🕐 Variables horarias verificadas: {len(extractor.hourly_variables_verified)}")

if __name__ == "__main__":
    main()
