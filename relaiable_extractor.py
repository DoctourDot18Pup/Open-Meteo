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

class ReliableWeatherExtractor32States:
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
            # Removido: relative_humidity_2m (NO disponible para datos diarios históricos)
        ]
        
        self.hourly_variables_verified = [
            # Variables horarias que funcionan garantizado
            "temperature_2m", "relative_humidity_2m", "dewpoint_2m", 
            "precipitation", "wind_speed_10m", "apparent_temperature", 
            "pressure_msl", "cloud_cover", "is_day"
        ]
        
        # LOS 32 ESTADOS DE MÉXICO CON SUS COORDENADAS GEOGRÁFICAS
        self.all_mexican_states = {
            # Región Norte
            "Baja California": {"lat": 32.6519, "lon": -115.4683, "region": "Norte_Pacifico", "capital": "Mexicali"},
            "Baja California Sur": {"lat": 26.0444, "lon": -111.6661, "region": "Norte_Pacifico", "capital": "La Paz"},
            "Chihuahua": {"lat": 28.6353, "lon": -106.0889, "region": "Norte_Desertico", "capital": "Chihuahua"},
            "Coahuila": {"lat": 27.0587, "lon": -101.7068, "region": "Norte_Centro", "capital": "Saltillo"},
            "Nuevo León": {"lat": 25.5922, "lon": -99.9962, "region": "Norte_Industrial", "capital": "Monterrey"},
            "Sonora": {"lat": 29.0729, "lon": -110.9559, "region": "Noroeste_Arido", "capital": "Hermosillo"},
            "Tamaulipas": {"lat": 24.2669, "lon": -98.8363, "region": "Golfo_Norte", "capital": "Ciudad Victoria"},
            
            # Región Centro-Norte
            "Aguascalientes": {"lat": 21.8853, "lon": -102.2916, "region": "Bajio_Norte", "capital": "Aguascalientes"},
            "Durango": {"lat": 24.0277, "lon": -104.6532, "region": "Norte_Montañoso", "capital": "Durango"},
            "San Luis Potosí": {"lat": 22.1565, "lon": -100.9855, "region": "Altiplano_Central", "capital": "San Luis Potosí"},
            "Zacatecas": {"lat": 22.7709, "lon": -102.5832, "region": "Altiplano_Norte", "capital": "Zacatecas"},
            
            # Región Centro
            "Ciudad de México": {"lat": 19.4326, "lon": -99.1332, "region": "Valle_Mexico", "capital": "Ciudad de México"},
            "Estado de México": {"lat": 19.3569, "lon": -99.6561, "region": "Valle_Mexico", "capital": "Toluca"},
            "Guanajuato": {"lat": 21.0190, "lon": -101.2574, "region": "Bajio_Central", "capital": "Guanajuato"},
            "Hidalgo": {"lat": 20.0910, "lon": -98.7624, "region": "Sierra_Oriental", "capital": "Pachuca"},
            "Morelos": {"lat": 18.6813, "lon": -99.1013, "region": "Sur_Centro", "capital": "Cuernavaca"},
            "Puebla": {"lat": 19.0414, "lon": -98.2063, "region": "Valle_Poblano", "capital": "Puebla"},
            "Querétaro": {"lat": 20.5888, "lon": -100.3899, "region": "Bajio_Oriental", "capital": "Santiago de Querétaro"},
            "Tlaxcala": {"lat": 19.3139, "lon": -98.2404, "region": "Altiplano_Oriental", "capital": "Tlaxcala"},
            
            # Región Occidente
            "Colima": {"lat": 19.2433, "lon": -103.7244, "region": "Pacifico_Centro", "capital": "Colima"},
            "Jalisco": {"lat": 20.6597, "lon": -103.3496, "region": "Occidente", "capital": "Guadalajara"},
            "Michoacán": {"lat": 19.5665, "lon": -101.7068, "region": "Pacifico_Michoacan", "capital": "Morelia"},
            "Nayarit": {"lat": 21.7514, "lon": -104.8455, "region": "Pacifico_Norte", "capital": "Tepic"},
            "Sinaloa": {"lat": 24.8069, "lon": -107.3940, "region": "Pacifico_Intensivo", "capital": "Culiacán"},
            
            # Región Sur
            "Chiapas": {"lat": 16.7569, "lon": -93.1292, "region": "Sur_Tropical", "capital": "Tuxtla Gutiérrez"},
            "Guerrero": {"lat": 17.4392, "lon": -99.5451, "region": "Pacifico_Sur", "capital": "Chilpancingo"},
            "Oaxaca": {"lat": 17.0732, "lon": -96.7266, "region": "Sur_Montañoso", "capital": "Oaxaca de Juárez"},
            
            # Región Golfo
            "Veracruz": {"lat": 19.1738, "lon": -96.1342, "region": "Golfo_Centro", "capital": "Xalapa"},
            "Tabasco": {"lat": 17.8409, "lon": -92.6189, "region": "Golfo_Tropical", "capital": "Villahermosa"},
            
            # Región Sureste
            "Campeche": {"lat": 19.8414, "lon": -90.5328, "region": "Peninsula_Oeste", "capital": "Campeche"},
            "Quintana Roo": {"lat": 19.1817, "lon": -88.4791, "region": "Peninsula_Caribe", "capital": "Chetumal"},
            "Yucatán": {"lat": 20.7099, "lon": -89.0943, "region": "Peninsula_Norte", "capital": "Mérida"}
        }

    def test_variable_availability_sample(self):
        """
        Prueba qué variables están disponibles en una muestra de estados
        """
        sample_states = ["Ciudad de México", "Guanajuato", "Yucatán", "Sonora", "Chiapas"]
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=3)
        
        print(f"\n🧪 PROBANDO DISPONIBILIDAD EN MUESTRA DE ESTADOS")
        print("="*60)
        
        working_states = []
        
        for state in sample_states:
            coords = self.all_mexican_states[state]
            
            try:
                params = {
                    "latitude": coords["lat"],
                    "longitude": coords["lon"],
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "daily": self.daily_variables_verified[:3],  # Solo las primeras 3
                    "timezone": "America/Mexico_City"
                }
                
                responses = self.openmeteo.weather_api(self.archive_url, params=params)
                response = responses[0]
                daily = response.Daily()
                
                # Verificar datos
                data = daily.Variables(0).ValuesAsNumpy()
                if not np.isnan(data).all():
                    working_states.append(state)
                    print(f"   ✅ {state} ({coords['region']})")
                else:
                    print(f"   ⚠️ {state} - datos vacíos")
                    
            except Exception as e:
                print(f"   ❌ {state} - error: {str(e)[:50]}...")
            
            time.sleep(0.5)
        
        print(f"\n📊 ESTADOS FUNCIONANDO: {len(working_states)}/{len(sample_states)}")
        return working_states

    def get_reliable_daily_data_all_states(self, start_date, end_date, batch_size=8):
        """
        Extrae datos diarios de todos los 32 estados en lotes
        """
        all_datasets = []
        states_list = list(self.all_mexican_states.items())
        total_states = len(states_list)
        
        print(f"🌎 EXTRAYENDO DATOS DE LOS 32 ESTADOS MEXICANOS")
        print(f"📅 Período: {start_date} a {end_date}")
        print("="*60)
        
        # Procesar en lotes para evitar sobrecarga
        for i in range(0, total_states, batch_size):
            batch = states_list[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_states // batch_size) + (1 if total_states % batch_size != 0 else 0)
            
            print(f"\n📦 LOTE {batch_num}/{total_batches}: {len(batch)} estados")
            print("-" * 40)
            
            for state_name, coords in batch:
                df = self.get_reliable_daily_data(state_name, coords, start_date, end_date)
                
                if df is not None:
                    all_datasets.append(df)
                    print(f"   ✅ {state_name}: {len(df)} registros")
                else:
                    print(f"   ❌ {state_name}: Error en extracción")
                
                time.sleep(0.8)  # Rate limiting más conservador
            
            # Pausa entre lotes
            if i + batch_size < total_states:
                print(f"⏳ Pausa de 3 segundos antes del siguiente lote...")
                time.sleep(3)
        
        return all_datasets

    def get_reliable_daily_data(self, state_name, coords, start_date, end_date):
        """
        Extrae datos diarios confiables para un estado específico
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
            logger.info(f"📊 {state_name} - Extrayendo datos diarios...")
            
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
            
            # Extraer variables
            variables_with_data = []
            for i, var in enumerate(self.daily_variables_verified):
                try:
                    data = daily.Variables(i).ValuesAsNumpy()
                    daily_data[var] = data
                    
                    # Verificar calidad de datos
                    non_nan_count = (~np.isnan(data)).sum()
                    if non_nan_count > 0:
                        variables_with_data.append(var)
                        
                except Exception as e:
                    logger.warning(f"   ⚠️ {var}: {e}")
                    daily_data[var] = np.full(len(daily_data["date"]), np.nan)
            
            df = pd.DataFrame(data=daily_data)
            df['state'] = state_name
            df['latitude'] = coords["lat"]
            df['longitude'] = coords["lon"]
            df['region'] = coords["region"]
            df['capital'] = coords["capital"]
            df['elevation'] = response.Elevation()
            df['variables_count'] = len(variables_with_data)
            
            # Agregar índices agrícolas
            df = self._add_agricultural_indices_safe(df)
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error {state_name}: {e}")
            return None

    def _add_agricultural_indices_safe(self, df):
        """
        Calcula índices agrícolas específicos para México
        """
        # Rangos térmicos
        if 'temperature_2m_max' in df.columns and 'temperature_2m_min' in df.columns:
            df['thermal_range'] = df['temperature_2m_max'] - df['temperature_2m_min']
            
            # Grados día de crecimiento (GDD) - Base 10°C para maíz
            df['growing_degree_days'] = np.maximum(
                ((df['temperature_2m_max'] + df['temperature_2m_min']) / 2) - 10, 0
            )
            
            # Unidades de frío para cultivos de invierno
            df['chill_hours_estimate'] = np.where(
                (df['temperature_2m_min'] >= 0) & (df['temperature_2m_max'] <= 7), 1, 0
            )
            
            # Riesgo de heladas (crítico para agricultura mexicana)
            df['frost_risk'] = np.where(df['temperature_2m_min'] <= 2, 1, 0)
            
        # Estrés térmico
        if 'apparent_temperature_max' in df.columns:
            df['heat_stress_risk'] = np.where(df['apparent_temperature_max'] > 35, 1, 0)
            df['extreme_heat_risk'] = np.where(df['apparent_temperature_max'] > 40, 1, 0)
        
        # Análisis de precipitación
        if 'precipitation_sum' in df.columns:
            df['is_dry_day'] = (df['precipitation_sum'] < 1).astype(int)
            df['is_rainy_day'] = (df['precipitation_sum'] > 10).astype(int)
            df['heavy_rain_day'] = (df['precipitation_sum'] > 25).astype(int)
        
        # Radiación solar y fotosíntesis
        if 'shortwave_radiation_sum' in df.columns:
            df['high_radiation_day'] = (df['shortwave_radiation_sum'] > 20).astype(int)
            df['optimal_radiation'] = np.where(
                (df['shortwave_radiation_sum'] >= 15) & (df['shortwave_radiation_sum'] <= 25), 1, 0
            )
        
        # Duración del día
        if 'daylight_duration' in df.columns:
            df['daylight_hours'] = df['daylight_duration'] / 3600
        
        # Variables temporales y estacionales
        if 'date' in df.columns:
            df['month'] = df['date'].dt.month
            df['day_of_year'] = df['date'].dt.dayofyear
            df['week_of_year'] = df['date'].dt.isocalendar().week
            
            # Estaciones agrícolas mexicanas
            df['agricultural_season'] = df['month'].map({
                12: 'invierno', 1: 'invierno', 2: 'invierno',
                3: 'primavera', 4: 'primavera', 5: 'primavera',
                6: 'verano', 7: 'verano', 8: 'verano',
                9: 'otoño', 10: 'otoño', 11: 'otoño'
            })
            
            # Época de lluvias/secas
            df['rainy_season'] = df['month'].map({
                6: 'lluvia', 7: 'lluvia', 8: 'lluvia', 9: 'lluvia',
                10: 'transicion', 11: 'seca', 12: 'seca',
                1: 'seca', 2: 'seca', 3: 'seca', 4: 'seca', 5: 'transicion'
            })
        
        return df

    def comprehensive_quality_report_32_states(self, daily_combined):
        """
        Genera reporte completo de calidad para los 32 estados
        """
        print("\n" + "="*80)
        print("📊 REPORTE DE CALIDAD DE DATOS - 32 ESTADOS MEXICANOS")
        print("="*80)
        
        # Estadísticas generales
        total_records = len(daily_combined)
        states_processed = daily_combined['state'].nunique()
        date_range = f"{daily_combined['date'].min().date()} a {daily_combined['date'].max().date()}"
        
        print(f"\n🌎 COBERTURA GEOGRÁFICA:")
        print(f"   Estados procesados: {states_processed}/32")
        print(f"   Total de registros: {total_records:,}")
        print(f"   Período de datos: {date_range}")
        
        # Completitud por estado
        print(f"\n📍 COMPLETITUD POR ESTADO:")
        state_completeness = daily_combined.groupby('state').size().sort_values(ascending=False)
        for state, count in state_completeness.head(10).items():
            print(f"   {state}: {count} registros")
        if len(state_completeness) > 10:
            print(f"   ... y {len(state_completeness) - 10} estados más")
        
        # Análisis por región climática
        print(f"\n🌡️ ANÁLISIS POR REGIÓN:")
        region_stats = daily_combined.groupby('region').agg({
            'state': 'count',
            'temperature_2m_mean': 'mean',
            'precipitation_sum': 'mean'
        }).round(2)
        
        for region, stats in region_stats.iterrows():
            print(f"   {region}: {stats['state']} registros, "
                  f"Temp promedio: {stats['temperature_2m_mean']:.1f}°C, "
                  f"Precipitación: {stats['precipitation_sum']:.1f}mm")
        
        # Calidad de variables principales
        print(f"\n✅ CALIDAD DE VARIABLES PRINCIPALES:")
        key_vars = ['temperature_2m_max', 'temperature_2m_min', 'precipitation_sum', 
                   'shortwave_radiation_sum', 'wind_speed_10m_max']
        
        for var in key_vars:
            if var in daily_combined.columns:
                non_null = daily_combined[var].notna().sum()
                completeness = (non_null / total_records) * 100
                
                if completeness > 95:
                    status = "✅"
                elif completeness > 85:
                    status = "⚠️"
                else:
                    status = "❌"
                
                print(f"   {status} {var}: {completeness:.1f}% completo ({non_null:,}/{total_records:,})")
        
        # Valores extremos por región
        print(f"\n🌡️ RANGOS DE TEMPERATURA POR REGIÓN:")
        temp_ranges = daily_combined.groupby('region').agg({
            'temperature_2m_min': 'min',
            'temperature_2m_max': 'max'
        }).round(1)
        
        for region, temps in temp_ranges.iterrows():
            print(f"   {region}: {temps['temperature_2m_min']}°C a {temps['temperature_2m_max']}°C")
        
        print("="*80)

def main():
    """
    Extractor meteorológico para los 32 estados mexicanos
    """
    extractor = ReliableWeatherExtractor32States()
    
    print("🇲🇽 EXTRACTOR METEOROLÓGICO - 32 ESTADOS DE MÉXICO")
    print("="*60)
    print("🎯 Objetivo: Datos meteorológicos completos y confiables")
    print("🧪 Con validación exhaustiva de calidad")
    print("="*60)
    
    # 1. Prueba de disponibilidad en muestra
    print(f"\n🧪 FASE 1: PRUEBA DE DISPONIBILIDAD")
    working_sample = extractor.test_variable_availability_sample()
    
    if len(working_sample) < 3:
        print(f"\n❌ Error: Solo {len(working_sample)} estados funcionando en la muestra")
        print("Revisa tu conexión a internet y la API de Open-Meteo")
        return
    
    # 2. Configurar período de extracción
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)  # Últimos 30 días
    
    print(f"\n📅 FASE 2: EXTRACCIÓN COMPLETA")
    print(f"Período: {start_date} a {end_date}")
    
    # 3. Extraer datos de todos los estados
    daily_datasets = extractor.get_reliable_daily_data_all_states(
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
        batch_size=6  # Lotes más pequeños para mayor estabilidad
    )
    
    # 4. Combinar y procesar resultados
    if daily_datasets:
        daily_combined = pd.concat(daily_datasets, ignore_index=True)
        
        # Guardar archivo principal
        timestamp = datetime.now().strftime('%Y%m%d_%H%M')
        filename_daily = f"mexico_32_estados_daily_{timestamp}.csv"
        daily_combined.to_csv(filename_daily, index=False)
        
        print(f"\n💾 ARCHIVO GENERADO: {filename_daily}")
        print(f"📊 {len(daily_combined):,} registros totales")
        print(f"🌎 {daily_combined['state'].nunique()} estados procesados")
        
        # Generar reporte de calidad
        extractor.comprehensive_quality_report_32_states(daily_combined)
        
        # Estadísticas rápidas
        print(f"\n📈 ESTADÍSTICAS RÁPIDAS:")
        print(f"   Temperatura promedio nacional: {daily_combined['temperature_2m_mean'].mean():.1f}°C")
        print(f"   Precipitación total: {daily_combined['precipitation_sum'].sum():.1f}mm")
        print(f"   Estados con datos de heladas: {daily_combined['frost_risk'].sum()}")
        print(f"   Días con estrés térmico: {daily_combined['heat_stress_risk'].sum()}")
