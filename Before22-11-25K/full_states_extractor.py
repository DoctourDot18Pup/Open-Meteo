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

class FullStatesExtractor:
    def __init__(self):
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
        retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
        self.openmeteo = openmeteo_requests.Client(session=retry_session)
        
        self.archive_url = "https://archive-api.open-meteo.com/v1/archive"
        self.forecast_url = "https://api.open-meteo.com/v1/forecast"
        
        # TODOS los 32 estados mexicanos
        self.mexican_states = {
            "Aguascalientes": {"lat": 21.8853, "lon": -102.2916},
            "Baja California": {"lat": 32.6519, "lon": -115.4683},
            "Baja California Sur": {"lat": 24.1444, "lon": -110.3128},
            "Campeche": {"lat": 19.8414, "lon": -90.5328},
            "Chiapas": {"lat": 16.7569, "lon": -93.1292},
            "Chihuahua": {"lat": 28.6353, "lon": -106.0889},
            "Ciudad de Mexico": {"lat": 19.4326, "lon": -99.1332},
            "Coahuila": {"lat": 25.4232, "lon": -101.0053},
            "Colima": {"lat": 19.2433, "lon": -103.7244},
            "Durango": {"lat": 24.0277, "lon": -104.6532},
            "Guanajuato": {"lat": 21.0190, "lon": -101.2574},
            "Guerrero": {"lat": 17.4392, "lon": -99.5451},
            "Hidalgo": {"lat": 20.0910, "lon": -98.7624},
            "Jalisco": {"lat": 20.6597, "lon": -103.3496},
            "Estado de Mexico": {"lat": 19.3569, "lon": -99.6561},
            "Michoacan": {"lat": 19.5665, "lon": -101.7068},
            "Morelos": {"lat": 18.6813, "lon": -99.1013},
            "Nayarit": {"lat": 21.7514, "lon": -104.8455},
            "Nuevo Leon": {"lat": 25.5922, "lon": -99.9962},
            "Oaxaca": {"lat": 17.0732, "lon": -96.7266},
            "Puebla": {"lat": 19.0414, "lon": -98.2063},
            "Queretaro": {"lat": 20.5888, "lon": -100.3899},
            "Quintana Roo": {"lat": 19.1817, "lon": -88.4791},
            "San Luis Potosi": {"lat": 22.1565, "lon": -100.9855},
            "Sinaloa": {"lat": 24.8069, "lon": -107.3940},
            "Sonora": {"lat": 29.0729, "lon": -110.9559},
            "Tabasco": {"lat": 17.8409, "lon": -92.6189},
            "Tamaulipas": {"lat": 24.2669, "lon": -98.8363},
            "Tlaxcala": {"lat": 19.3139, "lon": -98.2404},
            "Veracruz": {"lat": 19.1738, "lon": -96.1342},
            "Yucatan": {"lat": 20.7099, "lon": -89.0943},
            "Zacatecas": {"lat": 22.7709, "lon": -102.5832}
        }

    def get_daily_data_batch(self, states_batch, start_date, end_date):
        """
        Obtiene datos diarios para un lote de estados
        """
        batch_data = []
        
        for state_name, coords in states_batch.items():
            params = {
                "latitude": coords["lat"],
                "longitude": coords["lon"],
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
            
            try:
                logger.info(f"Procesando datos diarios para {state_name}...")
                
                responses = self.openmeteo.weather_api(self.archive_url, params=params)
                response = responses[0]
                
                # Procesar datos diarios
                daily = response.Daily()
                daily_data = {
                    "date": pd.date_range(
                        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                        freq=pd.Timedelta(seconds=daily.Interval()),
                        inclusive="left"
                    )
                }
                
                # Variables meteorológicas
                variables = [
                    "temperature_2m_max", "temperature_2m_min", "precipitation_sum",
                    "wind_speed_10m_max", "shortwave_radiation_sum"
                ]
                
                for i, var in enumerate(variables):
                    try:
                        daily_data[var] = daily.Variables(i).ValuesAsNumpy()
                    except:
                        daily_data[var] = np.full(len(daily_data["date"]), np.nan)
                        logger.warning(f"Variable {var} no disponible para {state_name}")
                
                # Crear DataFrame
                df = pd.DataFrame(data=daily_data)
                df['state'] = state_name
                df['latitude'] = coords["lat"]
                df['longitude'] = coords["lon"]
                df['elevation'] = response.Elevation()
                df['data_type'] = 'daily'
                
                batch_data.append(df)
                
                logger.info(f"✅ {state_name} - {len(df)} registros extraídos")
                
            except Exception as e:
                logger.error(f"❌ Error procesando {state_name}: {e}")
                continue
                
            # Pausa para rate limiting
            time.sleep(0.5)
        
        return batch_data

    def extract_all_states_recent(self, days_back=30):
        """
        Extrae datos recientes para TODOS los 32 estados mexicanos
        """
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"🚀 Iniciando extracción para {len(self.mexican_states)} estados")
        logger.info(f"📅 Período: {start_date} a {end_date}")
        
        # Procesar en lotes de 8 estados para evitar sobrecarga
        state_names = list(self.mexican_states.keys())
        batch_size = 8
        all_data = []
        
        for i in range(0, len(state_names), batch_size):
            batch_states = {}
            batch_names = state_names[i:i+batch_size]
            
            for name in batch_names:
                batch_states[name] = self.mexican_states[name]
            
            logger.info(f"📦 Procesando lote {i//batch_size + 1}: {', '.join(batch_names)}")
            
            batch_data = self.get_daily_data_batch(
                batch_states,
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d")
            )
            
            all_data.extend(batch_data)
            
            # Pausa entre lotes
            if i + batch_size < len(state_names):
                logger.info("⏳ Pausa entre lotes...")
                time.sleep(2)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"🎉 Extracción completada: {len(combined_df)} registros totales")
            return combined_df
        else:
            logger.error("❌ No se pudieron obtener datos")
            return None

    def generate_summary_report(self, df):
        """
        Genera reporte resumen de los datos extraídos
        """
        if df is None or df.empty:
            print("No hay datos para el reporte")
            return
            
        print("\n" + "="*60)
        print("📊 REPORTE RESUMEN - DATOS METEOROLÓGICOS MÉXICO")
        print("="*60)
        
        # Información general
        print(f"📅 Período de datos: {df['date'].min().strftime('%Y-%m-%d')} a {df['date'].max().strftime('%Y-%m-%d')}")
        print(f"🏛️  Estados incluidos: {df['state'].nunique()}/32")
        print(f"📊 Total de registros: {len(df):,}")
        print(f"📈 Promedio registros por estado: {len(df)//df['state'].nunique():.0f}")
        
        # Estados procesados
        print(f"\n🗺️  Estados procesados:")
        states_list = sorted(df['state'].unique())
        for i in range(0, len(states_list), 4):
            line_states = states_list[i:i+4]
            print(f"   {', '.join(line_states)}")
        
        # Estadísticas meteorológicas
        print(f"\n🌡️  RANGOS DE TEMPERATURAS:")
        print(f"   Máxima más alta: {df['temperature_2m_max'].max():.1f}°C")
        print(f"   Mínima más baja: {df['temperature_2m_min'].min():.1f}°C")
        print(f"   Promedio máximas: {df['temperature_2m_max'].mean():.1f}°C")
        print(f"   Promedio mínimas: {df['temperature_2m_min'].mean():.1f}°C")
        
        print(f"\n☔ PRECIPITACIÓN:")
        print(f"   Máxima diaria: {df['precipitation_sum'].max():.1f} mm")
        print(f"   Promedio diario: {df['precipitation_sum'].mean():.2f} mm")
        print(f"   Días sin lluvia: {(df['precipitation_sum'] == 0).sum()}/{len(df)} ({(df['precipitation_sum'] == 0).mean()*100:.1f}%)")
        
        print(f"\n🌪️  VIENTO:")
        print(f"   Velocidad máxima: {df['wind_speed_10m_max'].max():.1f} km/h")
        print(f"   Promedio: {df['wind_speed_10m_max'].mean():.1f} km/h")
        
        print(f"\n☀️  RADIACIÓN SOLAR:")
        print(f"   Máxima: {df['shortwave_radiation_sum'].max():.1f} MJ/m²")
        print(f"   Promedio: {df['shortwave_radiation_sum'].mean():.1f} MJ/m²")
        
        # Estados con temperaturas extremas
        print(f"\n🔥 ESTADO MÁS CALUROSO:")
        hottest_day = df.loc[df['temperature_2m_max'].idxmax()]
        print(f"   {hottest_day['state']}: {hottest_day['temperature_2m_max']:.1f}°C el {hottest_day['date'].strftime('%Y-%m-%d')}")
        
        print(f"\n🥶 ESTADO MÁS FRÍO:")
        coldest_day = df.loc[df['temperature_2m_min'].idxmin()]
        print(f"   {coldest_day['state']}: {coldest_day['temperature_2m_min']:.1f}°C el {coldest_day['date'].strftime('%Y-%m-%d')}")
        
        print(f"\n💧 MAYOR PRECIPITACIÓN:")
        rainiest_day = df.loc[df['precipitation_sum'].idxmax()]
        print(f"   {rainiest_day['state']}: {rainiest_day['precipitation_sum']:.1f} mm el {rainiest_day['date'].strftime('%Y-%m-%d')}")
        
        print("="*60)

def main():
    """
    Función principal para extraer datos de todos los estados
    """
    extractor = FullStatesExtractor()
    
    print("🇲🇽 EXTRACTOR COMPLETO - 32 ESTADOS MEXICANOS")
    print("="*50)
    
    try:
        # Extraer datos para todos los estados (últimos 30 días)
        df = extractor.extract_all_states_recent(days_back=30)
        
        if df is not None:
            # Generar reporte resumen
            extractor.generate_summary_report(df)
            
            # Guardar datos
            filename = f"mexico_all_states_weather_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(filename, index=False)
            print(f"\n💾 Datos guardados en: {filename}")
            
            # Validación rápida
            print(f"\n✅ VALIDACIÓN:")
            print(f"   Estados únicos: {df['state'].nunique()}")
            print(f"   Registros totales: {len(df)}")
            print(f"   Columnas: {len(df.columns)}")
            print(f"   Valores nulos: {df.isnull().sum().sum()}")
            
            # Mostrar muestra de datos
            print(f"\n📋 MUESTRA DE DATOS:")
            print(df[['date', 'state', 'temperature_2m_max', 'temperature_2m_min', 'precipitation_sum']].head(10))
            
        else:
            print("❌ Error: No se pudieron extraer datos")
            
    except KeyboardInterrupt:
        print("\n⏹️  Extracción cancelada por el usuario")
    except Exception as e:
        print(f"❌ Error durante la extracción: {e}")
        logger.error(f"Error crítico: {e}")

if __name__ == "__main__":
    main()
