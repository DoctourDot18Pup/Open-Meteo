import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import logging
from datetime import datetime, timedelta
import time
import numpy as np
import os

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HistoricalExtractor:
    def __init__(self):
        # Setup the Open-Meteo API client with cache and retry on error
        cache_session = requests_cache.CachedSession('.cache', expire_after=86400)  # 24 hour cache
        retry_session = retry(cache_session, retries=5, backoff_factor=0.3)
        self.openmeteo = openmeteo_requests.Client(session=retry_session)
        
        self.archive_url = "https://archive-api.open-meteo.com/v1/archive"
        
        # Estados mexicanos (nombres normalizados sin acentos)
        self.mexican_states = {
            "Aguascalientes": {"lat": 21.8853, "lon": -102.2916, "region": "Bajio", "display_name": "Aguascalientes"},
            "Baja California": {"lat": 32.6519, "lon": -115.4683, "region": "Norte_Arido", "display_name": "Baja California"},
            "Campeche": {"lat": 19.8414, "lon": -90.5328, "region": "Sureste_Tropical", "display_name": "Campeche"},
            "Chiapas": {"lat": 16.7569, "lon": -93.1292, "region": "Sur_Tropical", "display_name": "Chiapas"},
            "Chihuahua": {"lat": 28.6353, "lon": -106.0889, "region": "Norte_Desertico", "display_name": "Chihuahua"},
            "Ciudad de Mexico": {"lat": 19.4326, "lon": -99.1332, "region": "Centro_Templado", "display_name": "Ciudad de México"},
            "Guanajuato": {"lat": 21.0190, "lon": -101.2574, "region": "Bajio", "display_name": "Guanajuato"},
            "Guerrero": {"lat": 17.4392, "lon": -99.5451, "region": "Sur_Calido", "display_name": "Guerrero"},
            "Jalisco": {"lat": 20.6597, "lon": -103.3496, "region": "Occidente", "display_name": "Jalisco"},
            "Estado de Mexico": {"lat": 19.3569, "lon": -99.6561, "region": "Centro_Templado", "display_name": "Estado de México"},
            "Michoacan": {"lat": 19.5665, "lon": -101.7068, "region": "Occidente", "display_name": "Michoacán"},
            "Nuevo Leon": {"lat": 25.5922, "lon": -99.9962, "region": "Noreste", "display_name": "Nuevo León"},
            "Oaxaca": {"lat": 17.0732, "lon": -96.7266, "region": "Sur_Montanoso", "display_name": "Oaxaca"},
            "Puebla": {"lat": 19.0414, "lon": -98.2063, "region": "Centro", "display_name": "Puebla"},
            "Queretaro": {"lat": 20.5888, "lon": -100.3899, "region": "Bajio", "display_name": "Queretaro"},
            "Quintana Roo": {"lat": 19.1817, "lon": -88.4791, "region": "Caribe", "display_name": "Quintana Roo"},
            "Sinaloa": {"lat": 24.8069, "lon": -107.3940, "region": "Pacifico_Norte", "display_name": "Sinaloa"},
            "Sonora": {"lat": 29.0729, "lon": -110.9559, "region": "Noroeste_Arido", "display_name": "Sonora"},
            "Tabasco": {"lat": 17.8409, "lon": -92.6189, "region": "Golfo_Humedo", "display_name": "Tabasco"},
            "Tamaulipas": {"lat": 24.2669, "lon": -98.8363, "region": "Golfo_Norte", "display_name": "Tamaulipas"},
            "Veracruz": {"lat": 19.1738, "lon": -96.1342, "region": "Golfo_Centro", "display_name": "Veracruz"},
            "Yucatan": {"lat": 20.7099, "lon": -89.0943, "region": "Peninsula", "display_name": "Yucatán"},
            "Zacatecas": {"lat": 22.7709, "lon": -102.5832, "region": "Norte_Centro", "display_name": "Zacatecas"}
        }

    def get_historical_data_year(self, state_name, coords, year):
        """
        Obtiene datos históricos para un estado específico en un año completo
        """
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        
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
            logger.info(f"📅 {state_name} - Año {year}...")
            
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
            
            variables = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", 
                        "wind_speed_10m_max", "shortwave_radiation_sum"]
            
            for i, var in enumerate(variables):
                try:
                    daily_data[var] = daily.Variables(i).ValuesAsNumpy()
                except:
                    daily_data[var] = np.full(len(daily_data["date"]), np.nan)
            
            # Crear DataFrame
            df = pd.DataFrame(data=daily_data)
            df['state'] = state_name  # Nombre normalizado sin acentos
            df['state_display'] = coords["display_name"]  # Nombre original con acentos para mostrar
            df['year'] = year
            df['latitude'] = coords["lat"]
            df['longitude'] = coords["lon"]
            df['region'] = coords["region"]
            df['elevation'] = response.Elevation()
            
            # Agregar variables derivadas importantes para agricultura
            df['temp_range'] = df['temperature_2m_max'] - df['temperature_2m_min']
            df['month'] = df['date'].dt.month
            df['day_of_year'] = df['date'].dt.dayofyear
            df['season'] = df['month'].map({12:1, 1:1, 2:1,  # Invierno
                                         3:2, 4:2, 5:2,     # Primavera  
                                         6:3, 7:3, 8:3,     # Verano
                                         9:4, 10:4, 11:4})  # Otoño
            
            logger.info(f"✅ {state_name} {year}: {len(df)} registros extraídos")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error {state_name} {year}: {e}")
            return None

    def extract_historical_batch(self, states_subset=None, years_range=(2020, 2024), save_yearly=True):
        """
        Extrae datos históricos para un conjunto de estados y años
        """
        if states_subset is None:
            states_to_process = self.mexican_states
        else:
            states_to_process = {k: v for k, v in self.mexican_states.items() 
                               if k in states_subset}
        
        start_year, end_year = years_range
        years = list(range(start_year, end_year + 1))
        
        logger.info(f"🚀 EXTRACCIÓN HISTÓRICA INICIADA")
        logger.info(f"📍 Estados: {len(states_to_process)} ({', '.join(states_to_process.keys())})")
        logger.info(f"📅 Años: {years}")
        logger.info(f"📊 Total estimado: {len(states_to_process) * len(years) * 365} registros")
        
        all_data = []
        total_states = len(states_to_process)
        
        for idx, (state_name, coords) in enumerate(states_to_process.items(), 1):
            logger.info(f"\n🏛️  PROCESANDO {state_name} ({idx}/{total_states}) - {coords['region']}")
            
            state_data = []
            
            for year in years:
                df_year = self.get_historical_data_year(state_name, coords, year)
                
                if df_year is not None:
                    state_data.append(df_year)
                    all_data.append(df_year)
                
                # Pausa para evitar rate limiting
                time.sleep(1)
            
            # Guardar datos por estado si se especifica
            if save_yearly and state_data:
                state_combined = pd.concat(state_data, ignore_index=True)
                # Nombre de archivo normalizado (sin acentos ni espacios)
                safe_state_name = state_name.replace(' ', '_').replace('ñ', 'n').replace('é', 'e').replace('á', 'a').replace('ó', 'o').replace('í', 'i').replace('ú', 'u').lower()
                filename = f"historico_{safe_state_name}_{start_year}_{end_year}.csv"
                state_combined.to_csv(filename, index=False)
                logger.info(f"💾 Guardado: {filename}")
            
            # Pausa más larga entre estados
            if idx < total_states:
                logger.info("⏳ Pausa entre estados...")
                time.sleep(2)
        
        # Combinar todos los datos
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"\n🎉 EXTRACCIÓN COMPLETADA: {len(combined_df)} registros totales")
            return combined_df
        else:
            logger.error("❌ No se pudieron extraer datos históricos")
            return None

    def generate_agricultural_analysis(self, df):
        """
        Genera análisis específico para agricultura
        """
        if df is None or df.empty:
            return
            
        print("\n" + "="*70)
        print("🌾 ANÁLISIS AGRÍCOLA - DATOS HISTÓRICOS MÉXICO")
        print("="*70)
        
        # Resumen general
        years = sorted(df['year'].unique())
        states = sorted(df['state'].unique())
        
        print(f"📊 Período analizado: {min(years)} - {max(years)} ({len(years)} años)")
        print(f"🗺️  Estados incluidos: {len(states)}")
        print(f"📈 Total registros: {len(df):,}")
        
        # Análisis por regiones climáticas
        print(f"\n🌍 ANÁLISIS POR REGIÓN:")
        region_stats = df.groupby('region').agg({
            'temperature_2m_max': 'mean',
            'temperature_2m_min': 'mean', 
            'precipitation_sum': 'sum',
            'shortwave_radiation_sum': 'mean'
        }).round(2)
        
        for region, stats in region_stats.iterrows():
            print(f"   {region}:")
            print(f"     Temp promedio: {stats['temperature_2m_min']:.1f}°C - {stats['temperature_2m_max']:.1f}°C")
            print(f"     Precipitación anual: {stats['precipitation_sum']:.0f} mm")
            print(f"     Radiación solar: {stats['shortwave_radiation_sum']:.1f} MJ/m²")
        
        # Análisis de riesgos agrícolas
        print(f"\n⚠️  ANÁLISIS DE RIESGOS CLIMÁTICOS:")
        
        # Heladas (temperatura < 0°C)
        frost_days = (df['temperature_2m_min'] < 0).sum()
        frost_risk_states = df[df['temperature_2m_min'] < 0]['state'].value_counts().head()
        print(f"🧊 Días con heladas detectados: {frost_days}")
        if len(frost_risk_states) > 0:
            print("   Estados con mayor riesgo de heladas:")
            for state, days in frost_risk_states.items():
                print(f"     - {state}: {days} días")
        
        # Sequías (precipitación < 1mm por más de 30 días consecutivos)
        print(f"\n🏜️  Análisis de sequías por estado:")
        for state in df['state'].unique()[:5]:  # Top 5 para no saturar
            state_data = df[df['state'] == state].sort_values('date')
            dry_days = (state_data['precipitation_sum'] < 1).sum()
            total_days = len(state_data)
            dry_percentage = (dry_days / total_days) * 100
            print(f"   {state}: {dry_percentage:.1f}% días secos")
        
        # Temperaturas extremas
        extreme_hot = df[df['temperature_2m_max'] > 40]
        extreme_cold = df[df['temperature_2m_min'] < 5]
        
        print(f"\n🔥 Días con calor extremo (>40°C): {len(extreme_hot)}")
        print(f"❄️  Días con frío extremo (<5°C): {len(extreme_cold)}")
        
        # Análisis estacional
        print(f"\n📅 PATRONES ESTACIONALES:")
        seasonal_stats = df.groupby('season').agg({
            'temperature_2m_max': 'mean',
            'precipitation_sum': 'mean',
            'shortwave_radiation_sum': 'mean'
        }).round(2)
        
        seasons = {1: "Invierno", 2: "Primavera", 3: "Verano", 4: "Otoño"}
        for season_num, stats in seasonal_stats.iterrows():
            season_name = seasons[season_num]
            print(f"   {season_name}:")
            print(f"     Temp máx promedio: {stats['temperature_2m_max']:.1f}°C")
            print(f"     Precipitación promedio: {stats['precipitation_sum']:.2f} mm/día")
            print(f"     Radiación solar: {stats['shortwave_radiation_sum']:.1f} MJ/m²")
        
        print("="*70)

def main():
    """
    Función principal - Extracción histórica
    """
    extractor = HistoricalExtractor()
    
    print("🏛️  EXTRACTOR DE DATOS HISTÓRICOS - MÉXICO")
    print("📅 Período: 2020-2024 (5 años)")
    print("="*50)
    
    # Opción 1: Estados clave para prueba inicial (más rápido)
    key_states = [
        "Guanajuato",      # Bajío agrícola
        "Sinaloa",         # Agricultura intensiva
        "Chiapas",         # Sur tropical
        "Sonora",          # Norte árido
        "Veracruz",        # Golfo húmedo
        "Queretaro"        # Tu estado! (sin acento)
    ]
    
    print(f"🎯 EXTRACCIÓN INICIAL - Estados Clave:")
    print(f"   {', '.join(key_states)}")
    
    try:
        # Extraer datos históricos
        df = extractor.extract_historical_batch(
            states_subset=key_states,
            years_range=(2020, 2024),
            save_yearly=True
        )
        
        if df is not None:
            # Guardar archivo consolidado
            filename = f"mexico_historical_weather_2020_2024_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(filename, index=False)
            
            # Análisis agrícola
            extractor.generate_agricultural_analysis(df)
            
            print(f"\n💾 ARCHIVOS GENERADOS:")
            print(f"   📄 Consolidado: {filename}")
            print(f"   📄 Por estado: historico_[estado]_2020_2024.csv")
            print(f"\n✅ Extracción histórica completada exitosamente!")
            
            # Mostrar muestra final
            print(f"\n📋 MUESTRA DE DATOS HISTÓRICOS:")
            sample_cols = ['date', 'state', 'year', 'temperature_2m_max', 'precipitation_sum', 'season']
            print(df[sample_cols].head(10))
            
        else:
            print("❌ Error en la extracción histórica")
            
    except KeyboardInterrupt:
        print("\n⏹️  Extracción cancelada por el usuario")
    except Exception as e:
        logger.error(f"Error crítico: {e}")
        print(f"❌ Error durante la extracción: {e}")

if __name__ == "__main__":
    main()
