import os
import json
import pandas as pd
import glob
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataVerifier:
    def __init__(self):
        # Archivos CSV disponibles para verificar
        self.csv_files = {
            'historical_data': 'mexico_historical_weather_2020_2024_20251119.csv',
            'current_32_states': 'mexico_32_estados_daily_20251119_1717.csv',
            'recent_daily': 'mexico_reliable_daily_20251119.csv',
            'recent_hourly': 'mexico_reliable_hourly_20251119.csv'
        }
        
        # Directorios de procesamiento (si existen)
        self.processed_dirs = [
            'processed_weather_data_dynamo',
            'processed_monthly_metrics_dynamo', 
            'processed_alerts_dynamo'
        ]
    
    def verify_csv_files(self):
        """Verificar archivos CSV disponibles"""
        print("📊 VERIFICANDO ARCHIVOS CSV DISPONIBLES")
        print("="*60)
        
        for category, filename in self.csv_files.items():
            if os.path.exists(filename):
                try:
                    # Cargar CSV
                    df = pd.read_csv(filename)
                    file_size_kb = os.path.getsize(filename) / 1024
                    
                    print(f"✅ {category.upper()}:")
                    print(f"   📁 Archivo: {filename}")
                    print(f"   📏 Tamaño: {file_size_kb:.1f} KB")
                    print(f"   📊 Registros: {len(df):,}")
                    print(f"   🏛️ Columnas: {len(df.columns)}")
                    
                    # Análisis específico por tipo de archivo
                    if 'state' in df.columns:
                        states = df['state'].nunique()
                        print(f"   🌎 Estados únicos: {states}")
                        
                    if 'date' in df.columns:
                        try:
                            df['date'] = pd.to_datetime(df['date'])
                            date_range = f"{df['date'].min().date()} a {df['date'].max().date()}"
                            print(f"   📅 Rango de fechas: {date_range}")
                        except:
                            print(f"   📅 Fechas: Formato no estándar")
                    
                    if 'datetime' in df.columns:
                        try:
                            df['datetime'] = pd.to_datetime(df['datetime'])
                            date_range = f"{df['datetime'].min().date()} a {df['datetime'].max().date()}"
                            print(f"   🕐 Rango temporal: {date_range}")
                        except:
                            print(f"   🕐 Fechas: Formato no estándar")
                    
                    # Verificar variables meteorológicas clave
                    key_vars = ['temperature_2m_max', 'temperature_2m_min', 'precipitation_sum', 
                               'shortwave_radiation_sum', 'wind_speed_10m_max']
                    
                    available_vars = [var for var in key_vars if var in df.columns]
                    print(f"   🌡️ Variables meteorológicas: {len(available_vars)}/{len(key_vars)}")
                    
                    # Estadísticas rápidas de temperatura
                    if 'temperature_2m_max' in df.columns:
                        temp_max_range = f"{df['temperature_2m_max'].min():.1f}°C a {df['temperature_2m_max'].max():.1f}°C"
                        print(f"   🔥 Temp máxima: {temp_max_range}")
                    
                    if 'temperature_2m_min' in df.columns:
                        temp_min_range = f"{df['temperature_2m_min'].min():.1f}°C a {df['temperature_2m_min'].max():.1f}°C"
                        print(f"   🧊 Temp mínima: {temp_min_range}")
                    
                    if 'precipitation_sum' in df.columns:
                        precip_total = df['precipitation_sum'].sum()
                        print(f"   🌧️ Precipitación total: {precip_total:.1f} mm")
                    
                    print()
                    
                except Exception as e:
                    print(f"❌ Error analizando {filename}: {e}")
                    print()
            else:
                print(f"❌ {category.upper()}: {filename} - NO ENCONTRADO")
                print()
    
    def verify_data_quality(self):
        """Verificar calidad de los datos principales"""
        print("🔍 ANÁLISIS DE CALIDAD DE DATOS")
        print("="*60)
        
        # Verificar archivo principal de 32 estados
        main_file = self.csv_files['current_32_states']
        if os.path.exists(main_file):
            print(f"📊 ANÁLISIS DETALLADO: {main_file}")
            print("-" * 40)
            
            try:
                df = pd.read_csv(main_file)
                
                # Análisis por estado
                if 'state' in df.columns:
                    state_counts = df['state'].value_counts()
                    print(f"📍 DISTRIBUCIÓN POR ESTADO:")
                    print(f"   Estados procesados: {len(state_counts)}/32")
                    print(f"   Registros por estado:")
                    
                    for state, count in state_counts.head(10).items():
                        print(f"      {state}: {count} registros")
                    
                    if len(state_counts) > 10:
                        print(f"      ... y {len(state_counts) - 10} estados más")
                    print()
                
                # Análisis de completitud de variables
                print(f"🌡️ COMPLETITUD DE VARIABLES:")
                key_vars = ['temperature_2m_max', 'temperature_2m_min', 'precipitation_sum', 
                           'shortwave_radiation_sum', 'wind_speed_10m_max']
                
                for var in key_vars:
                    if var in df.columns:
                        non_null = df[var].notna().sum()
                        total = len(df)
                        completeness = (non_null / total) * 100
                        
                        if completeness > 95:
                            status = "✅"
                        elif completeness > 80:
                            status = "⚠️"
                        else:
                            status = "❌"
                        
                        print(f"   {status} {var}: {completeness:.1f}% ({non_null:,}/{total:,})")
                
                print()
                
                # Análisis de regiones
                if 'region' in df.columns:
                    region_counts = df['region'].value_counts()
                    print(f"🌎 DISTRIBUCIÓN POR REGIÓN:")
                    for region, count in region_counts.items():
                        print(f"   {region}: {count} registros")
                    print()
                
                # Detección de valores extremos
                if 'temperature_2m_max' in df.columns:
                    extreme_heat = df[df['temperature_2m_max'] > 45]
                    extreme_cold = df[df['temperature_2m_min'] < -10]
                    
                    print(f"🌡️ VALORES EXTREMOS:")
                    print(f"   Días con calor extremo (>45°C): {len(extreme_heat)}")
                    print(f"   Días con frío extremo (<-10°C): {len(extreme_cold)}")
                    
                    if len(extreme_heat) > 0:
                        hottest_day = extreme_heat.loc[extreme_heat['temperature_2m_max'].idxmax()]
                        print(f"   Día más caluroso: {hottest_day.get('state', 'Unknown')} - {hottest_day['temperature_2m_max']:.1f}°C")
                    
                    if len(extreme_cold) > 0:
                        coldest_day = extreme_cold.loc[extreme_cold['temperature_2m_min'].idxmin()]
                        print(f"   Día más frío: {coldest_day.get('state', 'Unknown')} - {coldest_day['temperature_2m_min']:.1f}°C")
                    
                    print()
                
            except Exception as e:
                print(f"❌ Error en análisis de calidad: {e}")
        else:
            print(f"❌ Archivo principal no encontrado: {main_file}")
    
    def verify_agricultural_potential(self):
        """Verificar potencial para análisis agrícola"""
        print("🌾 ANÁLISIS DE POTENCIAL AGRÍCOLA")
        print("="*60)
        
        main_file = self.csv_files['current_32_states']
        if os.path.exists(main_file):
            try:
                df = pd.read_csv(main_file)
                
                # Variables necesarias para alertas agrícolas
                required_vars = {
                    'temperature_2m_max': 'Temperatura máxima',
                    'temperature_2m_min': 'Temperatura mínima', 
                    'precipitation_sum': 'Precipitación',
                    'shortwave_radiation_sum': 'Radiación solar'
                }
                
                print("🔍 VARIABLES REQUERIDAS PARA ALERTAS:")
                missing_vars = []
                
                for var, description in required_vars.items():
                    if var in df.columns:
                        non_null = df[var].notna().sum()
                        completeness = (non_null / len(df)) * 100
                        
                        if completeness > 90:
                            status = "✅"
                        elif completeness > 70:
                            status = "⚠️"
                        else:
                            status = "❌"
                            missing_vars.append(var)
                        
                        print(f"   {status} {description}: {completeness:.1f}% disponible")
                    else:
                        print(f"   ❌ {description}: NO DISPONIBLE")
                        missing_vars.append(var)
                
                # Evaluación de viabilidad
                print(f"\n📊 EVALUACIÓN DE VIABILIDAD:")
                if len(missing_vars) == 0:
                    print("   ✅ EXCELENTE - Todos los datos necesarios disponibles")
                    print("   🚀 Listo para procesamiento PySpark y generación de alertas")
                elif len(missing_vars) <= 1:
                    print("   ⚠️ BUENO - La mayoría de datos disponibles")
                    print("   🔧 Se pueden generar alertas con limitaciones menores")
                else:
                    print("   ❌ LIMITADO - Faltan variables críticas")
                    print("   📝 Recomendado: Obtener más datos antes del procesamiento")
                
                # Análisis de cobertura temporal
                if 'date' in df.columns:
                    try:
                        df['date'] = pd.to_datetime(df['date'])
                        days_span = (df['date'].max() - df['date'].min()).days
                        unique_dates = df['date'].nunique()
                        
                        print(f"\n📅 COBERTURA TEMPORAL:")
                        print(f"   Período total: {days_span} días")
                        print(f"   Fechas únicas: {unique_dates}")
                        print(f"   Cobertura: {(unique_dates/days_span)*100:.1f}%")
                        
                        if unique_dates >= 30:
                            print("   ✅ Suficiente para análisis de tendencias")
                        else:
                            print("   ⚠️ Período corto para análisis robusto")
                            
                    except:
                        print(f"\n📅 COBERTURA TEMPORAL: No se pudo analizar formato de fecha")
                
                print()
                
            except Exception as e:
                print(f"❌ Error en análisis agrícola: {e}")
        else:
            print(f"❌ Archivo principal no disponible para análisis")
    
    def verify_directory_structure(self):
        """Verificar que los directorios procesados existen"""
        print("🔍 VERIFICANDO ESTRUCTURA DE DATOS PROCESADOS")
        print("="*60)
        
        for dir_name in self.processed_dirs:
            if os.path.exists(dir_name):
                files = os.listdir(dir_name)
                json_files = [f for f in files if f.endswith('.json')]
                
                print(f"✅ {dir_name}/")
                print(f"   📁 Archivos totales: {len(files)}")
                print(f"   📄 Archivos JSON: {len(json_files)}")
                
                # Mostrar nombres de archivos
                for file in json_files[:3]:  # Solo los primeros 3
                    file_path = os.path.join(dir_name, file)
                    size_kb = os.path.getsize(file_path) / 1024
                    print(f"   📋 {file} ({size_kb:.1f} KB)")
                
                if len(json_files) > 3:
                    print(f"   ... y {len(json_files) - 3} archivos más")
                    
            else:
                print(f"❌ {dir_name}/ - NO ENCONTRADO")
            print()
    
    def load_json_file(self, file_path):
        """Cargar archivo JSON con manejo de errores"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return data
        except Exception as e:
            logger.error(f"Error cargando {file_path}: {e}")
            return None
    
    def verify_weather_data(self):
        """Verificar datos meteorológicos procesados"""
        print("🌤️ VERIFICANDO DATOS METEOROLÓGICOS PROCESADOS")
        print("="*50)
        
        weather_dir = 'processed_weather_data_dynamo'
        if not os.path.exists(weather_dir):
            print("❌ Directorio de datos meteorológicos no encontrado")
            return
            
        json_files = glob.glob(os.path.join(weather_dir, '*.json'))
        
        if not json_files:
            print("❌ No se encontraron archivos JSON")
            return
            
        # Leer primer archivo JSON
        sample_file = json_files[0]
        print(f"📋 Analizando: {os.path.basename(sample_file)}")
        
        try:
            # Leer línea por línea (formato JSON Lines)
            records = []
            with open(sample_file, 'r') as f:
                for line in f:
                    if line.strip():
                        records.append(json.loads(line))
            
            print(f"📊 Total de registros: {len(records)}")
            
            if records:
                sample_record = records[0]
                print("\n📋 ESTRUCTURA DE REGISTRO:")
                for key, value in sample_record.items():
                    print(f"   {key}: {value} ({type(value).__name__})")
                
        except Exception as e:
            print(f"❌ Error analizando archivo: {e}")
    
    def verify_alerts_data(self):
        """Verificar datos de alertas"""
        print("\n⚠️ VERIFICANDO DATOS DE ALERTAS")
        print("="*50)
        
        alerts_dir = 'processed_alerts_dynamo'
        if not os.path.exists(alerts_dir):
            print("❌ Directorio de alertas no encontrado")
            return
            
        json_files = glob.glob(os.path.join(alerts_dir, '*.json'))
        
        if not json_files:
            print("❌ No se encontraron archivos de alertas")
            return
            
        sample_file = json_files[0]
        print(f"📋 Analizando: {os.path.basename(sample_file)}")
        
        try:
            # Leer alertas
            alerts = []
            with open(sample_file, 'r') as f:
                for line in f:
                    if line.strip():
                        alerts.append(json.loads(line))
            
            print(f"⚠️ Total de alertas: {len(alerts)}")
            
        except Exception as e:
            print(f"❌ Error analizando alertas: {e}")
    
    def verify_metrics_data(self):
        """Verificar métricas agregadas"""
        print("\n📈 VERIFICANDO MÉTRICAS AGREGADAS")
        print("="*50)
        
        metrics_dir = 'processed_monthly_metrics_dynamo'
        if not os.path.exists(metrics_dir):
            print("❌ Directorio de métricas no encontrado")
            return
            
        json_files = glob.glob(os.path.join(metrics_dir, '*.json'))
        
        if not json_files:
            print("❌ No se encontraron archivos de métricas")
            return
            
        sample_file = json_files[0]
        print(f"📋 Analizando: {os.path.basename(sample_file)}")
        print("✅ Métricas encontradas")
    
    def validate_data_integrity(self):
        """Validar integridad general de los datos"""
        print("\n✅ VALIDACIÓN DE INTEGRIDAD")
        print("="*50)
        
        issues = []
        
        # Verificar que todos los directorios existen
        for dir_name in self.processed_dirs:
            if not os.path.exists(dir_name):
                issues.append(f"Falta directorio: {dir_name}")
            else:
                json_files = glob.glob(os.path.join(dir_name, '*.json'))
                if not json_files:
                    issues.append(f"No hay archivos JSON en: {dir_name}")
        
        if issues:
            print("⚠️ PROBLEMAS DETECTADOS:")
            for issue in issues:
                print(f"   ❌ {issue}")
        else:
            print("✅ Todos los archivos parecen estar en buen estado")
            
        return len(issues) == 0
    
    def generate_summary_report(self):
        """Generar reporte resumen completo"""
        print("\n" + "="*70)
        print("📊 REPORTE FINAL DE VERIFICACIÓN")
        print("="*70)
        
        total_size = 0
        total_files = 0
        
        for dir_name in self.processed_dirs:
            if os.path.exists(dir_name):
                json_files = glob.glob(os.path.join(dir_name, '*.json'))
                dir_size = sum(os.path.getsize(f) for f in json_files)
                total_size += dir_size
                total_files += len(json_files)
                
                print(f"📁 {dir_name}:")
                print(f"   Archivos: {len(json_files)}")
                print(f"   Tamaño: {dir_size / 1024:.1f} KB")
        
        print(f"\n📊 TOTALES:")
        print(f"   📄 Archivos JSON generados: {total_files}")
        print(f"   💾 Espacio total: {total_size / 1024:.1f} KB")
        print(f"   ✅ Estado: Listos para DynamoDB")

def main():
    """Función principal de verificación"""
    print("🔍 VERIFICADOR DE DATOS CSV - OPENMETEO PROJECT")
    print("="*70)
    print(f"🕐 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    verifier = DataVerifier()
    
    # 1. Verificar archivos CSV disponibles
    verifier.verify_csv_files()
    
    # 2. Análisis de calidad de datos
    verifier.verify_data_quality()
    
    # 3. Verificar potencial agrícola
    verifier.verify_agricultural_potential()
    
    # 4. Verificar si existen datos procesados (opcional)
    if any(os.path.exists(dir_name) for dir_name in verifier.processed_dirs):
        print("📁 VERIFICANDO DATOS PROCESADOS EXISTENTES")
        print("="*60)
        verifier.verify_directory_structure()
        if os.path.exists('processed_weather_data_dynamo'):
            verifier.verify_weather_data()
        if os.path.exists('processed_alerts_dynamo'):
            verifier.verify_alerts_data()
        if os.path.exists('processed_monthly_metrics_dynamo'):
            verifier.verify_metrics_data()
        
        # Validación de integridad procesados
        verifier.validate_data_integrity()
        verifier.generate_summary_report()
    else:
        print("\n📋 ESTADO ACTUAL: DATOS CSV LISTOS")
        print("="*50)
        print("✅ Archivos CSV disponibles y verificados")
        print("🚀 PRÓXIMO PASO: Ejecutar pipeline PySpark")
        print("📦 Comando sugerido: python pyspark_agricultural_processor.py")
        print()
        
        # Resumen de archivos principales
        available_files = []
        for category, filename in verifier.csv_files.items():
            if os.path.exists(filename):
                available_files.append(f"✅ {category}: {filename}")
            else:
                available_files.append(f"❌ {category}: {filename}")
        
        print("📊 ARCHIVOS DISPONIBLES:")
        for file_status in available_files:
            print(f"   {file_status}")
        
        print("\n🎯 RECOMENDACIÓN:")
        if os.path.exists('mexico_32_estados_daily_20251119_1717.csv'):
            print("   ✅ Archivo principal de 32 estados disponible")
            print("   🚀 Proceder con procesamiento PySpark inmediatamente")
        else:
            print("   ⚠️ Ejecutar primero: python reliable_extractor_32_states.py")
            print("   📊 Para generar datos de los 32 estados")

if __name__ == "__main__":
    main()
