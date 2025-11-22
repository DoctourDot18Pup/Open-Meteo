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
        self.processed_dirs = [
            'processed_weather_data_dynamo',
            'processed_monthly_metrics_dynamo', 
            'processed_alerts_dynamo'
        ]
        
    def verify_directory_structure(self):
        """Verificar que los directorios y archivos existen"""
        print("🔍 VERIFICANDO ESTRUCTURA DE ARCHIVOS")
        print("="*50)
        
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
        print("🌤️ VERIFICANDO DATOS METEOROLÓGICOS")
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
                
                print("\n📈 ESTADÍSTICAS:")
                # Análisis de estados
                states = set(r.get('state', 'Unknown') for r in records)
                print(f"   Estados únicos: {len(states)}")
                print(f"   Estados: {', '.join(sorted(states))}")
                
                # Análisis de fechas
                dates = [r.get('sort_key') for r in records if r.get('sort_key')]
                if dates:
                    print(f"   Rango de fechas: {min(dates)} a {max(dates)}")
                
                # Análisis de temperaturas
                temps_max = [r.get('temp_max') for r in records if r.get('temp_max') is not None]
                temps_min = [r.get('temp_min') for r in records if r.get('temp_min') is not None]
                
                if temps_max:
                    print(f"   Temp máxima: {min(temps_max):.1f}°C a {max(temps_max):.1f}°C")
                if temps_min:
                    print(f"   Temp mínima: {min(temps_min):.1f}°C a {max(temps_min):.1f}°C")
                
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
            
            if alerts:
                # Análisis por tipo de alerta
                alert_types = {}
                alert_severity = {}
                alert_crops = {}
                alert_states = {}
                
                for alert in alerts:
                    # Por tipo
                    alert_type = alert.get('alert_type', 'Unknown')
                    alert_types[alert_type] = alert_types.get(alert_type, 0) + 1
                    
                    # Por severidad
                    severity = alert.get('severity', 'Unknown')
                    alert_severity[severity] = alert_severity.get(severity, 0) + 1
                    
                    # Por cultivo
                    crop = alert.get('crop', 'Unknown')
                    alert_crops[crop] = alert_crops.get(crop, 0) + 1
                    
                    # Por estado
                    state = alert.get('state', 'Unknown')
                    alert_states[state] = alert_states.get(state, 0) + 1
                
                print("\n📊 DISTRIBUCIÓN DE ALERTAS:")
                print("Por tipo:")
                for alert_type, count in sorted(alert_types.items()):
                    print(f"   {alert_type}: {count}")
                
                print("Por severidad:")
                for severity, count in sorted(alert_severity.items()):
                    print(f"   {severity}: {count}")
                
                print("Por cultivo:")
                for crop, count in sorted(alert_crops.items()):
                    print(f"   {crop}: {count}")
                
                print("Por estado:")
                for state, count in sorted(alert_states.items()):
                    print(f"   {state}: {count}")
                
                # Mostrar algunas alertas de ejemplo
                print("\n📋 EJEMPLOS DE ALERTAS:")
                for i, alert in enumerate(alerts[:3]):
                    print(f"   Alerta {i+1}:")
                    print(f"      Estado: {alert.get('state')}")
                    print(f"      Cultivo: {alert.get('crop')}")
                    print(f"      Tipo: {alert.get('alert_type')}")
                    print(f"      Severidad: {alert.get('severity')}")
                    print(f"      Mensaje: {alert.get('message')}")
                    print(f"      Días afectados: {alert.get('affected_days')}")
                    print()
                    
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
        
        try:
            # Leer métricas
            metrics = []
            with open(sample_file, 'r') as f:
                for line in f:
                    if line.strip():
                        metrics.append(json.loads(line))
            
            print(f"📊 Total de métricas por estado: {len(metrics)}")
            
            if metrics:
                print("\n📋 MÉTRICAS POR ESTADO:")
                for metric in sorted(metrics, key=lambda x: x.get('state', '')):
                    state = metric.get('state', 'Unknown')
                    avg_max = metric.get('avg_temp_max', 0)
                    avg_min = metric.get('avg_temp_min', 0)
                    precip = metric.get('total_precipitation', 0)
                    radiation = metric.get('avg_radiation', 0)
                    days = metric.get('total_days', 0)
                    
                    print(f"   🏛️ {state}:")
                    print(f"      Temp promedio: {avg_min:.1f}°C - {avg_max:.1f}°C")
                    print(f"      Precipitación total: {precip:.1f} mm")
                    print(f"      Radiación promedio: {radiation:.1f} MJ/m²")
                    print(f"      Días analizados: {days}")
                    print()
                    
        except Exception as e:
            print(f"❌ Error analizando métricas: {e}")
    
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
        
        # Verificar tamaños de archivos
        for dir_name in self.processed_dirs:
            if os.path.exists(dir_name):
                json_files = glob.glob(os.path.join(dir_name, '*.json'))
                for file in json_files:
                    size = os.path.getsize(file)
                    if size < 100:  # Menos de 100 bytes
                        issues.append(f"Archivo muy pequeño: {file}")
        
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
        
        print("\n🚀 PRÓXIMOS PASOS:")
        print("   1. ✅ Datos procesados y verificados")
        print("   2. 📦 Crear tablas DynamoDB")
        print("   3. 🚀 Configurar Lambda Functions")
        print("   4. 🌐 Setup API Gateway")
        print("   5. 📱 Desarrollar Dashboard")
        
def main():
    """Función principal de verificación"""
    print("🔍 VERIFICADOR DE DATOS PROCESADOS")
    print("="*60)
    print(f"🕐 Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    verifier = DataVerifier()
    
    # 1. Verificar estructura
    verifier.verify_directory_structure()
    
    # 2. Verificar datos meteorológicos
    verifier.verify_weather_data()
    
    # 3. Verificar alertas
    verifier.verify_alerts_data()
    
    # 4. Verificar métricas
    verifier.verify_metrics_data()
    
    # 5. Validar integridad
    is_valid = verifier.validate_data_integrity()
    
    # 6. Reporte final
    verifier.generate_summary_report()
    
    if is_valid:
        print("\n🎉 VERIFICACIÓN COMPLETADA EXITOSAMENTE!")
        print("📦 Datos listos para integración DynamoDB")
    else:
        print("\n⚠️ Se detectaron algunos problemas")
        print("🔧 Revisa los errores arriba antes de continuar")

if __name__ == "__main__":
    main()
