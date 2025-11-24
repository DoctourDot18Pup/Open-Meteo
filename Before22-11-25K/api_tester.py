import requests
import json
import os
import sys
from datetime import datetime

# Colores para consola
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def load_api_url():
    """Intenta cargar la URL desde el archivo generado anteriormente o pide input"""
    try:
        with open('api_info.json', 'r') as f:
            data = json.load(f)
            return data['api_url']
    except FileNotFoundError:
        print(f"{Colors.WARNING}⚠️ No se encontró api_info.json{Colors.ENDC}")
        url = input("👉 Por favor, introduce la URL base de tu API Gateway: ").strip()
        return url.rstrip('/')

def print_response(response, expected_code):
    """Imprime la respuesta formateada"""
    is_success = response.status_code == expected_code
    
    # Icono de estado
    if is_success:
        status_icon = "✅"
        color = Colors.OKGREEN
    else:
        status_icon = "❌"
        color = Colors.FAIL
        
    print(f"   {status_icon} Status: {color}{response.status_code}{Colors.ENDC} (Esperado: {expected_code})")
    print(f"   ⏱️  Tiempo: {response.elapsed.total_seconds()}s")
    
    try:
        data = response.json()
        # Imprimir JSON bonito pero truncado si es muy largo
        formatted_json = json.dumps(data, indent=2)
        if len(formatted_json) > 500:
            print(f"   📄 Respuesta (Truncada): {Colors.OKCYAN}{formatted_json[:500]}...{Colors.ENDC}\n")
        else:
            print(f"   📄 Respuesta: {Colors.OKCYAN}{formatted_json}{Colors.ENDC}\n")
            
        # Advertencia si la lista está vacía (común en tablas nuevas)
        if response.status_code == 200:
            if 'data' in data and not data['data']:
                print(f"      {Colors.WARNING}⚠️  Nota: La lista 'data' está vacía. (¿Llenaste las tablas DynamoDB?){Colors.ENDC}\n")
            elif 'alerts' in data and not data['alerts']:
                print(f"      {Colors.WARNING}⚠️  Nota: La lista 'alerts' está vacía.{Colors.ENDC}\n")
                
    except Exception:
        print(f"   📄 Respuesta (Texto): {response.text}\n")

def run_tests():
    print(f"{Colors.HEADER}{Colors.BOLD}🌐 MONITOR DE DISPONIBILIDAD DE API - OPENMETEO{Colors.ENDC}")
    print("="*60)
    
    base_url = load_api_url()
    print(f"🎯 Objetivo: {Colors.BOLD}{base_url}{Colors.ENDC}\n")

    # Definición de escenarios de prueba
    tests = [
        {
            "section": "🌤️ WEATHER API",
            "scenarios": [
                {
                    "name": "Obtener clima por Estado (Happy Path)",
                    "endpoint": "/weather",
                    "params": {"state": "Guanajuato", "limit": 5},
                    "expected": 200,
                    "desc": "Debe retornar datos para Guanajuato"
                },
                {
                    "name": "Validación de Parámetros Faltantes",
                    "endpoint": "/weather",
                    "params": {}, # Sin params intencionalmente
                    "expected": 400,
                    "desc": "Debe fallar porque 'state' es requerido en tu Lambda"
                }
            ]
        },
        {
            "section": "⚠️ ALERTS API",
            "scenarios": [
                {
                    "name": "Listado General de Alertas",
                    "endpoint": "/alerts",
                    "params": {"limit": 3},
                    "expected": 200,
                    "desc": "Scan general de la tabla de alertas"
                },
                {
                    "name": "Filtrado por Severidad Crítica",
                    "endpoint": "/alerts",
                    "params": {"severity": "CRÍTICO"},
                    "expected": 200,
                    "desc": "Debe filtrar solo alertas críticas"
                },
                {
                    "name": "Filtrado Combinado (Estado + Cultivo)",
                    "endpoint": "/alerts",
                    "params": {"state": "Sinaloa", "crop": "maiz"},
                    "expected": 200,
                    "desc": "Filtro compuesto"
                }
            ]
        },
        {
            "section": "📊 METRICS API",
            "scenarios": [
                {
                    "name": "Métricas Generales",
                    "endpoint": "/metrics",
                    "params": {"limit": 10},
                    "expected": 200,
                    "desc": "Obtención de métricas históricas"
                },
                {
                    "name": "Filtro por Año Específico",
                    "endpoint": "/metrics",
                    "params": {"year": "2024", "state": "Jalisco"},
                    "expected": 200,
                    "desc": "Query específico a la tabla de métricas"
                }
            ]
        }
    ]

    total_pass = 0
    total_tests = 0

    for module in tests:
        print(f"{Colors.HEADER}___ {module['section']} ___{Colors.ENDC}")
        
        for scenario in module['scenarios']:
            total_tests += 1
            print(f"{Colors.BOLD}🔹 Prueba {total_tests}: {scenario['name']}{Colors.ENDC}")
            print(f"   ℹ️  Desc: {scenario['desc']}")
            print(f"   🔗 GET {scenario['endpoint']} | Params: {scenario['params']}")
            
            try:
                response = requests.get(
                    f"{base_url}{scenario['endpoint']}", 
                    params=scenario['params'],
                    timeout=10
                )
                print_response(response, scenario['expected'])
                
                if response.status_code == scenario['expected']:
                    total_pass += 1
                    
            except requests.exceptions.ConnectionError:
                print(f"   {Colors.FAIL}❌ ERROR DE CONEXIÓN: No se pudo conectar al host{Colors.ENDC}\n")
            except Exception as e:
                print(f"   {Colors.FAIL}❌ ERROR INESPERADO: {e}{Colors.ENDC}\n")

    # Resumen final
    print("="*60)
    success_rate = (total_pass / total_tests) * 100
    color_rate = Colors.OKGREEN if success_rate == 100 else (Colors.WARNING if success_rate > 50 else Colors.FAIL)
    
    print(f"📊 RESUMEN DE EJECUCIÓN")
    print(f"   Total Pruebas: {total_tests}")
    print(f"   Exitosas:      {color_rate}{total_pass}{Colors.ENDC}")
    print(f"   Fallidas:      {Colors.FAIL if total_tests - total_pass > 0 else Colors.OKGREEN}{total_tests - total_pass}{Colors.ENDC}")
    print(f"   Tasa de Éxito: {color_rate}{success_rate:.1f}%{Colors.ENDC}")
    print("="*60)

if __name__ == "__main__":
    # Verificar dependencias
    try:
        import requests
    except ImportError:
        print("❌ Error: Necesitas instalar 'requests'. Ejecuta: pip install requests")
        sys.exit(1)
        
    run_tests()
