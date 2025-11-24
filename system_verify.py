import boto3
import requests
import sys
import time
import json

# Colores para la consola
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

class AgroV2Verifier:
    def __init__(self):
        self.region = 'us-east-1'
        self.apigateway = boto3.client('apigateway', region_name=self.region)
        self.api_name = 'AgroV2-Public-API'
        self.stage = 'dev'
        self.base_url = None

    def find_api_url(self):
        print(f"{Colors.HEADER}🔍 Buscando API Gateway '{self.api_name}'...{Colors.ENDC}")
        try:
            apis = self.apigateway.get_rest_apis()
            for item in apis['items']:
                if item['name'] == self.api_name:
                    api_id = item['id']
                    self.base_url = f"https://{api_id}.execute-api.{self.region}.amazonaws.com/{self.stage}"
                    print(f"   ✅ API Encontrada: {Colors.OKBLUE}{self.base_url}{Colors.ENDC}")
                    return True
            
            print(f"   {Colors.FAIL}❌ No se encontró la API. ¿Ejecutaste 'api_gateway_v2.py'?{Colors.ENDC}")
            return False
        except Exception as e:
            print(f"   {Colors.FAIL}❌ Error de conexión AWS: {e}{Colors.ENDC}")
            return False

    def test_endpoint(self, name, endpoint, params, check_field=None):
        url = f"{self.base_url}/{endpoint}"
        print(f"\n🧪 Probando {Colors.BOLD}{name}{Colors.ENDC}...")
        print(f"   🔗 URL: {url}")
        print(f"   📝 Params: {params}")

        start_time = time.time()
        try:
            response = requests.get(url, params=params, timeout=10)
            latency = (time.time() - start_time) * 1000
            
            if response.status_code == 200:
                data = response.json()
                count = 0
                
                # Manejo de diferentes estructuras de respuesta
                if 'data' in data: count = len(data['data'])     # Weather
                elif 'alerts' in data: count = len(data['alerts']) # Alerts
                elif 'metrics' in data: count = len(data['metrics']) # Metrics
                
                print(f"   {Colors.OKGREEN}✅ STATUS 200 OK{Colors.ENDC} ({int(latency)}ms)")
                print(f"   📦 Datos recibidos: {count} registros")
                
                # Validación de contenido específico
                if check_field and count > 0:
                    sample = data.get('data', [])[0] if 'data' in data else {}
                    if check_field in sample:
                         print(f"   🧠 Validación ML: Campo '{check_field}' encontrado (Valor: {sample[check_field]})")
                    else:
                         print(f"   {Colors.WARNING}⚠️ Advertencia: Campo '{check_field}' no encontrado en la respuesta.{Colors.ENDC}")
                
                return True
            else:
                print(f"   {Colors.FAIL}❌ ERROR {response.status_code}{Colors.ENDC}")
                print(f"   Mensaje: {response.text}")
                return False

        except Exception as e:
            print(f"   {Colors.FAIL}❌ Excepción: {e}{Colors.ENDC}")
            return False

    def run_full_audit(self):
        print(f"{Colors.BOLD}🚀 INICIANDO AUDITORÍA DE SISTEMA AGRO V2{Colors.ENDC}")
        print("="*50)
        
        if not self.find_api_url():
            return

        # TEST 1: Clima Básico
        self.test_endpoint(
            "Weather Service (Básico)", 
            "weather", 
            {"state": "Guanajuato"}
        )

        # TEST 2: Clima con Filtro IA (Machine Learning)
        # Buscamos el cluster '0' (o el que exista en tus datos)
        self.test_endpoint(
            "Weather Service (Filtro AI)", 
            "weather", 
            {"state": "Guanajuato", "cluster": "0"},
            check_field="cluster_clima"
        )

        # TEST 3: Alertas
        self.test_endpoint(
            "Alerts Service", 
            "alerts", 
            {"state": "Guanajuato"}
        )

        # TEST 4: Métricas
        self.test_endpoint(
            "Metrics Service", 
            "metrics", 
            {"state": "Guanajuato"}
        )

        print("\n" + "="*50)
        print(f"{Colors.OKGREEN}🎉 AUDITORÍA COMPLETADA{Colors.ENDC}")

if __name__ == "__main__":
    verifier = AgroV2Verifier()
    verifier.run_full_audit()

