import boto3
import json
import logging
from datetime import datetime
from botocore.exceptions import ClientError
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LambdaFunctionURLs:
    def __init__(self, region='us-east-1'):
        self.region = region
        self.lambda_client = boto3.client('lambda', region_name=region)
        
        # Funciones Lambda existentes
        self.functions = [
            'openmeteo-weather-weather-api-dev',
            'openmeteo-weather-alerts-api-dev', 
            'openmeteo-weather-metrics-api-dev'
        ]
        
        self.function_urls = {}
    
    def create_function_url(self, function_name):
        """
        Crear URL pública para función Lambda
        """
        try:
            logger.info(f"🔗 Creando Function URL para: {function_name}")
            
            response = self.lambda_client.create_function_url_config(
                FunctionName=function_name,
                Config={
                    'AuthType': 'NONE',  # Sin autenticación
                    'Cors': {
                        'AllowCredentials': False,
                        'AllowHeaders': ['content-type', 'x-amz-date', 'authorization'],
                        'AllowMethods': ['*'],  # Cambio aquí: usar * en lugar de lista
                        'AllowOrigins': ['*'],
                        'ExposeHeaders': [],
                        'MaxAge': 300
                    }
                }
            )
            
            function_url = response['FunctionUrl']
            self.function_urls[function_name] = function_url
            
            logger.info(f"✅ Function URL creada: {function_url}")
            return function_url
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                logger.info(f"⚠️ Function URL ya existe para {function_name}")
                # Obtener URL existente
                try:
                    response = self.lambda_client.get_function_url_config(
                        FunctionName=function_name
                    )
                    function_url = response['FunctionUrl']
                    self.function_urls[function_name] = function_url
                    logger.info(f"📋 URL existente: {function_url}")
                    return function_url
                except:
                    return None
            else:
                logger.error(f"❌ Error creando Function URL para {function_name}: {e}")
                return None
    
    def get_existing_urls(self):
        """
        Obtener URLs existentes sin crear nuevas
        """
        logger.info("📋 OBTENIENDO FUNCTION URLS EXISTENTES")
        logger.info("="*50)
        
        for function_name in self.functions:
            try:
                response = self.lambda_client.get_function_url_config(
                    FunctionName=function_name
                )
                function_url = response['FunctionUrl']
                self.function_urls[function_name] = function_url
                logger.info(f"✅ {function_name}: {function_url}")
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    logger.info(f"❌ No existe Function URL para: {function_name}")
                else:
                    logger.error(f"❌ Error obteniendo URL para {function_name}: {e}")
        
        return self.function_urls
    
    def create_all_function_urls(self):
        """
        Crear Function URLs para todas las funciones Lambda
        """
        logger.info("🔗 CREANDO FUNCTION URLS (VERSIÓN CORREGIDA)")
        logger.info("="*60)
        
        # Primero verificar URLs existentes
        existing_urls = self.get_existing_urls()
        
        # Crear URLs faltantes
        for function_name in self.functions:
            if function_name not in existing_urls:
                url = self.create_function_url(function_name)
                if url:
                    self.function_urls[function_name] = url
        
        logger.info(f"✅ {len(self.function_urls)} Function URLs disponibles")
        return self.function_urls
    
    def test_function_url(self, function_name, function_url):
        """
        Probar Function URL con petición HTTP
        """
        try:
            logger.info(f"🧪 Probando Function URL: {function_name}")
            
            # Crear payload básico para Lambda
            test_payload = {
                'httpMethod': 'GET',
                'path': '/health',
                'queryStringParameters': {},
                'headers': {'Content-Type': 'application/json'},
                'body': None
            }
            
            response = requests.post(
                function_url,
                json=test_payload,
                timeout=30,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                logger.info(f"✅ Test exitoso para {function_name}")
                return True
            else:
                logger.warning(f"⚠️ Test parcial para {function_name} - Status: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error probando {function_name}: {e}")
            return False
    
    def test_all_urls(self):
        """
        Probar todas las Function URLs
        """
        if not self.function_urls:
            logger.warning("⚠️ No hay Function URLs para probar")
            return False
        
        logger.info("🧪 PROBANDO TODAS LAS FUNCTION URLS")
        logger.info("="*45)
        
        results = {}
        for function_name, function_url in self.function_urls.items():
            results[function_name] = self.test_function_url(function_name, function_url)
        
        successful_tests = sum(results.values())
        logger.info(f"📊 Pruebas exitosas: {successful_tests}/{len(results)}")
        
        return successful_tests > 0
    
    def generate_documentation(self):
        """
        Generar documentación completa de Function URLs
        """
        if not self.function_urls:
            return "No hay Function URLs disponibles"
        
        weather_url = self.function_urls.get('openmeteo-weather-weather-api-dev', 'N/A')
        alerts_url = self.function_urls.get('openmeteo-weather-alerts-api-dev', 'N/A')
        metrics_url = self.function_urls.get('openmeteo-weather-metrics-api-dev', 'N/A')
        
        documentation = f"""
🌐 OPENMETEO WEATHER API - LAMBDA FUNCTION URLS
{'='*60}

📍 ENDPOINTS DIRECTOS (sin API Gateway):

🌤️ WEATHER API:
   URL: {weather_url}
   
   Método: POST con JSON body:
   {{
     "httpMethod": "GET",
     "path": "/weather",
     "queryStringParameters": {{"state": "Guanajuato", "limit": "10"}}
   }}

⚠️ ALERTS API:
   URL: {alerts_url}
   
   Método: POST con JSON body:
   {{
     "httpMethod": "GET", 
     "path": "/alerts",
     "queryStringParameters": {{"state": "Sinaloa", "crop": "maiz"}}
   }}

📊 METRICS API:
   URL: {metrics_url}
   
   Método: POST con JSON body:
   {{
     "httpMethod": "GET",
     "path": "/metrics", 
     "queryStringParameters": {{"state": "Chiapas"}}
   }}

🔧 FORMATO DE REQUEST:
   Content-Type: application/json
   Method: POST
   Body: Evento simulado de API Gateway

📱 EJEMPLO JAVASCRIPT/FLUTTER:
```javascript
const response = await fetch('{weather_url}', {{
  method: 'POST',
  headers: {{ 'Content-Type': 'application/json' }},
  body: JSON.stringify({{
    httpMethod: 'GET',
    path: '/weather',
    queryStringParameters: {{ state: 'Guanajuato' }}
  }})
}});
const data = await response.json();
```

🚀 VENTAJAS:
   ✅ No requiere API Gateway (compatible con LabRole)
   ✅ CORS configurado automáticamente  
   ✅ URLs públicas directas
   ✅ Sin costos adicionales

💡 NOTA:
   Estas URLs funcionan directamente con las Lambda Functions
   sin necesidad de permisos adicionales de API Gateway.
        """
        
        return documentation
    
    def save_api_info(self):
        """
        Guardar información de API en archivo JSON
        """
        api_info = {
            "timestamp": datetime.now().isoformat(),
            "type": "Lambda Function URLs",
            "region": self.region,
            "function_urls": self.function_urls,
            "endpoints": {
                "weather": {
                    "url": self.function_urls.get('openmeteo-weather-weather-api-dev', ''),
                    "method": "POST",
                    "description": "Obtener datos meteorológicos por estado"
                },
                "alerts": {
                    "url": self.function_urls.get('openmeteo-weather-alerts-api-dev', ''),
                    "method": "POST", 
                    "description": "Obtener alertas agrícolas por estado y cultivo"
                },
                "metrics": {
                    "url": self.function_urls.get('openmeteo-weather-metrics-api-dev', ''),
                    "method": "POST",
                    "description": "Obtener métricas agregadas por estado"
                }
            }
        }
        
        with open('api_info.json', 'w', encoding='utf-8') as f:
            json.dump(api_info, f, indent=2, ensure_ascii=False)
        
        logger.info("💾 Información de API guardada en: api_info.json")
        return api_info

def main():
    """
    Función principal para configurar Function URLs
    """
    try:
        print("🔗 LAMBDA FUNCTION URLS SETUP - VERSIÓN CORREGIDA")
        print("="*65)
        print(f"🕐 Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*65)
        
        # Inicializar configurador
        setup = LambdaFunctionURLs()
        
        # Crear o obtener Function URLs
        function_urls = setup.create_all_function_urls()
        
        if function_urls:
            # Generar documentación
            docs = setup.generate_documentation()
            print(docs)
            
            # Guardar información de API
            setup.save_api_info()
            
            print(f"\n📋 RESUMEN:")
            print(f"   ✅ {len(function_urls)} Function URLs disponibles")
            print(f"   📄 Documentación generada")
            print(f"   💾 Info guardada en api_info.json")
            print(f"   📱 URLs listas para integrar con Flutter Web")
            print(f"   💡 Sin necesidad de permisos adicionales de API Gateway")
            
        else:
            print("❌ No se pudieron obtener Function URLs")
            print("💡 Verifica que las Lambda Functions existan")
            for func_name in setup.functions:
                print(f"   - {func_name}")
        
    except Exception as e:
        logger.error(f"❌ Error en setup Function URLs: {e}")
        print(f"\n💡 Si obtienes errores de permisos:")
        print(f"   - Tu sesión de AWS Academy puede haber expirado")
        print(f"   - Reinicia el laboratorio desde AWS Academy")
        print(f"   - Vuelve a configurar las credenciales AWS")

if __name__ == "__main__":
    main()
