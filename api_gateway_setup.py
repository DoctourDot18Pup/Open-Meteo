import boto3
import json
import logging
from datetime import datetime
from botocore.exceptions import ClientError
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ApiGatewaySetup:
    def __init__(self, region='us-east-1'):
        self.region = region
        self.api_client = boto3.client('apigateway', region_name=region)
        self.lambda_client = boto3.client('lambda', region_name=region)
        
        # --- CORRECCIÓN 1: Cliente STS para obtener Account ID ---
        self.sts_client = boto3.client('sts', region_name=region)
        try:
            self.account_id = self.sts_client.get_caller_identity()["Account"]
            logger.info(f"🆔 AWS Account ID detectado: {self.account_id}")
        except Exception as e:
            logger.error(f"❌ Error obteniendo Account ID: {e}")
            raise
        # ----------------------------------------------------------
        
        # Configuración
        self.api_name = 'openmeteo-weather-api'
        self.api_description = 'OpenMeteo Weather API for Agricultural Alerts'
        self.stage_name = 'dev'
        
        # Lambda functions ARNs (Coinciden con tu setup de lambdas)
        self.lambda_functions = {
            'weather': 'openmeteo-weather-weather-api-dev',
            'alerts': 'openmeteo-weather-alerts-api-dev',
            'metrics': 'openmeteo-weather-metrics-api-dev'
        }
        
        self.api_id = None
        self.root_resource_id = None
    
    def create_rest_api(self):
        """
        Crear API REST Gateway
        """
        try:
            logger.info(f"🌐 Creando API REST: {self.api_name}")
            
            response = self.api_client.create_rest_api(
                name=self.api_name,
                description=self.api_description,
                endpointConfiguration={
                    'types': ['REGIONAL']
                },
                tags={
                    'Project': 'OpenMeteoWeather',
                    'Environment': 'dev'
                }
            )
            
            self.api_id = response['id']
            logger.info(f"✅ API creada: {self.api_id}")
            
            # Obtener root resource
            resources = self.api_client.get_resources(restApiId=self.api_id)
            self.root_resource_id = resources['items'][0]['id']
            
            return response
            
        except ClientError as e:
            logger.error(f"❌ Error creando API: {e}")
            raise
    
    def create_resource(self, parent_id, path_part):
        """
        Crear recurso en API Gateway
        """
        try:
            logger.info(f"📁 Creando recurso: /{path_part}")
            
            response = self.api_client.create_resource(
                restApiId=self.api_id,
                parentId=parent_id,
                pathPart=path_part
            )
            
            logger.info(f"✅ Recurso creado: {response['id']}")
            return response
            
        except ClientError as e:
            logger.error(f"❌ Error creando recurso {path_part}: {e}")
            raise
    
    def create_method(self, resource_id, http_method, lambda_function_name):
        """
        Crear método HTTP para recurso
        """
        try:
            logger.info(f"🔗 Creando método {http_method} para {lambda_function_name}")
            
            # Crear método
            self.api_client.put_method(
                restApiId=self.api_id,
                resourceId=resource_id,
                httpMethod=http_method,
                authorizationType='NONE',
                requestParameters={}
            )
            
            # Obtener ARN de la función Lambda
            lambda_response = self.lambda_client.get_function(
                FunctionName=lambda_function_name
            )
            
            lambda_arn = lambda_response['Configuration']['FunctionArn']
            
            # Construir URI de integración
            lambda_uri = f"arn:aws:apigateway:{self.region}:lambda:path/2015-03-31/functions/{lambda_arn}/invocations"
            
            # Crear integración con Lambda
            self.api_client.put_integration(
                restApiId=self.api_id,
                resourceId=resource_id,
                httpMethod=http_method,
                type='AWS_PROXY',
                integrationHttpMethod='POST',
                uri=lambda_uri
            )
            
            # Dar permisos a API Gateway para invocar Lambda
            self.add_lambda_permission(lambda_function_name, http_method, resource_id)
            
            logger.info(f"✅ Método {http_method} configurado")
            
        except ClientError as e:
            logger.error(f"❌ Error creando método {http_method}: {e}")
            raise
    
    def add_lambda_permission(self, function_name, http_method, resource_id):
        """
        Dar permiso a API Gateway para invocar Lambda
        """
        try:
            statement_id = f"api-gateway-invoke-{function_name}-{http_method}-{resource_id}"
            
            # --- CORRECCIÓN 2: Uso de Account ID real en SourceArn ---
            # Se reemplaza el '*' por self.account_id
            source_arn = f"arn:aws:execute-api:{self.region}:{self.account_id}:{self.api_id}/*/{http_method}/*"
            # ---------------------------------------------------------
            
            self.lambda_client.add_permission(
                FunctionName=function_name,
                StatementId=statement_id,
                Action='lambda:InvokeFunction',
                Principal='apigateway.amazonaws.com',
                SourceArn=source_arn
            )
            
            logger.info(f"✅ Permisos Lambda configurados para {function_name} (SourceArn válido)")
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                logger.info(f"⚠️ Permisos ya existen para {function_name}")
            else:
                logger.warning(f"⚠️ Error configurando permisos para {function_name}: {e}")
                # No lanzamos raise aquí para permitir que el proceso continúe, 
                # pero el log advertirá si falló.
    
    def enable_cors(self, resource_id):
        """
        Habilitar CORS para un recurso
        """
        try:
            logger.info(f"🌐 Habilitando CORS para recurso {resource_id}")
            
            # Crear método OPTIONS
            self.api_client.put_method(
                restApiId=self.api_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                authorizationType='NONE'
            )
            
            # Integración MOCK para OPTIONS
            self.api_client.put_integration(
                restApiId=self.api_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                type='MOCK',
                requestTemplates={
                    'application/json': '{"statusCode": 200}'
                }
            )
            
            # Respuesta OPTIONS
            self.api_client.put_method_response(
                restApiId=self.api_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                statusCode='200',
                responseParameters={
                    'method.response.header.Access-Control-Allow-Headers': False,
                    'method.response.header.Access-Control-Allow-Methods': False,
                    'method.response.header.Access-Control-Allow-Origin': False
                }
            )
            
            # Integración response
            self.api_client.put_integration_response(
                restApiId=self.api_id,
                resourceId=resource_id,
                httpMethod='OPTIONS',
                statusCode='200',
                responseParameters={
                    'method.response.header.Access-Control-Allow-Headers': "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
                    'method.response.header.Access-Control-Allow-Methods': "'GET,OPTIONS,POST,PUT'",
                    'method.response.header.Access-Control-Allow-Origin': "'*'"
                }
            )
            
            logger.info(f"✅ CORS habilitado")
            
        except ClientError as e:
            logger.warning(f"⚠️ Error habilitando CORS: {e}")
    
    def deploy_api(self):
        """
        Desplegar API a stage
        """
        try:
            logger.info(f"🚀 Desplegando API a stage: {self.stage_name}")
            
            response = self.api_client.create_deployment(
                restApiId=self.api_id,
                stageName=self.stage_name,
                description='Initial deployment of OpenMeteo Weather API'
            )
            
            # Construir URL de la API
            api_url = f"https://{self.api_id}.execute-api.{self.region}.amazonaws.com/{self.stage_name}"
            
            logger.info(f"✅ API desplegada exitosamente")
            logger.info(f"🌐 URL base: {api_url}")
            
            return api_url
            
        except ClientError as e:
            logger.error(f"❌ Error desplegando API: {e}")
            raise
    
    def create_complete_api(self):
        """
        Crear API completa con todos los endpoints
        """
        logger.info("🌐 CREANDO API GATEWAY COMPLETO")
        logger.info("="*50)
        
        try:
            # 1. Crear API REST
            self.create_rest_api()
            
            # 2. Crear recursos y métodos
            endpoints = [
                {
                    'path': 'weather',
                    'lambda_function': self.lambda_functions['weather'],
                    'description': 'Weather data endpoints'
                },
                {
                    'path': 'alerts',
                    'lambda_function': self.lambda_functions['alerts'],
                    'description': 'Agricultural alerts endpoints'
                },
                {
                    'path': 'metrics',
                    'lambda_function': self.lambda_functions['metrics'],
                    'description': 'Metrics and analytics endpoints'
                }
            ]
            
            created_resources = {}
            
            for endpoint in endpoints:
                # Crear recurso
                resource = self.create_resource(
                    self.root_resource_id, 
                    endpoint['path']
                )
                
                resource_id = resource['id']
                created_resources[endpoint['path']] = resource_id
                
                # Crear método GET
                self.create_method(
                    resource_id, 
                    'GET', 
                    endpoint['lambda_function']
                )
                
                # Habilitar CORS
                self.enable_cors(resource_id)
                
                # Pequeña pausa para evitar rate limits
                time.sleep(1)
            
            # 3. Desplegar API
            api_url = self.deploy_api()
            
            return {
                'api_id': self.api_id,
                'api_url': api_url,
                'endpoints': created_resources
            }
            
        except Exception as e:
            logger.error(f"❌ Error creando API completo: {e}")
            raise
    
    def test_endpoints(self, api_url):
        """
        Probar endpoints de la API
        """
        logger.info("🧪 PROBANDO ENDPOINTS DE LA API")
        logger.info("="*40)
        
        import requests
        
        test_endpoints = [
            {
                'name': 'Weather - States list',
                'url': f"{api_url}/weather",
                'params': {}
            },
            {
                'name': 'Alerts - General',
                'url': f"{api_url}/alerts",
                'params': {'limit': 5}
            },
            {
                'name': 'Metrics - Summary',
                'url': f"{api_url}/metrics",
                'params': {'limit': 3}
            }
        ]
        
        results = {}
        
        for test in test_endpoints:
            try:
                logger.info(f"🔗 Probando: {test['name']}")
                
                response = requests.get(
                    test['url'], 
                    params=test['params'],
                    timeout=10
                )
                
                if response.status_code == 200:
                    logger.info(f"✅ {test['name']}: Status {response.status_code}")
                    results[test['name']] = 'SUCCESS'
                else:
                    logger.warning(f"⚠️ {test['name']}: Status {response.status_code}")
                    # Intentar leer el mensaje de error si es un 500
                    try:
                        logger.warning(f"   Respuesta: {response.text}")
                    except:
                        pass
                    results[test['name']] = f'HTTP {response.status_code}'
                
            except Exception as e:
                logger.error(f"❌ {test['name']}: Error - {e}")
                results[test['name']] = 'ERROR'
        
        return results
    
    def get_api_documentation(self, api_url):
        """
        Generar documentación de la API
        """
        return f"""
🌐 OPENMETEO WEATHER API - DOCUMENTACIÓN
{'='*50}

📍 URL Base: {api_url}

🌤️ WEATHER ENDPOINTS:
   GET /weather?state={{state_name}}     - Datos meteorológicos por estado
   GET /weather?limit={{number}}         - Limitar resultados
   
   Ejemplo: {api_url}/weather?state=Guanajuato&limit=10

⚠️ ALERTS ENDPOINTS:
   GET /alerts?state={{state_name}}      - Alertas por estado
   GET /alerts?crop={{crop_code}}        - Alertas por cultivo
   GET /alerts?severity={{level}}        - Alertas por severidad
   
   Ejemplo: {api_url}/alerts?state=Sinaloa&crop=maiz

📊 METRICS ENDPOINTS:
   GET /metrics?state={{state_name}}     - Métricas por estado
   GET /metrics?year={{year}}            - Métricas por año
   
   Ejemplo: {api_url}/metrics?state=Chiapas

🔧 PARÁMETROS DISPONIBLES:
   - state: Nombre del estado mexicano
   - crop: maiz, frijol, chile, trigo
   - severity: CRÍTICO, ALTO, MEDIO
   - limit: Número de resultados (default: 30)
   - year: Año específico
   - month: Mes específico
        """

def main():
    """
    Setup completo de API Gateway
    """
    print("🌐 API GATEWAY SETUP - OPENMETEO WEATHER")
    print("="*50)
    print(f"🕐 Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)
    
    # Configuración
    region = 'us-east-1'
    
    try:
        # Inicializar setup
        setup = ApiGatewaySetup(region=region)
        
        # Crear API completo
        print("\n🌐 CREANDO API GATEWAY")
        api_info = setup.create_complete_api()
        
        # Esperar a que la API esté disponible
        print("\n⏳ Esperando disponibilidad de la API (15s)...")
        time.sleep(15) # Aumenté un poco el tiempo para asegurar propagación
        
        # Probar endpoints
        print("\n🧪 PROBANDO ENDPOINTS")
        test_results = setup.test_endpoints(api_info['api_url'])
        
        # Documentación
        documentation = setup.get_api_documentation(api_info['api_url'])
        
        print("\n" + "="*60)
        print("🎉 API GATEWAY COMPLETADO EXITOSAMENTE")
        print("="*60)
        
        print(f"\n📊 RESUMEN:")
        print(f"   API ID: {api_info['api_id']}")
        print(f"   URL: {api_info['api_url']}")
        print(f"   Endpoints: {len(api_info['endpoints'])}")
        print(f"   Región: {region}")
        
        print(f"\n🔗 ENDPOINTS CREADOS:")
        for path, resource_id in api_info['endpoints'].items():
            print(f"   {path.upper()}: {api_info['api_url']}/{path}")
        
        print(f"\n🧪 RESULTADOS DE PRUEBAS:")
        for test_name, result in test_results.items():
            status = "✅" if result == 'SUCCESS' else "⚠️"
            print(f"   {status} {test_name}: {result}")
        
        print(f"\n📋 DOCUMENTACIÓN:")
        print(documentation)
        
        # Guardar información de la API
        with open('api_info.json', 'w') as f:
            json.dump({
                'api_url': api_info['api_url'],
                'api_id': api_info['api_id'],
                'endpoints': api_info['endpoints'],
                'region': region,
                'account_id': setup.account_id, # Guardamos el account_id también
                'created_at': datetime.now().isoformat()
            }, f, indent=2)
        
        print(f"\n💾 Información guardada en: api_info.json")
        
    except Exception as e:
        logger.error(f"❌ Error en setup API Gateway: {e}")
        print(f"\n💡 Verifica que las Lambda functions estén creadas y se llamen correctamente.")

if __name__ == "__main__":
    main()
