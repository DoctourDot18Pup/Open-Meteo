import boto3
import time
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class ApiGatewayV2:
    def __init__(self):
        self.region = 'us-east-1'
        self.api_client = boto3.client('apigateway', region_name=self.region)
        self.lambda_client = boto3.client('lambda', region_name=self.region)
        self.sts_client = boto3.client('sts', region_name=self.region)
        
        # Nombre de la nueva API
        self.api_name = 'AgroV2-Public-API'
        self.stage_name = 'dev'
        
        # Mapeo a las funciones AgroV2 que acabas de crear
        self.lambda_map = {
            'weather': 'AgroV2-Weather-Service-dev',
            'alerts': 'AgroV2-Alerts-Service-dev',
            'metrics': 'AgroV2-Metrics-Service-dev'
        }
        
        # Obtener Account ID para permisos
        self.account_id = self.sts_client.get_caller_identity()["Account"]

    def create_api(self):
        print(f"🚀 Creando API Gateway: {self.api_name}...")
        try:
            api = self.api_client.create_rest_api(
                name=self.api_name,
                description='API Publica para Proyecto AgroV2 (Spark + ML)',
                endpointConfiguration={'types': ['REGIONAL']}
            )
            self.api_id = api['id']
            print(f"   ✅ API ID: {self.api_id}")
            
            # Obtener ID de la raíz (/)
            resources = self.api_client.get_resources(restApiId=self.api_id)
            self.root_id = resources['items'][0]['id']
            
        except ClientError as e:
            logger.error(f"❌ Error creando API: {e}")
            raise

    def create_resource_and_method(self, path_part, function_name):
        print(f"\nConfigurando endpoint: /{path_part} ...")
        
        # 1. Crear Recurso (URL path)
        resp = self.api_client.create_resource(
            restApiId=self.api_id,
            parentId=self.root_id,
            pathPart=path_part
        )
        resource_id = resp['id']
        
        # 2. Crear Método GET
        self.api_client.put_method(
            restApiId=self.api_id,
            resourceId=resource_id,
            httpMethod='GET',
            authorizationType='NONE'
        )
        
        # 3. Integración con Lambda
        lambda_arn = f"arn:aws:lambda:{self.region}:{self.account_id}:function:{function_name}"
        uri = f"arn:aws:apigateway:{self.region}:lambda:path/2015-03-31/functions/{lambda_arn}/invocations"
        
        self.api_client.put_integration(
            restApiId=self.api_id,
            resourceId=resource_id,
            httpMethod='GET',
            type='AWS_PROXY',
            integrationHttpMethod='POST', # API Gateway siempre usa POST para hablar con Lambda
            uri=uri
        )
        
        # 4. Permisos (Resource Policy en Lambda)
        try:
            self.lambda_client.add_permission(
                FunctionName=function_name,
                StatementId=f'apigateway-invoke-{path_part}-{int(time.time())}',
                Action='lambda:InvokeFunction',
                Principal='apigateway.amazonaws.com',
                SourceArn=f"arn:aws:execute-api:{self.region}:{self.account_id}:{self.api_id}/*/GET/{path_part}"
            )
            print(f"   ✅ Permisos Lambda otorgados.")
        except ClientError:
            print(f"   ⚠️ Permisos ya existían.")

        # 5. Habilitar CORS (Para Dashboard Web)
        self.enable_cors(resource_id)

    def enable_cors(self, resource_id):
        # Crear metodo OPTIONS
        self.api_client.put_method(
            restApiId=self.api_id,
            resourceId=resource_id,
            httpMethod='OPTIONS',
            authorizationType='NONE'
        )
        # Respuesta Mock
        self.api_client.put_integration(
            restApiId=self.api_id,
            resourceId=resource_id,
            httpMethod='OPTIONS',
            type='MOCK',
            requestTemplates={'application/json': '{"statusCode": 200}'}
        )
        # Headers de respuesta
        response_params = {
            'method.response.header.Access-Control-Allow-Headers': "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
            'method.response.header.Access-Control-Allow-Methods': "'GET,OPTIONS'",
            'method.response.header.Access-Control-Allow-Origin': "'*'"
        }
        self.api_client.put_method_response(
            restApiId=self.api_id,
            resourceId=resource_id,
            httpMethod='OPTIONS',
            statusCode='200',
            responseParameters={k: False for k in response_params}
        )
        self.api_client.put_integration_response(
            restApiId=self.api_id,
            resourceId=resource_id,
            httpMethod='OPTIONS',
            statusCode='200',
            responseParameters=response_params
        )
        print("   ✅ CORS habilitado.")

    def deploy(self):
        print("\n🚀 Desplegando API al mundo...")
        self.api_client.create_deployment(
            restApiId=self.api_id,
            stageName=self.stage_name
        )
        url = f"https://{self.api_id}.execute-api.{self.region}.amazonaws.com/{self.stage_name}"
        print("\n" + "="*50)
        print("🎉 API GATEWAY LISTO")
        print("="*50)
        print(f"URL Base: {url}")
        print("\nEndpoints Disponibles:")
        print(f"   🌤️  Clima:   {url}/weather?state=Guanajuato")
        print(f"   🤖  Con ML:  {url}/weather?state=Guanajuato&cluster=2")
        print(f"   ⚠️  Alertas: {url}/alerts?state=Guanajuato")
        print(f"   📈  Metricas: {url}/metrics?state=Guanajuato")

    def run(self):
        self.create_api()
        # Crear los 3 endpoints conectados a sus respectivas lambdas
        self.create_resource_and_method('weather', self.lambda_map['weather'])
        self.create_resource_and_method('alerts', self.lambda_map['alerts'])
        self.create_resource_and_method('metrics', self.lambda_map['metrics'])
        self.deploy()

if __name__ == "__main__":
    setup = ApiGatewayV2()
    setup.run()
