import json
import boto3
import zipfile
import os
import logging
from datetime import datetime
import tempfile
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LambdaFunctionsAcademySetup:
    def __init__(self, region='us-east-1'):
        self.region = region
        self.lambda_client = boto3.client('lambda', region_name=region)
        self.sts_client = boto3.client('sts', region_name=region)
        
        # Configuración para AWS Academy
        self.function_prefix = 'openmeteo-weather'
        self.environment = 'dev'
        
        # Obtener ARN del LabRole automáticamente
        self.lab_role_arn = None
        
    def get_lab_role_arn(self):
        """
        Obtener ARN del LabRole de AWS Academy
        """
        try:
            # Obtener información de la identidad actual
            identity = self.sts_client.get_caller_identity()
            account_id = identity['Account']
            
            # Construir ARN del LabRole
            lab_role_arn = f"arn:aws:iam::{account_id}:role/LabRole"
            
            logger.info(f"🔐 Usando LabRole: {lab_role_arn}")
            return lab_role_arn
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo LabRole: {e}")
            raise
    
    def create_weather_api_function(self):
        """
        Crear función Lambda para API de datos meteorológicos
        """
        function_code = '''
import json
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicializar DynamoDB
dynamodb = boto3.resource('dynamodb')
weather_table = dynamodb.Table('openmeteo-weather-weather-data-dev')

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    """
    API Handler para datos meteorológicos
    """
    try:
        # Extraer parámetros
        http_method = event.get('httpMethod', 'GET')
        path = event.get('path', '')
        query_params = event.get('queryStringParameters') or {}
        
        logger.info(f"Method: {http_method}, Path: {path}")
        
        if http_method == 'GET':
            if '/weather' in path:
                return get_weather_data(query_params)
            elif '/states' in path:
                return get_available_states()
            else:
                return create_response(400, {'error': 'Invalid path'})
        else:
            return create_response(405, {'error': 'Method not allowed'})
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})

def get_weather_data(params):
    """
    Obtener datos meteorológicos por estado
    """
    state = params.get('state')
    limit = int(params.get('limit', 30))
    
    if not state:
        return create_response(400, {'error': 'State parameter required'})
    
    try:
        # Consulta a DynamoDB
        response = weather_table.query(
            KeyConditionExpression=Key('state').eq(state),
            ScanIndexForward=False,  # Más reciente primero
            Limit=limit
        )
        
        items = response['Items']
        
        return create_response(200, {
            'state': state,
            'count': len(items),
            'data': items
        })
        
    except Exception as e:
        logger.error(f"Error querying weather data: {e}")
        return create_response(500, {'error': 'Failed to retrieve weather data'})

def get_available_states():
    """
    Obtener lista de estados disponibles
    """
    try:
        # Scan para obtener estados únicos
        response = weather_table.scan(
            ProjectionExpression='#state',
            ExpressionAttributeNames={'#state': 'state'}
        )
        
        states = list(set(item['state'] for item in response['Items']))
        states.sort()
        
        return create_response(200, {
            'states': states,
            'count': len(states)
        })
        
    except Exception as e:
        logger.error(f"Error getting states: {e}")
        return create_response(500, {'error': 'Failed to retrieve states'})

def create_response(status_code, body):
    """
    Crear respuesta HTTP estándar
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }
'''
        
        return self.create_lambda_function('weather-api', function_code, 'weather_api.lambda_handler')
    
    def create_alerts_api_function(self):
        """
        Crear función Lambda para API de alertas agrícolas
        """
        function_code = '''
import json
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicializar DynamoDB
dynamodb = boto3.resource('dynamodb')
alerts_table = dynamodb.Table('openmeteo-weather-agricultural-alerts-dev')

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    """
    API Handler para alertas agrícolas
    """
    try:
        http_method = event.get('httpMethod', 'GET')
        path = event.get('path', '')
        query_params = event.get('queryStringParameters') or {}
        
        logger.info(f"Method: {http_method}, Path: {path}")
        
        if http_method == 'GET':
            if '/alerts' in path:
                return get_alerts(query_params)
            elif '/critical' in path:
                return get_critical_alerts(query_params)
            elif '/crops' in path:
                return get_available_crops()
            else:
                return create_response(400, {'error': 'Invalid path'})
        else:
            return create_response(405, {'error': 'Method not allowed'})
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})

def get_alerts(params):
    """
    Obtener alertas por estado y filtros
    """
    state = params.get('state')
    crop = params.get('crop')
    severity = params.get('severity')
    limit = int(params.get('limit', 50))
    
    try:
        if state:
            # Consulta por estado
            response = alerts_table.query(
                KeyConditionExpression=Key('state').eq(state),
                ScanIndexForward=False,
                Limit=limit
            )
        else:
            # Scan general
            response = alerts_table.scan(Limit=limit)
        
        items = response['Items']
        
        # Filtrar por crop y severity si se especifica
        if crop:
            items = [item for item in items if item.get('crop') == crop]
        
        if severity:
            items = [item for item in items if item.get('severity') == severity]
        
        return create_response(200, {
            'alerts': items,
            'count': len(items),
            'filters': {
                'state': state,
                'crop': crop,
                'severity': severity
            }
        })
        
    except Exception as e:
        logger.error(f"Error querying alerts: {e}")
        return create_response(500, {'error': 'Failed to retrieve alerts'})

def get_critical_alerts(params):
    """
    Obtener solo alertas críticas
    """
    try:
        # Buscar alertas críticas
        response = alerts_table.scan(
            FilterExpression=Attr('severity').eq('CRÍTICO'),
            Limit=50
        )
        
        items = response['Items']
        
        return create_response(200, {
            'critical_alerts': items,
            'count': len(items)
        })
        
    except Exception as e:
        logger.error(f"Error querying critical alerts: {e}")
        return create_response(500, {'error': 'Failed to retrieve critical alerts'})

def get_available_crops():
    """
    Obtener lista de cultivos disponibles
    """
    try:
        response = alerts_table.scan(
            ProjectionExpression='crop, crop_name'
        )
        
        crops = {}
        for item in response['Items']:
            crop_code = item.get('crop')
            crop_name = item.get('crop_name')
            if crop_code and crop_name:
                crops[crop_code] = crop_name
        
        return create_response(200, {
            'crops': crops,
            'count': len(crops)
        })
        
    except Exception as e:
        logger.error(f"Error getting crops: {e}")
        return create_response(500, {'error': 'Failed to retrieve crops'})

def create_response(status_code, body):
    """
    Crear respuesta HTTP estándar
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }
'''
        
        return self.create_lambda_function('alerts-api', function_code, 'alerts_api.lambda_handler')
    
    def create_metrics_api_function(self):
        """
        Crear función Lambda para API de métricas
        """
        function_code = '''
import json
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Inicializar DynamoDB
dynamodb = boto3.resource('dynamodb')
metrics_table = dynamodb.Table('openmeteo-weather-monthly-metrics-dev')

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    """
    API Handler para métricas y analytics
    """
    try:
        http_method = event.get('httpMethod', 'GET')
        path = event.get('path', '')
        query_params = event.get('queryStringParameters') or {}
        
        logger.info(f"Method: {http_method}, Path: {path}")
        
        if http_method == 'GET':
            if '/metrics' in path:
                return get_metrics(query_params)
            elif '/dashboard' in path:
                return get_dashboard_data(query_params)
            elif '/summary' in path:
                return get_summary_stats()
            else:
                return create_response(400, {'error': 'Invalid path'})
        else:
            return create_response(405, {'error': 'Method not allowed'})
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})

def get_metrics(params):
    """
    Obtener métricas por estado
    """
    state = params.get('state')
    year = params.get('year')
    month = params.get('month')
    
    try:
        if state:
            response = metrics_table.query(
                KeyConditionExpression=Key('state').eq(state),
                ScanIndexForward=False
            )
        else:
            response = metrics_table.scan()
        
        items = response['Items']
        
        # Filtrar por año/mes si se especifica
        if year:
            items = [item for item in items if str(item.get('year')) == year]
        if month:
            items = [item for item in items if str(item.get('month')) == month]
        
        return create_response(200, {
            'metrics': items,
            'count': len(items)
        })
        
    except Exception as e:
        logger.error(f"Error querying metrics: {e}")
        return create_response(500, {'error': 'Failed to retrieve metrics'})

def get_dashboard_data(params):
    """
    Obtener datos para dashboard principal
    """
    try:
        # Obtener métricas recientes
        response = metrics_table.scan(Limit=50)
        items = response['Items']
        
        # Calcular estadísticas agregadas
        if items:
            total_states = len(set(item['state'] for item in items))
            avg_temp = sum(item.get('avg_temp_max', 0) for item in items) / len(items)
            total_precipitation = sum(item.get('total_precipitation', 0) for item in items)
            high_risk_days = sum(item.get('total_hot_days', 0) for item in items)
            
            dashboard_data = {
                'summary': {
                    'total_states': total_states,
                    'avg_temperature': round(avg_temp, 1),
                    'total_precipitation': round(total_precipitation, 1),
                    'high_risk_days': int(high_risk_days)
                },
                'recent_metrics': items[:10],  # Últimas 10
                'last_updated': datetime.now().isoformat()
            }
        else:
            dashboard_data = {
                'summary': {},
                'recent_metrics': [],
                'last_updated': datetime.now().isoformat()
            }
        
        return create_response(200, dashboard_data)
        
    except Exception as e:
        logger.error(f"Error getting dashboard data: {e}")
        return create_response(500, {'error': 'Failed to retrieve dashboard data'})

def get_summary_stats():
    """
    Obtener estadísticas resumen del sistema
    """
    try:
        # Obtener conteos básicos
        response = metrics_table.scan(Select='COUNT')
        metrics_count = response['Count']
        
        summary = {
            'system_stats': {
                'metrics_periods': metrics_count,
                'last_check': datetime.now().isoformat(),
                'system_status': 'operational'
            },
            'data_health': 'operational' if metrics_count > 0 else 'no_data'
        }
        
        return create_response(200, summary)
        
    except Exception as e:
        logger.error(f"Error getting summary: {e}")
        return create_response(500, {'error': 'Failed to retrieve summary'})

def create_response(status_code, body):
    """
    Crear respuesta HTTP estándar
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }
'''
        
        return self.create_lambda_function('metrics-api', function_code, 'metrics_api.lambda_handler')
    
    def create_lambda_function(self, function_name, code, handler):
        """
        Crear función Lambda individual usando LabRole
        """
        full_function_name = f"{self.function_prefix}-{function_name}-{self.environment}"
        
        try:
            logger.info(f"🚀 Creando función Lambda: {full_function_name}")
            
            # Crear archivo ZIP temporal
            with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
                with zipfile.ZipFile(tmp_file.name, 'w') as zip_file:
                    zip_file.writestr(f"{function_name.replace('-', '_')}.py", code)
                
                tmp_file_path = tmp_file.name
            
            # Leer contenido del ZIP
            with open(tmp_file_path, 'rb') as zip_file:
                zip_content = zip_file.read()
            
            # Crear función Lambda con LabRole
            response = self.lambda_client.create_function(
                FunctionName=full_function_name,
                Runtime='python3.9',
                Role=self.lab_role_arn,
                Handler=handler,
                Code={'ZipFile': zip_content},
                Description=f'OpenMeteo {function_name} API endpoint',
                Timeout=30,
                MemorySize=512,
                Environment={
                    'Variables': {
                        'ENVIRONMENT': self.environment,
                        'REGION': self.region
                    }
                },
                Tags={
                    'Project': 'OpenMeteoWeather',
                    'Environment': self.environment
                }
            )
            
            # Limpiar archivo temporal
            os.unlink(tmp_file_path)
            
            logger.info(f"✅ Función creada: {response['FunctionArn']}")
            return response
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                logger.info(f"⚠️ Función {full_function_name} ya existe")
                return {'FunctionName': full_function_name, 'Status': 'Already exists'}
            else:
                logger.error(f"❌ Error creando función {full_function_name}: {e}")
                raise
        finally:
            # Limpiar archivo temporal
            try:
                if 'tmp_file_path' in locals():
                    os.unlink(tmp_file_path)
            except:
                pass
    
    def create_all_functions(self):
        """
        Crear todas las funciones Lambda con LabRole
        """
        logger.info("🚀 CREANDO LAMBDA FUNCTIONS CON AWS ACADEMY LABROLE")
        logger.info("="*65)
        
        # 1. Obtener LabRole ARN
        self.lab_role_arn = self.get_lab_role_arn()
        
        # 2. Crear funciones
        functions_created = {}
        
        logger.info("📡 Creando función Weather API...")
        functions_created['weather'] = self.create_weather_api_function()
        
        logger.info("⚠️ Creando función Alerts API...")
        functions_created['alerts'] = self.create_alerts_api_function()
        
        logger.info("📊 Creando función Metrics API...")
        functions_created['metrics'] = self.create_metrics_api_function()
        
        logger.info("✅ Todas las funciones Lambda creadas")
        return functions_created
    
    def list_functions(self):
        """
        Listar funciones Lambda creadas
        """
        logger.info("📋 FUNCIONES LAMBDA DISPONIBLES")
        logger.info("="*40)
        
        try:
            response = self.lambda_client.list_functions()
            
            openmeteo_functions = [
                func for func in response['Functions']
                if func['FunctionName'].startswith(self.function_prefix)
            ]
            
            for func in openmeteo_functions:
                logger.info(f"🚀 {func['FunctionName']}")
                logger.info(f"   Runtime: {func['Runtime']}")
                logger.info(f"   Handler: {func['Handler']}")
                logger.info(f"   Last Modified: {func['LastModified']}")
                logger.info(f"   Memory: {func['MemorySize']} MB")
                logger.info()
            
            return openmeteo_functions
            
        except Exception as e:
            logger.error(f"❌ Error listando funciones: {e}")
            return []
    
    def test_function(self, function_name):
        """
        Probar función Lambda con evento de prueba
        """
        full_function_name = f"{self.function_prefix}-{function_name}-{self.environment}"
        
        # Evento de prueba para API Gateway
        test_event = {
            'httpMethod': 'GET',
            'path': f'/{function_name}',
            'queryStringParameters': {'limit': '5'},
            'headers': {},
            'body': None
        }
        
        try:
            logger.info(f"🧪 Probando función: {full_function_name}")
            
            response = self.lambda_client.invoke(
                FunctionName=full_function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(test_event)
            )
            
            result = json.loads(response['Payload'].read())
            
            logger.info(f"✅ Función respondió con status: {result.get('statusCode', 'Unknown')}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Error probando función {full_function_name}: {e}")
            return None

def main():
    """
    Script principal para AWS Academy
    """
    print("🚀 LAMBDA FUNCTIONS SETUP - AWS ACADEMY COMPATIBLE")
    print("="*65)
    print(f"🕐 Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*65)
    
    # Configuración
    region = 'us-east-1'  # Fijo para Academy
    
    # Inicializar setup
    setup = LambdaFunctionsAcademySetup(region=region)
    
    try:
        # Crear todas las funciones
        print("\n🚀 CREANDO FUNCIONES LAMBDA")
        functions = setup.create_all_functions()
        
        # Listar funciones creadas
        print("\n📋 VERIFICANDO FUNCIONES CREADAS")
        available_functions = setup.list_functions()
        
        # Probar una función
        if available_functions:
            print("\n🧪 PROBANDO FUNCIÓN WEATHER API")
            test_result = setup.test_function('weather-api')
            if test_result:
                logger.info(f"Respuesta de prueba: {test_result.get('statusCode')}")
        
        print("\n" + "="*65)
        print("🎉 SETUP LAMBDA COMPLETADO EXITOSAMENTE")
        print("="*65)
        
        print(f"📊 RESUMEN:")
        print(f"   Funciones creadas: {len(available_functions)}")
        print(f"   Región: {region}")
        print(f"   Rol usado: LabRole")
        
        print(f"\n🌐 FUNCIONES DISPONIBLES:")
        for func in available_functions:
            endpoint_type = func['FunctionName'].split('-')[-2]
            print(f"   {endpoint_type.upper()}: {func['FunctionName']}")
        
        print(f"\n🚀 PRÓXIMO PASO: Configurar API Gateway")
        print(f"💡 Tip: Las funciones están listas para conectar con API Gateway")
        
    except Exception as e:
        logger.error(f"❌ Error en setup Lambda: {e}")
        print(f"\n💡 El LabRole debe tener permisos para:")
        print(f"   - Crear funciones Lambda")
        print(f"   - Acceso a DynamoDB")

if __name__ == "__main__":
    main()
