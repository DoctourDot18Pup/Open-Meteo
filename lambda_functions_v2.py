import json
import boto3
import zipfile
import os
import logging
import tempfile
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class LambdaSetupV2:
    def __init__(self):
        self.region = 'us-east-1'
        self.lambda_client = boto3.client('lambda', region_name=self.region)
        self.sts_client = boto3.client('sts', region_name=self.region)
        
        # --- NUEVA NOMENCLATURA PARA EVITAR CONFLICTOS ---
        self.function_prefix = 'AgroV2' 
        self.environment = 'dev'

    def get_lab_role_arn(self):
        """Obtiene el LabRole automáticamente"""
        try:
            account_id = self.sts_client.get_caller_identity()['Account']
            return f"arn:aws:iam::{account_id}:role/LabRole"
        except Exception as e:
            logger.error(f"❌ Error obteniendo LabRole: {e}")
            raise

    # ==============================================================================
    # 1. CÓDIGO FUENTE: WEATHER API (Con Soporte ML)
    # ==============================================================================
    def get_weather_code(self):
        return '''
import json
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
# --- TABLA CORRECTA ---
table = dynamodb.Table('Weather') 

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal): return float(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    try:
        method = event.get('httpMethod', 'GET')
        params = event.get('queryStringParameters') or {}
        
        if method == 'GET':
            state = params.get('state')
            limit = int(params.get('limit', 30))
            ml_cluster = params.get('cluster') # Filtro de IA
            
            if not state:
                return response(400, {'error': 'Falta parametro state'})

            # Query a DynamoDB
            res = table.query(
                KeyConditionExpression=Key('PK_State').eq(state),
                ScanIndexForward=False,
                Limit=limit
            )
            
            items = res.get('Items', [])

            # Filtrado ML en memoria
            if ml_cluster is not None:
                items = [i for i in items if str(i.get('cluster_clima', '')) == str(ml_cluster)]

            return response(200, {
                'state': state,
                'count': len(items),
                'ml_cluster_filter': ml_cluster,
                'data': items
            })
            
        return response(405, {'error': 'Method not allowed'})
        
    except Exception as e:
        logger.error(str(e))
        return response(500, {'error': str(e)})

def response(code, body):
    return {
        'statusCode': code,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'OPTIONS,GET'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }
'''

    # ==============================================================================
    # 2. CÓDIGO FUENTE: ALERTS API
    # ==============================================================================
    def get_alerts_code(self):
        return '''
import json
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
# --- TABLA CORRECTA ---
table = dynamodb.Table('Alerts')

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal): return float(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    try:
        method = event.get('httpMethod', 'GET')
        params = event.get('queryStringParameters') or {}
        
        if method == 'GET':
            # PK para alertas es "ALERT#{NombreEstado}"
            state = params.get('state')
            
            if not state:
                return response(400, {'error': 'Falta parametro state'})

            pk_alert = f"ALERT#{state}"

            res = table.query(
                KeyConditionExpression=Key('PK_Alert').eq(pk_alert),
                ScanIndexForward=False,
                Limit=50
            )
            
            return response(200, {'alerts': res.get('Items', [])})
            
        return response(405, {'error': 'Method not allowed'})
        
    except Exception as e:
        logger.error(str(e))
        return response(500, {'error': str(e)})

def response(code, body):
    return {
        'statusCode': code,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'OPTIONS,GET'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }
'''

    # ==============================================================================
    # 3. CÓDIGO FUENTE: METRICS API
    # ==============================================================================
    def get_metrics_code(self):
        return '''
import json
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
# --- TABLA CORRECTA ---
table = dynamodb.Table('MonthlyStats')

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal): return float(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    try:
        method = event.get('httpMethod', 'GET')
        params = event.get('queryStringParameters') or {}
        
        if method == 'GET':
            state = params.get('state')
            if not state:
                return response(400, {'error': 'Falta parametro state'})

            res = table.query(
                KeyConditionExpression=Key('PK_State').eq(state),
                ScanIndexForward=False 
            )
            
            return response(200, {'metrics': res.get('Items', [])})
            
        return response(405, {'error': 'Method not allowed'})
        
    except Exception as e:
        logger.error(str(e))
        return response(500, {'error': str(e)})

def response(code, body):
    return {
        'statusCode': code,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'OPTIONS,GET'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }
'''

    # ==============================================================================
    # LÓGICA DE DESPLIEGUE
    # ==============================================================================
    def deploy_function(self, name_suffix, code_str):
        # Nombre nuevo: AgroV2-Weather-Service-dev
        full_name = f"{self.function_prefix}-{name_suffix}-Service-{self.environment}"
        role_arn = self.get_lab_role_arn()
        
        print(f"🚀 Desplegando: {full_name}...")
        
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as t:
            with zipfile.ZipFile(t.name, 'w') as z:
                z.writestr("lambda_function.py", code_str)
            zip_path = t.name
            
        with open(zip_path, 'rb') as f:
            zip_content = f.read()
            
        try:
            self.lambda_client.create_function(
                FunctionName=full_name,
                Runtime='python3.9',
                Role=role_arn,
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': zip_content},
                Timeout=15
            )
            print(f"   ✅ Creada correctamente.")
        except ClientError as e:
            if 'ResourceConflictException' in str(e):
                print(f"   ⚠️ Ya existe. Actualizando código...")
                self.lambda_client.update_function_code(
                    FunctionName=full_name,
                    ZipFile=zip_content
                )
                print(f"   ✅ Código actualizado.")
            else:
                print(f"   ❌ Error: {e}")
        
        os.unlink(zip_path)
        return full_name

    def run(self):
        print("--- DESPLIEGUE DE BACKEND (AGRO V2) ---")
        
        # Desplegar las 3 funciones
        self.deploy_function('Weather', self.get_weather_code())
        self.deploy_function('Alerts', self.get_alerts_code())
        self.deploy_function('Metrics', self.get_metrics_code())
        
        print("\n🎉 Funciones Lambda desplegadas y listas.")
        print(f"   Prefijo: {self.function_prefix}")
        print("   Tablas conectadas: Weather, Alerts, MonthlyStats")

if __name__ == "__main__":
    setup = LambdaSetupV2()
    setup.run()
