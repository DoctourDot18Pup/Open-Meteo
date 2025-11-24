import boto3
import zipfile
import os
import json
import logging
import time
from decimal import Decimal
from botocore.exceptions import ClientError

# Configuración
FUNCTION_NAME = 'openmeteo-weather-alerts-api-dev'
REGION = 'us-east-1'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

def create_code_package():
    # El código completo y correcto de la Lambda
    code = r'''import json
import boto3
import logging
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
# Intentamos obtener la tabla dinámicamente o usamos el nombre fijo
try:
    alerts_table = dynamodb.Table('openmeteo-weather-agricultural-alerts-dev')
except:
    alerts_table = None

ALL_STATES = [
    "Aguascalientes", "Baja California", "Baja California Sur", "Campeche", "Chiapas", "Chihuahua",
    "Ciudad de México", "Coahuila", "Colima", "Durango", "Guanajuato", "Guerrero", "Hidalgo", "Jalisco",
    "México", "Michoacán", "Morelos", "Nayarit", "Nuevo León", "Oaxaca", "Puebla", "Querétaro",
    "Quintana Roo", "San Luis Potosí", "Sinaloa", "Sonora", "Tabasco", "Tamaulipas", "Tlaxcala",
    "Veracruz", "Yucatán", "Zacatecas"
]

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal): return float(o)
        return super(DecimalEncoder, self).default(o)

def create_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(body, cls=DecimalEncoder)
    }

def get_national_risk_map():
    try:
        if not alerts_table: return create_response(500, {'error': 'Table not found'})
        
        response = alerts_table.scan(ProjectionExpression="#st, severity", ExpressionAttributeNames={"#st": "state"})
        items = response.get('Items', [])
        
        risk_scores = {state: 0 for state in ALL_STATES}
        
        for item in items:
            state = item.get('state')
            severity = item.get('severity', 'INFO')
            if state in risk_scores:
                if severity == 'CRITICO': risk_scores[state] += 30
                elif severity == 'ALTO': risk_scores[state] += 15
                elif severity == 'MEDIO': risk_scores[state] += 10
                else: risk_scores[state] += 5

        heatmap_data = []
        for state, score in risk_scores.items():
            heatmap_data.append({"state": state, "risk": min(100, score)})
            
        top_risks = sorted(heatmap_data, key=lambda x: x['risk'], reverse=True)[:5]
        return create_response(200, {"heatmap_data": heatmap_data, "top_risks": top_risks})
    except Exception as e:
        logger.error(f"Risk Map Error: {str(e)}")
        return create_response(500, {'error': str(e)})

def get_alerts(params):
    state = params.get('state')
    limit = int(params.get('limit', 50))
    try:
        if state:
            response = alerts_table.query(KeyConditionExpression=Key('state').eq(state), ScanIndexForward=False, Limit=limit)
        else:
            response = alerts_table.scan(Limit=limit)
        return create_response(200, {'alerts': response.get('Items', []), 'count': response['Count']})
    except Exception as e:
        logger.error(f"Alerts Error: {str(e)}")
        return create_response(500, {'error': str(e)})

def lambda_handler(event, context):
    try:
        http_method = event.get('httpMethod', 'GET')
        query_params = event.get('queryStringParameters') or {}
        
        if http_method == 'GET':
            if query_params.get('mode') == 'risk_map':
                return get_national_risk_map()
            else:
                return get_alerts(query_params)
        return create_response(405, {'error': 'Method not allowed'})
    except Exception as e:
        logger.error(f"Handler Error: {str(e)}")
        return create_response(500, {'error': str(e)})
'''
    
    # 1. Escribir archivo estándar
    filename = 'lambda_function.py'
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(code)
    
    # 2. Crear ZIP asegurando la raíz (arcname)
    zip_name = 'reset_package.zip'
    with zipfile.ZipFile(zip_name, 'w') as z:
        z.write(filename, arcname=filename)
    
    os.remove(filename)
    return zip_name

def force_reset():
    client = boto3.client('lambda', region_name=REGION)
    zip_file = create_code_package()
    
    print(f"🔧 INICIANDO RESET DE: {FUNCTION_NAME}")
    
    try:
        # PASO 1: Cambiar configuración del Handler a estándar
        print("1️⃣  Cambiando Handler a 'lambda_function.lambda_handler'...")
        client.update_function_configuration(
            FunctionName=FUNCTION_NAME,
            Handler='lambda_function.lambda_handler'
        )
        time.sleep(2) # Esperar propagación
        
        # PASO 2: Subir código nuevo
        print("2️⃣  Subiendo código estandarizado...")
        with open(zip_file, 'rb') as f:
            zipped_code = f.read()
            
        client.update_function_code(
            FunctionName=FUNCTION_NAME,
            ZipFile=zipped_code
        )
        
        # Esperar a que la actualización termine
        time.sleep(5)
        print("✅ Configuración y Código actualizados.")
        
        # PASO 3: Prueba final
        print("🧪 Ejecutando prueba de humo...")
        payload = {"httpMethod": "GET", "queryStringParameters": {"mode": "risk_map"}}
        
        response = client.invoke(
            FunctionName=FUNCTION_NAME,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        res_payload = json.loads(response['Payload'].read())
        
        if 'FunctionError' in response:
            print(f"❌ Falló: {res_payload}")
        else:
            print(f"✅ ¡ÉXITO TOTAL! Status: {res_payload.get('statusCode')}")
            body = json.loads(res_payload.get('body', '{}'))
            if 'heatmap_data' in body:
                print(f"🗺️  Mapa cargado correctamente ({len(body['heatmap_data'])} estados).")
            else:
                print(f"⚠️ Respuesta: {body}")
                
    except Exception as e:
        print(f"❌ Error crítico: {e}")
    finally:
        if os.path.exists(zip_file):
            os.remove(zip_file)

if __name__ == "__main__":
    force_reset()
