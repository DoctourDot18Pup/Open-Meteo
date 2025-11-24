import boto3
import zipfile
import os
import tempfile
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AlertsLogicUpdater:
    def __init__(self, region='us-east-1'):
        self.region = region
        self.lambda_client = boto3.client('lambda', region_name=region)
        self.function_name = 'openmeteo-weather-alerts-api-dev' # La función existente
        
    def get_new_function_code(self):
        # Este código incluye TODA la lógica anterior + la NUEVA lógica de Risk Map
        return '''
import json
import boto3
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
alerts_table = dynamodb.Table('openmeteo-weather-agricultural-alerts-dev')

# Lista maestra para garantizar que el mapa siempre tenga 32 estados
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

def lambda_handler(event, context):
    try:
        http_method = event.get('httpMethod', 'GET')
        path = event.get('path', '')
        query_params = event.get('queryStringParameters') or {}
        
        logger.info(f"Method: {http_method}, Params: {query_params}")
        
        if http_method == 'GET':
            # --- NUEVA LÓGICA: Detección de modo "risk_map" ---
            mode = query_params.get('mode')
            
            if mode == 'risk_map':
                return get_national_risk_map()
            elif '/critical' in path:
                return get_critical_alerts(query_params)
            elif '/crops' in path:
                return get_available_crops()
            else:
                # Comportamiento por defecto (listar alertas)
                return get_alerts(query_params)
        else:
            return create_response(405, {'error': 'Method not allowed'})
    
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return create_response(500, {'error': str(e)})

def get_national_risk_map():
    """
    Calcula el riesgo acumulado por estado (0-100)
    """
    try:
        # 1. Obtener todas las alertas (solo campos necesarios para ahorrar lectura)
        response = alerts_table.scan(
            ProjectionExpression="#st, severity", 
            ExpressionAttributeNames={"#st": "state"}
        )
        items = response['Items']
        
        # 2. Inicializar contadores en 0
        risk_scores = {state: 0 for state in ALL_STATES}
        
        # 3. Sumar puntos según severidad
        for item in items:
            state = item.get('state')
            severity = item.get('severity', 'INFO')
            
            if state in risk_scores:
                if severity == 'CRITICO': risk_scores[state] += 30
                elif severity == 'ALTO': risk_scores[state] += 15
                elif severity == 'MEDIO': risk_scores[state] += 10
                else: risk_scores[state] += 5 # INFO
        
        # 4. Formatear y Normalizar (Tope 100)
        heatmap_data = []
        for state, score in risk_scores.items():
            final_risk = min(100, score)
            heatmap_data.append({"state": state, "risk": final_risk})
            
        # 5. Calcular Top 5 para gráficos
        top_risks = sorted(heatmap_data, key=lambda x: x['risk'], reverse=True)[:5]
        
        return create_response(200, {
            "heatmap_data": heatmap_data,
            "top_risks": top_risks,
            "total_alerts_analyzed": len(items)
        })
        
    except Exception as e:
        logger.error(f"Error calculating risk map: {e}")
        return create_response(500, {'error': 'Failed to calculate risk map'})

def get_alerts(params):
    state = params.get('state')
    crop = params.get('crop')
    limit = int(params.get('limit', 50))
    try:
        if state:
            response = alerts_table.query(
                KeyConditionExpression=Key('state').eq(state),
                ScanIndexForward=False, Limit=limit
            )
        else:
            response = alerts_table.scan(Limit=limit)
        
        items = response['Items']
        if crop: items = [i for i in items if i.get('crop') == crop]
        
        return create_response(200, {'alerts': items, 'count': len(items)})
    except Exception as e:
        return create_response(500, {'error': str(e)})

def get_critical_alerts(params):
    try:
        response = alerts_table.scan(FilterExpression=Attr('severity').eq('CRÍTICO'), Limit=50)
        return create_response(200, {'critical_alerts': response['Items']})
    except Exception as e:
        return create_response(500, {'error': str(e)})

def get_available_crops():
    try:
        response = alerts_table.scan(ProjectionExpression='crop')
        crops = list(set([i['crop'] for i in response['Items'] if 'crop' in i]))
        return create_response(200, {'crops': crops})
    except Exception as e:
        return create_response(500, {'error': str(e)})

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
'''

    def update_function(self):
        print(f"🚀 Actualizando lógica de: {self.function_name}")
        
        # Crear ZIP en memoria
        code = self.get_new_function_code()
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
            with zipfile.ZipFile(tmp.name, 'w') as zf:
                zf.writestr('lambda_function.py', code)
            zip_path = tmp.name

        with open(zip_path, 'rb') as f:
            zip_content = f.read()
        os.unlink(zip_path)

        try:
            self.lambda_client.update_function_code(
                FunctionName=self.function_name,
                ZipFile=zip_content
            )
            print("✅ Función actualizada exitosamente con endpoint /risk-map")
        except ClientError as e:
            print(f"❌ Error actualizando función: {e}")
            raise

if __name__ == "__main__":
    updater = AlertsLogicUpdater()
    updater.update_function()
