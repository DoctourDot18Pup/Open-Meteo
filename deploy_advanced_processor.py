import boto3
import json
import zipfile
import os
import tempfile
import logging
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AdvancedProcessorDeployer:
    def __init__(self, region='us-east-1'):
        self.region = region
        self.lambda_client = boto3.client('lambda', region_name=region)
        self.sts_client = boto3.client('sts', region_name=region)
        self.function_name = 'openmeteo-weather-alerts-processor-dev' # NOMBRE NUEVO Y ÚNICO
        
    def get_lab_role_arn(self):
        try:
            account_id = self.sts_client.get_caller_identity()['Account']
            return f"arn:aws:iam::{account_id}:role/LabRole"
        except Exception as e:
            logger.error(f"❌ Error obteniendo LabRole: {e}")
            raise

    def get_function_code(self):
        # Este es el código de la lógica avanzada (VPD, Dew Point, Ventanas)
        return '''
import json
import boto3
import math
import logging
from datetime import datetime, timedelta
from decimal import Decimal

# Configuración
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
# Aseguramos que apunte a la tabla correcta creada en dynamodb_setup.py
ALERTS_TABLE = dynamodb.Table('openmeteo-weather-agricultural-alerts-dev')

# --- LÓGICA MATEMÁTICA ---

def calculate_vpd(temp_c, rh):
    """Calcula Déficit de Presión de Vapor (kPa)"""
    if temp_c is None or rh is None: return 0.0
    svp = 0.6108 * math.exp((17.27 * temp_c) / (temp_c + 237.3))
    avp = svp * (rh / 100.0)
    return round(max(0, svp - avp), 2)

def calculate_dew_point(temp_c, rh):
    """Calcula Punto de Rocío (°C)"""
    if temp_c is None or rh is None: return 0.0
    a, b = 17.27, 237.7
    try:
        alpha = ((a * temp_c) / (b + temp_c)) + math.log(rh / 100.0)
        return round((b * alpha) / (a - alpha), 2)
    except:
        return temp_c

# --- EVALUADORES ---

def evaluate_risk(data):
    alerts = []
    state = data.get('state', 'Desconocido')
    crop = data.get('crop', 'General')
    temp = float(data.get('temp_max', 25))
    hum = float(data.get('humidity', 50))
    wind = float(data.get('wind_speed', 0))
    
    # 1. VPD (Estrés)
    vpd = calculate_vpd(temp, hum)
    if vpd > 1.6:
        alerts.append({
            'subtype': 'VPD_ALTO', 'severity': 'CRITICO',
            'msg': f"VPD {vpd}kPa. Cierre estomático. Riego urgente.",
            'act': "Riego foliar inmediato.", 'val': vpd
        })
    elif vpd < 0.4:
        alerts.append({
            'subtype': 'VPD_BAJO', 'severity': 'ALTO',
            'msg': f"VPD {vpd}kPa. Aire saturado. Riesgo fúngico.",
            'act': "Ventilar/Suspender riego.", 'val': vpd
        })

    # 2. HELADA RADIATIVA (Dew Point)
    dp = calculate_dew_point(float(data.get('temp_min', temp)), hum)
    if dp < 2.0 and float(data.get('temp_min', temp)) < 6.0:
        alerts.append({
            'subtype': 'HELADA_RADIATIVA', 'severity': 'CRITICO',
            'msg': f"Punto rocío {dp}°C. Helada inminente.",
            'act': "Activar riego anti-helada.", 'val': dp
        })

    # 3. VENTANA FUMIGACION
    if wind < 15 and temp < 27 and hum > 40:
        alerts.append({
            'subtype': 'VENTANA_OPTIMA', 'severity': 'INFO',
            'msg': "Condiciones ideales para fumigar.",
            'act': "Aplicar agroquímicos ahora.", 'val': 1
        })
        
    return alerts, state, crop

def lambda_handler(event, context):
    """
    Handler principal
    """
    try:
        # Detectar si viene de API Gateway o prueba directa
        body = event
        if 'body' in event:
            body = json.loads(event['body'])
            
        # Lista de datos a procesar (puede venir del evento o usar MOCK si está vacío)
        weather_list = body.get('weather_data', [])
        
        # MOCK DATA si no se envían datos (Para probar fácil)
        if not weather_list:
            logger.info("⚠️ Usando datos de prueba internos (MOCK)...")
            weather_list = [
                {'state': 'Guanajuato', 'crop': 'Maiz', 'temp_max': 33, 'humidity': 20, 'wind_speed': 10}, # VPD Alto
                {'state': 'Chihuahua', 'crop': 'Nuez', 'temp_max': 15, 'temp_min': 3, 'humidity': 35, 'wind_speed': 2}, # Helada
                {'state': 'Sinaloa', 'crop': 'Tomate', 'temp_max': 24, 'humidity': 60, 'wind_speed': 8} # Ventana Óptima
            ]

        count = 0
        for item in weather_list:
            new_alerts, state, crop = evaluate_risk(item)
            
            for alert in new_alerts:
                # ID único compuesto
                alert_id = f"{state}_{crop}_{alert['subtype']}_{datetime.now().strftime('%Y%m%d%H%M')}"
                
                dynamodb_item = {
                    'state': state,
                    'alert_id': alert_id,
                    'crop': crop,
                    'type': 'AVANZADA',
                    'severity': alert['severity'],
                    'message': alert['msg'],
                    'actionable_insight': alert['act'],
                    'metric_value': Decimal(str(alert['val'])),
                    'timestamp': datetime.now().isoformat(),
                    'created_at': datetime.now().isoformat()
                }
                
                ALERTS_TABLE.put_item(Item=dynamodb_item)
                count += 1
                logger.info(f"✅ Alerta guardada: {alert_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Procesamiento completado', 'alerts_generated': count})
        }
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(str(e))}
'''

    def deploy(self):
        print(f"🚀 Desplegando NUEVA función: {self.function_name}")
        
        # 1. Preparar ZIP
        code = self.get_function_code()
        with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
            with zipfile.ZipFile(tmp.name, 'w') as zf:
                zf.writestr('lambda_function.py', code)
            zip_path = tmp.name

        # 2. Leer ZIP
        with open(zip_path, 'rb') as f:
            zip_content = f.read()
        os.unlink(zip_path)

        # 3. Crear/Actualizar Lambda
        try:
            self.lambda_client.create_function(
                FunctionName=self.function_name,
                Runtime='python3.9',
                Role=self.get_lab_role_arn(),
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': zip_content},
                Timeout=15,
                MemorySize=128,
                Environment={'Variables': {'ENV': 'dev'}}
            )
            print("✅ Función CREADA exitosamente.")
        except ClientError as e:
            if 'ResourceConflictException' in str(e):
                print("⚠️ La función ya existía. Actualizando código...")
                self.lambda_client.update_function_code(
                    FunctionName=self.function_name,
                    ZipFile=zip_content
                )
                print("✅ Función ACTUALIZADA exitosamente.")
            else:
                raise e

if __name__ == "__main__":
    deployer = AdvancedProcessorDeployer()
    deployer.deploy()
