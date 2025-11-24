import boto3
import os
import glob
import json
from decimal import Decimal
from botocore.exceptions import ClientError

class DynamoDBLoaderV3:
    def __init__(self):
        print("Iniciando Loader de DynamoDB (Version 3 - Ajuste de Permisos)...")
        
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datos_procesados")

        self.table_map = {
            "weather_details": "Weather",          # Intento 1: Nombre simple
            "active_alerts": "Alerts",             # Intento 1: Nombre simple
            "monthly_stats": "MonthlyStats"        # Intento 1: Nombre simple
        }

        # Configuracion de esquemas
        self.tables_config = {
            self.table_map["weather_details"]: {
                "KeySchema": [
                    {'AttributeName': 'PK_State', 'KeyType': 'HASH'},
                    {'AttributeName': 'SK_Date', 'KeyType': 'RANGE'}
                ],
                "file_source": "dynamo_weather_details"
            },
            self.table_map["active_alerts"]: {
                "KeySchema": [
                    {'AttributeName': 'PK_Alert', 'KeyType': 'HASH'},
                    {'AttributeName': 'SK_Date', 'KeyType': 'RANGE'}
                ],
                "file_source": "dynamo_active_alerts"
            },
            self.table_map["monthly_stats"]: {
                "KeySchema": [
                    {'AttributeName': 'PK_State', 'KeyType': 'HASH'},
                    {'AttributeName': 'SK_MonthYear', 'KeyType': 'RANGE'}
                ],
                "file_source": "dashboard_monthly_stats"
            }
        }

    def create_table_if_not_exists(self, table_name, key_schema):
        try:
            table = self.dynamodb.Table(table_name)
            table.load()
            print(f"[INFO] Tabla existente detectada: {table_name}")
            return table
        except ClientError as e:
            error_code = e.response['Error']['Code']
            
            if error_code == 'ResourceNotFoundException':
                print(f"[INFO] Intentando crear tabla: {table_name}...")
                try:
                    # Definir atributos (solo llaves)
                    attr_defs = []
                    for k in key_schema:
                        attr_defs.append({'AttributeName': k['AttributeName'], 'AttributeType': 'S'})

                    table = self.dynamodb.create_table(
                        TableName=table_name,
                        KeySchema=key_schema,
                        AttributeDefinitions=attr_defs,
                        BillingMode='PAY_PER_REQUEST'
                    )
                    
                    print("[ESPERA] AWS esta creando la tabla (puede tardar 10s)...")
                    table.wait_until_exists()
                    print("[EXITO] Tabla activa.")
                    return table
                    
                except ClientError as create_error:
                    # Si falla aqui, es 100% un problema de nombre prohibido
                    print(f"\n[ERROR DE PERMISOS] AWS bloqueo el nombre '{table_name}'.")
                    print("--> SOLUCION: Edita 'self.table_map' al inicio del script.")
                    print("--> PRUEBA CON: 'openmeteo-weather-data-dev' u otros nombres permitidos por tu lab.\n")
                    return None

            elif error_code == 'AccessDeniedException':
                print(f"[ACCESO DENEGADO] No tienes permiso para ver la tabla {table_name}.")
                return None
            else:
                print(f"[ERROR] {e}")
                return None

    def find_spark_json_file(self, folder_name):
        search_path = os.path.join(self.base_dir, folder_name, "part-*.json")
        files = glob.glob(search_path)
        return files[0] if files else None

    def load_data(self):
        print("\n--- FASE DE CARGA MASIVA ---")
        
        for table_key, table_name in self.table_map.items():
            # Mapeo inverso para encontrar la config correcta
            config = self.tables_config[table_name]
            
            print(f"\n[PROCESANDO] {table_name}...")
            
            # 1. Crear/Verificar
            table = self.create_table_if_not_exists(table_name, config['KeySchema'])
            if not table: continue

            # 2. Archivo
            folder = config['file_source']
            json_file = self.find_spark_json_file(folder)
            if not json_file:
                print(f"[ALERTA] No hay datos para {folder}. Saltando.")
                continue

            print(f"[INFO] Leyendo datos de Spark...")

            # 3. Carga
            count = 0
            try:
                with table.batch_writer() as batch:
                    with open(json_file, 'r') as f:
                        for line in f:
                            if not line.strip(): continue
                            
                            item = json.loads(line, parse_float=Decimal)
                            
                            # Ajuste para MonthlyStats
                            if "monthly_stats" in table_key or "Stats" in table_name:
                                if 'PK_State' not in item:
                                    item['PK_State'] = item.get('state')
                                year_val = item.get('Year')
                                month_val = str(item.get('Month')).zfill(2)
                                item['SK_MonthYear'] = f"{year_val}-{month_val}"
                            
                            batch.put_item(Item=item)
                            count += 1
                            if count % 500 == 0:
                                print(f"[PROGRESO] {count} items subidos...")
                                
                print(f"[FIN] Exito: {count} registros en {table_name}")
                
            except Exception as e:
                print(f"[ERROR CARGA] {e}")

if __name__ == "__main__":
    loader = DynamoDBLoaderV3()
    loader.load_data()
