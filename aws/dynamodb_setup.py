import boto3
import json
import os
import logging
from datetime import datetime
from decimal import Decimal
import pandas as pd
from botocore.exceptions import ClientError

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DynamoDBSetup:
    def __init__(self, region='us-east-1', prefix='openmeteo-weather'):
        """
        Inicializar configuración de DynamoDB
        """
        self.region = region
        self.prefix = prefix
        self.environment = 'dev'
        
        # Inicializar clientes AWS
        try:
            self.dynamodb = boto3.resource('dynamodb', region_name=region)
            self.dynamodb_client = boto3.client('dynamodb', region_name=region)
            logger.info(f"✅ Cliente DynamoDB inicializado en región: {region}")
        except Exception as e:
            logger.error(f"❌ Error conectando a AWS: {e}")
            raise
        
        # Definir nombres de tablas
        self.table_names = {
            'weather': f"{prefix}-weather-data-{self.environment}",
            'alerts': f"{prefix}-agricultural-alerts-{self.environment}",
            'metrics': f"{prefix}-monthly-metrics-{self.environment}",
            'users': f"{prefix}-user-sessions-{self.environment}"
        }
    
    def create_weather_data_table(self):
        """
        Crear tabla para datos meteorológicos diarios
        """
        table_name = self.table_names['weather']
        
        try:
            logger.info(f"📊 Creando tabla: {table_name}")
            
            table = self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'state',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'sort_key',  # date
                        'KeyType': 'RANGE'  # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'state',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'sort_key',
                        'AttributeType': 'S'
                    }
                ],
                BillingMode='PAY_PER_REQUEST',  # On-demand pricing
                Tags=[
                    {
                        'Key': 'Project',
                        'Value': 'OpenMeteoWeather'
                    },
                    {
                        'Key': 'Environment',
                        'Value': self.environment
                    }
                ]
            )
            
            # Esperar a que la tabla esté activa
            table.wait_until_exists()
            logger.info(f"✅ Tabla {table_name} creada exitosamente")
            return table
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                logger.info(f"⚠️ Tabla {table_name} ya existe")
                return self.dynamodb.Table(table_name)
            else:
                logger.error(f"❌ Error creando tabla {table_name}: {e}")
                raise
    
    def create_alerts_table(self):
        """
        Crear tabla para alertas agrícolas
        """
        table_name = self.table_names['alerts']
        
        try:
            logger.info(f"⚠️ Creando tabla: {table_name}")
            
            table = self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'state',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'alert_id',
                        'KeyType': 'RANGE'  # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'state',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'alert_id',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'severity',
                        'AttributeType': 'S'
                    }
                ],
                BillingMode='PAY_PER_REQUEST',
                GlobalSecondaryIndexes=[
                    {
                        'IndexName': 'severity-index',
                        'KeySchema': [
                            {
                                'AttributeName': 'severity',
                                'KeyType': 'HASH'
                            }
                        ],
                        'Projection': {
                            'ProjectionType': 'ALL'
                        }
                    }
                ],
                Tags=[
                    {
                        'Key': 'Project',
                        'Value': 'OpenMeteoWeather'
                    },
                    {
                        'Key': 'Environment',
                        'Value': self.environment
                    }
                ]
            )
            
            table.wait_until_exists()
            logger.info(f"✅ Tabla {table_name} creada exitosamente")
            return table
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                logger.info(f"⚠️ Tabla {table_name} ya existe")
                return self.dynamodb.Table(table_name)
            else:
                logger.error(f"❌ Error creando tabla {table_name}: {e}")
                raise
    
    def create_metrics_table(self):
        """
        Crear tabla para métricas mensuales agregadas
        """
        table_name = self.table_names['metrics']
        
        try:
            logger.info(f"📈 Creando tabla: {table_name}")
            
            table = self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'state',
                        'KeyType': 'HASH'  # Partition key
                    },
                    {
                        'AttributeName': 'period_id',  # YYYY-MM
                        'KeyType': 'RANGE'  # Sort key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'state',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'period_id',
                        'AttributeType': 'S'
                    }
                ],
                BillingMode='PAY_PER_REQUEST',
                Tags=[
                    {
                        'Key': 'Project',
                        'Value': 'OpenMeteoWeather'
                    },
                    {
                        'Key': 'Environment',
                        'Value': self.environment
                    }
                ]
            )
            
            table.wait_until_exists()
            logger.info(f"✅ Tabla {table_name} creada exitosamente")
            return table
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                logger.info(f"⚠️ Tabla {table_name} ya existe")
                return self.dynamodb.Table(table_name)
            else:
                logger.error(f"❌ Error creando tabla {table_name}: {e}")
                raise
    
    def create_user_sessions_table(self):
        """
        Crear tabla para sesiones de usuario con TTL
        """
        table_name = self.table_names['users']
        
        try:
            logger.info(f"👥 Creando tabla: {table_name}")
            
            table = self.dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {
                        'AttributeName': 'user_id',
                        'KeyType': 'HASH'  # Partition key
                    }
                ],
                AttributeDefinitions=[
                    {
                        'AttributeName': 'user_id',
                        'AttributeType': 'S'
                    }
                ],
                BillingMode='PAY_PER_REQUEST',
                Tags=[
                    {
                        'Key': 'Project',
                        'Value': 'OpenMeteoWeather'
                    },
                    {
                        'Key': 'Environment',
                        'Value': self.environment
                    }
                ]
            )
            
            table.wait_until_exists()
            
            # Configurar TTL para expiración automática de sesiones
            self.dynamodb_client.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={
                    'AttributeName': 'expires_at',
                    'Enabled': True
                }
            )
            
            logger.info(f"✅ Tabla {table_name} creada con TTL exitosamente")
            return table
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceInUseException':
                logger.info(f"⚠️ Tabla {table_name} ya existe")
                return self.dynamodb.Table(table_name)
            else:
                logger.error(f"❌ Error creando tabla {table_name}: {e}")
                raise
    
    def create_all_tables(self):
        """
        Crear todas las tablas necesarias
        """
        logger.info("🏗️ CREANDO TODAS LAS TABLAS DYNAMODB")
        logger.info("="*60)
        
        tables_created = {}
        
        try:
            # Crear cada tabla
            tables_created['weather'] = self.create_weather_data_table()
            tables_created['alerts'] = self.create_alerts_table()
            tables_created['metrics'] = self.create_metrics_table()
            tables_created['users'] = self.create_user_sessions_table()
            
            logger.info("✅ Todas las tablas creadas exitosamente")
            return tables_created
            
        except Exception as e:
            logger.error(f"❌ Error en creación de tablas: {e}")
            raise
    
    def load_json_to_dynamodb(self, json_file_path, table_name, batch_size=25):
        """
        Cargar datos desde archivo JSON a tabla DynamoDB
        """
        table = self.dynamodb.Table(table_name)
        
        try:
            logger.info(f"📥 Cargando datos desde {json_file_path} a {table_name}")
            
            # Leer archivo JSON Lines
            items = []
            with open(json_file_path, 'r', encoding='utf-8') as f:
                for line_number, line in enumerate(f, 1):
                    if line.strip():
                        try:
                            item = json.loads(line, parse_float=Decimal)
                            items.append(item)
                        except json.JSONDecodeError as e:
                            logger.warning(f"⚠️ Error en línea {line_number}: {e}")
                            continue
            
            logger.info(f"📊 Total de items a cargar: {len(items)}")
            
            # Carga en lotes
            loaded_items = 0
            failed_items = 0
            
            for i in range(0, len(items), batch_size):
                batch = items[i:i+batch_size]
                
                try:
                    with table.batch_writer() as batch_writer:
                        for item in batch:
                            batch_writer.put_item(Item=item)
                    
                    loaded_items += len(batch)
                    logger.info(f"📦 Lote cargado: {loaded_items}/{len(items)} items")
                    
                except ClientError as e:
                    failed_items += len(batch)
                    logger.error(f"❌ Error cargando lote: {e}")
            
            logger.info(f"✅ Carga completada: {loaded_items} exitosos, {failed_items} fallidos")
            return loaded_items, failed_items
            
        except FileNotFoundError:
            logger.error(f"❌ Archivo no encontrado: {json_file_path}")
            return 0, 0
        except Exception as e:
            logger.error(f"❌ Error cargando datos: {e}")
            return 0, 0
    
    def load_all_processed_data(self):
        """
        Cargar todos los datos procesados a DynamoDB
        """
        logger.info("🚀 CARGANDO TODOS LOS DATOS PROCESADOS")
        logger.info("="*60)
        
        # Archivos y tablas correspondientes
        file_mappings = [
            {
                'file_pattern': 'processed_weather_data_dynamo/weather_data_*.json',
                'table_key': 'weather',
                'description': 'Datos meteorológicos'
            },
            {
                'file_pattern': 'processed_alerts_dynamo/agricultural_alerts_*.json',
                'table_key': 'alerts',
                'description': 'Alertas agrícolas'
            },
            {
                'file_pattern': 'processed_monthly_metrics_dynamo/monthly_metrics_*.json',
                'table_key': 'metrics',
                'description': 'Métricas mensuales'
            }
        ]
        
        results = {}
        
        for mapping in file_mappings:
            # Buscar archivo más reciente
            import glob
            files = glob.glob(mapping['file_pattern'])
            
            if files:
                latest_file = max(files, key=os.path.getctime)
                table_name = self.table_names[mapping['table_key']]
                
                logger.info(f"📁 {mapping['description']}: {latest_file}")
                
                loaded, failed = self.load_json_to_dynamodb(
                    latest_file, 
                    table_name
                )
                
                results[mapping['description']] = {
                    'file': latest_file,
                    'loaded': loaded,
                    'failed': failed
                }
            else:
                logger.warning(f"⚠️ No se encontraron archivos para: {mapping['description']}")
                results[mapping['description']] = {
                    'file': 'No encontrado',
                    'loaded': 0,
                    'failed': 0
                }
        
        return results
    
    def verify_data_load(self):
        """
        Verificar que los datos se cargaron correctamente
        """
        logger.info("🔍 VERIFICANDO CARGA DE DATOS")
        logger.info("="*50)
        
        for table_key, table_name in self.table_names.items():
            if table_key != 'users':  # Skip user sessions table
                try:
                    table = self.dynamodb.Table(table_name)
                    response = table.scan(Select='COUNT')
                    count = response['Count']
                    
                    logger.info(f"📊 {table_key.upper()}: {count} registros")
                    
                    # Mostrar ejemplo de registro
                    if count > 0:
                        sample = table.scan(Limit=1)
                        if sample['Items']:
                            logger.info(f"   Ejemplo: {list(sample['Items'][0].keys())}")
                    
                except Exception as e:
                    logger.error(f"❌ Error verificando {table_name}: {e}")
    
    def get_table_info(self):
        """
        Obtener información de todas las tablas creadas
        """
        logger.info("📋 INFORMACIÓN DE TABLAS DYNAMODB")
        logger.info("="*50)
        
        for table_key, table_name in self.table_names.items():
            try:
                table = self.dynamodb.Table(table_name)
                
                logger.info(f"🗃️ {table_key.upper()}:")
                logger.info(f"   Nombre: {table_name}")
                logger.info(f"   Estado: {table.table_status}")
                logger.info(f"   Región: {self.region}")
                logger.info(f"   ARN: {table.table_arn}")
                logger.info()
                
            except Exception as e:
                logger.error(f"❌ Error obteniendo info de {table_name}: {e}")

def main():
    """
    Función principal para setup completo de DynamoDB
    """
    print("🏗️ SETUP AWS DYNAMODB - OPENMETEO WEATHER PROJECT")
    print("="*70)
    print(f"🕐 Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Configuración
    region = input("🌍 ¿Región AWS? (default: us-east-1): ").strip() or 'us-east-1'
    prefix = input("🏷️ ¿Prefijo para tablas? (default: openmeteo-weather): ").strip() or 'openmeteo-weather'
    
    # Inicializar setup
    setup = DynamoDBSetup(region=region, prefix=prefix)
    
    try:
        # 1. Crear todas las tablas
        print("\n📊 PASO 1: CREANDO TABLAS")
        tables = setup.create_all_tables()
        
        # 2. Cargar datos procesados
        print("\n📥 PASO 2: CARGANDO DATOS")
        results = setup.load_all_processed_data()
        
        # 3. Verificar carga
        print("\n🔍 PASO 3: VERIFICANDO DATOS")
        setup.verify_data_load()
        
        # 4. Mostrar información final
        print("\n📋 PASO 4: INFORMACIÓN FINAL")
        setup.get_table_info()
        
        # Resumen final
        print("\n" + "="*70)
        print("🎉 SETUP DYNAMODB COMPLETADO EXITOSAMENTE")
        print("="*70)
        
        total_loaded = sum(r.get('loaded', 0) for r in results.values())
        total_failed = sum(r.get('failed', 0) for r in results.values())
        
        print(f"📊 RESUMEN DE CARGA:")
        for desc, result in results.items():
            print(f"   {desc}: {result['loaded']} registros cargados")
        
        print(f"\n✅ Total exitoso: {total_loaded} registros")
        if total_failed > 0:
            print(f"⚠️ Total fallido: {total_failed} registros")
        
        print(f"\n🚀 PRÓXIMO PASO: Crear Lambda Functions")
        print(f"🌐 Región: {region}")
        print(f"🏷️ Prefijo: {prefix}")
        
    except Exception as e:
        logger.error(f"❌ Error en setup: {e}")
        print(f"\n💡 Verifica tu configuración AWS:")
        print(f"   aws configure")
        print(f"   aws sts get-caller-identity")

if __name__ == "__main__":
    main()
