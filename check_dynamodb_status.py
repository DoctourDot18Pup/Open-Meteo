import boto3
from datetime import datetime

def check_dynamodb_tables():
    """
    Verificar estado de las tablas DynamoDB
    """
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        
        print("🔍 VERIFICANDO ESTADO DE DYNAMODB")
        print("="*50)
        
        # Tablas esperadas
        expected_tables = [
            'openmeteo-weather-weather-data-dev',
            'openmeteo-weather-agricultural-alerts-dev', 
            'openmeteo-weather-monthly-metrics-dev',
            'openmeteo-weather-user-sessions-dev'
        ]
        
        table_status = {}
        
        for table_name in expected_tables:
            try:
                table = dynamodb.Table(table_name)
                
                # Verificar que existe
                table.load()
                
                # Contar registros
                response = table.scan(Select='COUNT')
                count = response['Count']
                
                # Obtener muestra
                if count > 0:
                    sample = table.scan(Limit=1)
                    sample_item = sample.get('Items', [])
                else:
                    sample_item = []
                
                table_status[table_name] = {
                    'exists': True,
                    'status': table.table_status,
                    'count': count,
                    'has_data': count > 0,
                    'sample': len(sample_item) > 0
                }
                
                status_emoji = "✅" if count > 0 else "⚠️"
                print(f"{status_emoji} {table_name}")
                print(f"   Estado: {table.table_status}")
                print(f"   Registros: {count:,}")
                if count > 0:
                    print(f"   ✅ Tiene datos")
                else:
                    print(f"   ❌ Tabla vacía")
                print()
                
            except Exception as e:
                table_status[table_name] = {
                    'exists': False,
                    'error': str(e)
                }
                print(f"❌ {table_name}")
                print(f"   Error: {str(e)}")
                print()
        
        return table_status
        
    except Exception as e:
        print(f"❌ Error conectando a DynamoDB: {e}")
        return {}

def check_lambda_functions():
    """
    Verificar estado de las Lambda Functions
    """
    try:
        lambda_client = boto3.client('lambda', region_name='us-east-1')
        
        print("🔍 VERIFICANDO LAMBDA FUNCTIONS")
        print("="*40)
        
        expected_functions = [
            'openmeteo-weather-weather-api-dev',
            'openmeteo-weather-alerts-api-dev',
            'openmeteo-weather-metrics-api-dev'
        ]
        
        function_status = {}
        
        for func_name in expected_functions:
            try:
                response = lambda_client.get_function(FunctionName=func_name)
                
                config = response['Configuration']
                state = config['State']
                
                function_status[func_name] = {
                    'exists': True,
                    'state': state,
                    'runtime': config['Runtime'],
                    'role': config['Role']
                }
                
                status_emoji = "✅" if state == 'Active' else "⚠️"
                print(f"{status_emoji} {func_name}")
                print(f"   Estado: {state}")
                print(f"   Runtime: {config['Runtime']}")
                print(f"   Rol: {config['Role'].split('/')[-1]}")
                print()
                
            except Exception as e:
                function_status[func_name] = {
                    'exists': False,
                    'error': str(e)
                }
                print(f"❌ {func_name}")
                print(f"   Error: {str(e)}")
                print()
        
        return function_status
        
    except Exception as e:
        print(f"❌ Error verificando Lambda: {e}")
        return {}

def diagnose_api_issues(table_status, function_status):
    """
    Diagnosticar problemas de API
    """
    print("🔧 DIAGNÓSTICO DE PROBLEMAS")
    print("="*40)
    
    issues = []
    recommendations = []
    
    # Verificar tablas
    empty_tables = [name for name, status in table_status.items() 
                   if status.get('exists') and not status.get('has_data')]
    
    if empty_tables:
        issues.append(f"Tablas DynamoDB vacías: {len(empty_tables)}")
        recommendations.append("Ejecutar: python dynamodb_setup.py")
    
    # Verificar funciones
    inactive_functions = [name for name, status in function_status.items()
                         if status.get('exists') and status.get('state') != 'Active']
    
    if inactive_functions:
        issues.append(f"Lambda Functions inactivas: {len(inactive_functions)}")
        recommendations.append("Ejecutar: python lambda_functions_academy.py")
    
    # Mostrar diagnóstico
    if issues:
        print("❌ PROBLEMAS DETECTADOS:")
        for issue in issues:
            print(f"   • {issue}")
        
        print(f"\n🔧 RECOMENDACIONES:")
        for rec in recommendations:
            print(f"   • {rec}")
    else:
        print("✅ No se detectaron problemas obvios")
        print("💡 El problema puede estar en:")
        print("   • Permisos de LabRole")
        print("   • Variables de entorno")
        print("   • Configuración de API Gateway")
    
    return issues, recommendations

def main():
    """
    Verificación completa del sistema
    """
    print("🔍 VERIFICACIÓN COMPLETA DEL SISTEMA AWS")
    print("="*60)
    print(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Verificar DynamoDB
    table_status = check_dynamodb_tables()
    
    # Verificar Lambda
    function_status = check_lambda_functions()
    
    # Diagnosticar
    issues, recommendations = diagnose_api_issues(table_status, function_status)
    
    print("\n" + "="*60)
    print("📊 RESUMEN FINAL")
    print("="*60)
    
    # Contar recursos exitosos
    working_tables = sum(1 for status in table_status.values() 
                        if status.get('exists') and status.get('has_data'))
    
    working_functions = sum(1 for status in function_status.values()
                           if status.get('exists') and status.get('state') == 'Active')
    
    print(f"📊 Tablas DynamoDB funcionando: {working_tables}/4")
    print(f"🚀 Lambda Functions activas: {working_functions}/3")
    
    if working_tables == 4 and working_functions == 3:
        print(f"\n🎉 TODO ESTÁ FUNCIONANDO!")
        print(f"💡 El problema de API 500 puede ser:")
        print(f"   • Configuración de variables de entorno")
        print(f"   • Formato de requests a las Lambda")
    elif len(recommendations) > 0:
        print(f"\n🔧 SIGUIENTE PASO:")
        print(f"   {recommendations[0]}")
    else:
        print(f"\n❌ Se requiere investigación adicional")

if __name__ == "__main__":
    main()
