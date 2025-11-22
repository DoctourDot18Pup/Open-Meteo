import boto3
import json
from datetime import datetime

def get_current_api_info():
    """
    Obtener información actual del API Gateway
    """
    try:
        # Cliente API Gateway
        api_client = boto3.client('apigateway', region_name='us-east-1')
        
        print("🌐 BUSCANDO API GATEWAY ACTUAL")
        print("="*50)
        
        # Listar APIs REST
        response = api_client.get_rest_apis()
        apis = response.get('items', [])
        
        # Filtrar API de OpenMeteo
        openmeteo_apis = [
            api for api in apis 
            if 'openmeteo' in api.get('name', '').lower()
        ]
        
        if openmeteo_apis:
            api = openmeteo_apis[0]  # Tomar la primera
            api_id = api['id']
            api_name = api['name']
            
            print(f"✅ API encontrada:")
            print(f"   Nombre: {api_name}")
            print(f"   ID: {api_id}")
            print(f"   Creada: {api.get('createdDate', 'Unknown')}")
            
            # Construir URL
            api_url = f"https://{api_id}.execute-api.us-east-1.amazonaws.com/dev"
            
            print(f"\n🔗 URL ACTUAL:")
            print(f"   {api_url}")
            
            # Verificar stage
            try:
                stages = api_client.get_stages(restApiId=api_id)
                stage_names = [stage['stageName'] for stage in stages.get('item', [])]
                print(f"\n📍 Stages disponibles: {stage_names}")
            except:
                print(f"\n📍 Stage por defecto: dev")
            
            # Guardar información
            api_info = {
                'api_id': api_id,
                'api_name': api_name,
                'api_url': api_url,
                'region': 'us-east-1',
                'updated_at': datetime.now().isoformat()
            }
            
            with open('current_api_info.json', 'w') as f:
                json.dump(api_info, f, indent=2)
            
            print(f"\n💾 Info guardada en: current_api_info.json")
            
            return api_info
            
        else:
            print("❌ No se encontró API de OpenMeteo")
            print("\n💡 Opciones:")
            print("   1. Ejecutar: python api_gateway_setup.py")
            print("   2. O usar Function URLs como alternativa")
            
            return None
    
    except Exception as e:
        print(f"❌ Error buscando API: {e}")
        return None

def test_new_url(api_url):
    """
    Probar la nueva URL encontrada
    """
    if not api_url:
        return
    
    import requests
    
    print(f"\n🧪 PROBANDO NUEVA URL")
    print("="*30)
    
    test_endpoints = [
        f"{api_url}/weather",
        f"{api_url}/alerts?limit=3",
        f"{api_url}/metrics?limit=3"
    ]
    
    for endpoint in test_endpoints:
        try:
            print(f"🔗 Probando: {endpoint}")
            response = requests.get(endpoint, timeout=10)
            
            if response.status_code == 200:
                print(f"   ✅ Status: 200 - Funcionando!")
            else:
                print(f"   ⚠️ Status: {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Error: {str(e)[:100]}...")
    
def main():
    """
    Script principal
    """
    print("🔍 DETECTOR DE API GATEWAY ACTUAL")
    print("="*40)
    print(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*40)
    
    # Obtener info actual
    api_info = get_current_api_info()
    
    if api_info:
        # Probar nueva URL
        test_new_url(api_info['api_url'])
        
        print(f"\n🎯 PRÓXIMOS PASOS:")
        print(f"   1. Actualizar api_tester.py con nueva URL:")
        print(f"      {api_info['api_url']}")
        print(f"   2. Ejecutar: python api_tester.py")
        print(f"   3. ¡APIs listas para Flutter!")
    else:
        print(f"\n🔧 RECOMENDACIÓN:")
        print(f"   Ejecutar: python api_gateway_setup.py")
        print(f"   Para crear nuevamente el API Gateway")

if __name__ == "__main__":
    main()

