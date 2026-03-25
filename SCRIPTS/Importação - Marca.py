import pandas as pd
import requests
import time
import math

BASE_URL = "https://api-v2.gauderp.com/v1"

try:
    from import_data.authentication import authentication
except ImportError:
    def authentication(user, password):
        return "seu_token_v2_aqui"

try:
    token = authentication("cia do mar", "admin")
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    print("Autenticação V2 realizada com sucesso.")
except Exception as e:
    print(f"Erro na autenticação: {e}")
    exit()

errored_brands = []

def clean(value):
    if pd.isna(value) or str(value).strip() == "" or str(value).lower() == "nan":
        return None
    return str(value).strip()

def create_brand(name, reference=None):
    url = f"{BASE_URL}/inventory/brands"

    body = {
        "name": clean(name),
        "reference": clean(reference)
    }

    body = {k: v for k, v in body.items() if v is not None}

    if not body.get("name"):
        return None

    try:
        response = requests.post(url, headers=headers, json=body)
        
        if response.status_code == 201:
            brand_data = response.json()
            print(f"Marca '{body['name']}' - ID: {brand_data['id']}")
            return brand_data
        elif response.status_code == 409:
            print(f"Marca '{body['name']}' já existe.")
            return None
        else:
            print(f"Erro '{body['name']}': {response.status_code} - {response.text}")
            errored_brands.append({"name": name, "error": response.text})
            return None
    except Exception as e:
        print(f"Erro de rede: {e}")
        errored_brands.append({"name": name, "error": str(e)})
        return None

file_path = "Importação_Marca.csv" 

try:
    df = pd.read_csv(file_path, sep=';', dtype=str, encoding='latin1', engine='python')
    print(f"Lendo '{file_path}' com {len(df)} registros.")
except Exception as e:
    print(f"Erro ao ler o arquivo CSV: {e}")
    exit()

for idx, row in df.iterrows():
    name_val = row.get("NomeMarca") or row.get("Nome") or row.get("Marca")
    ref_val = row.get("Referencia") or row.get("Codigo")

    if name_val:
        create_brand(name=name_val, reference=ref_val)
    
    time.sleep(0.1)
    print(f"Progresso: {idx+1}/{len(df)}", end='\r')

print("\n\nProcessamento concluído!")

if errored_brands:
    try:
        pd.DataFrame(errored_brands).to_excel("erros_marcas_v2.xlsx", index=False)
        print(f"Verifique os erros em 'erros_marcas_v2.xlsx'")
    except:
        pass