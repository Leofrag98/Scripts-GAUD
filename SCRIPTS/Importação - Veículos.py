import pandas as pd
import requests
import json
import time

FILE_PATH = 'Importação_Veículos.csv' 
SEP = ';' 
BASE_URL = "https://api-v2.gauderp.com/v1/fleet/vehicles"

def get_token(user, password):
    url = "https://api-v2.gauderp.com/v1/authentication/login"
    payload = {"username": user, "password": password}
    try:
        res = requests.post(url, json=payload, timeout=15)
        return res.json().get("accessToken") or res.json().get("token")
    except: return None

token = get_token("ekballo", "admin")
if not token:
    print("Erro: Não foi possível obter o token de autenticação.")
    exit()

headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

errored_items = []

def clean_str(value):
    if pd.isna(value) or str(value).strip() == '':
        return None
    return str(value).strip()

def to_float_or_none(value):
    if pd.isna(value) or str(value).strip() == '':
        return 0.0
    try:
        return float(str(value).replace(',', '.'))
    except:
        return 0.0

def to_int_or_none(value):
    if pd.isna(value) or str(value).strip() == '':
        return None
    try:
        return int(float(str(value).replace(',', '.')))
    except:
        return None

def create_vehicle(row):
    placa = clean_str(row.get('placa'))
    customer_id = to_int_or_none(row.get('idcliente'))

    if not placa:
        return False, "Dados obrigatórios faltando (Placa)"
    
    if not customer_id:
        return False, f"ID Cliente não encontrado para a placa {placa}"

    payload = {
        "identification": placa,
        "mileage": to_float_or_none(row.get('km')),
        "oilChangeMileage": to_float_or_none(row.get('km_troca_oleo')),
        "oilChangeAlert": True,
        "renavam": clean_str(row.get('renavam')),
        "chassis": clean_str(row.get('chassi')),
        "yearOfManufacture": to_int_or_none(row.get('ano_fabricacao')),
        "modelYear": to_int_or_none(row.get('ano_modelo')),
        "maker": clean_str(row.get('montadora')), 
        "model": clean_str(row.get('modelo')),    
        "customer": {
            "id": customer_id
        },
        "version": clean_str(row.get('versao')),
        "observation": clean_str(row.get('observacao'))
    }

    try:
        payload = {k: v for k, v in payload.items() if v is not None}
        response = requests.post(BASE_URL, headers=headers, json=payload, timeout=20)
        
        if response.status_code in [200, 201]:
            return True, f"Criado ID: {response.json().get('id')}"
        else:
            return False, f"Status {response.status_code}: {response.text}"
    except Exception as e:
        return False, f"Erro: {str(e)}"

print(f"Iniciando Importação de Veículos V2: {FILE_PATH}")

processed_count = 0
try:
    df = pd.read_csv(FILE_PATH, sep=SEP, dtype=str, encoding='latin1')
    df.columns = df.columns.str.strip().str.lower()

    for index, item in df.iterrows():
        success, msg = create_vehicle(item)
        processed_count += 1
        
        if not success:
            print(f"\n[ERRO] Linha {index+1} | Placa {item.get('placa')}: {msg}")
            errored_items.append({"linha": index+1, "placa": item.get('placa'), "erro": msg})
        else:
            print(f"Progresso: {processed_count}/{len(df)} - {msg}", end='\r')
            
        time.sleep(0.05)
except Exception as e:
    print(f"\nERRO CRÍTICO NA LEITURA: {e}")

if errored_items:
    pd.DataFrame(errored_items).to_excel("erros_v2_veiculos.xlsx", index=False)
    print(f"\nConcluído com {len(errored_items)} falhas. Verifique 'erros_v2_veiculos.xlsx'.")
else:
    print("\n\nSucesso total na importação dos veículos!")