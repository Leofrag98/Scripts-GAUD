import pandas as pd
import requests
import json
import math
import re
import time

# --- Autenticação e Configurações V2 ---
try:
    from import_data.authentication import authentication
except ImportError:
    def authentication(user, password):
        print("AVISO: Usando função de autenticação placeholder.")
        return "seu_token_v2_aqui"

# Configurações V2
token = authentication("Maxx Autopecas", "admin")
headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}
BASE_URL = "https://api-v2.gauderp.com/v1"

errored_workshops = []

# --- Funções Auxiliares ---

def strip(value):
    return str(value).strip() if value and not pd.isna(value) else None

def clean_cnpj(cnpj):
    cnpj_original = strip(cnpj)
    if not cnpj_original: return None
    return re.sub(r'\D', '', cnpj_original)

def search_workshop_by_cnpj(cnpj_original):
    """Busca a oficina na API V2 pelo CNPJ."""
    cnpj_limpo = clean_cnpj(cnpj_original)
    if not cnpj_limpo: return None

    # Endpoint V2: service/workshops
    url = f"{BASE_URL}/service/workshops?documentNumber$eq={cnpj_limpo}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json().get('data', [])
            return data[0] if data else None
        return None
    except Exception as e:
        print(f"Erro de rede ao buscar CNPJ {cnpj_original}: {e}")
        return None

def create_workshop(payload, original_item):
    """Cria uma nova oficina (POST) na V2."""
    url = f"{BASE_URL}/service/workshops"
    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code in (200, 201):
            print(f" -> Oficina {payload.get('documentNumber')} CRIADA.")
            return True
        else:
            print(f" -> Erro ao CRIAR: {response.status_code} - {response.text}")
            original_item['Erro'] = f"POST {response.status_code}: {response.text}"
            errored_workshops.append(original_item)
            return False
    except Exception as e:
        original_item['Erro'] = f"Erro Rede POST: {e}"
        errored_workshops.append(original_item)
        return False

# --- Processamento Principal ---

file_path = 'Importação_Oficina.csv'
print(f"Iniciando Importação V2: {file_path}")

try:
    # Leitura em chunks para performance
    file_chunks = pd.read_csv(file_path, sep=';', dtype=str, encoding='latin1', 
                             chunksize=100, engine='python', on_bad_lines='skip')

    processed_count = 0
    for i, chunk in enumerate(file_chunks):
        print(f"\nProcessando bloco {i+1}...")
        chunk.fillna(value="", inplace=True)
        item_list = chunk.to_dict(orient='records')

        for item in item_list:
            processed_count += 1
            cnpj_csv = strip(item.get("CNPJ"))
            razao_social = strip(item.get("RazaoSocial"))
            nome_fantasia = strip(item.get("NomeFantasia"))

            if not cnpj_csv:
                continue

            # 1. Buscar oficina existente
            existing_data = search_workshop_by_cnpj(cnpj_csv)

            if existing_data:
                # --- CASO 1: ATUALIZAR (PUT) ---
                workshop_id = existing_data['id']
                print(f"[{processed_count}] Atualizando CNPJ {cnpj_csv} [ID: {workshop_id}]")
                
                # Na V2, o PUT exige o objeto completo para não limpar campos
                payload = existing_data.copy()
                payload['name'] = razao_social
                payload['fantasyName'] = nome_fantasia
                
                # Removemos campos de auditoria que a API não aceita no envio
                for key in ['createdAt', 'updatedAt', 'id']:
                    payload.pop(key, None)
                
                url_put = f"{BASE_URL}/service/workshops/{workshop_id}"
                
                try:
                    res = requests.put(url_put, headers=headers, json=payload)
                    if res.status_code not in (200, 204):
                        item['Erro'] = f"PUT {res.status_code}: {res.text}"
                        errored_workshops.append(item)
                except Exception as e:
                    item['Erro'] = f"Rede PUT: {e}"
                    errored_workshops.append(item)
            
            else:
                # --- CASO 2: CRIAR (POST) ---
                print(f"[{processed_count}] Criando CNPJ {cnpj_csv}")
                payload_create = {
                    "documentNumber": clean_cnpj(cnpj_csv),
                    "name": razao_social,
                    "fantasyName": nome_fantasia,
                    "main": True,
                    "exemptStateRegistration": True
                }
                create_workshop(payload_create, item)
            
            # Pequena pausa para estabilidade da API
            time.sleep(0.1)

except Exception as e:
    print(f"❌ Erro fatal: {e}")

# --- Finalização ---
if errored_workshops:
    pd.DataFrame(errored_workshops).to_excel("errored_workshops_v2.xlsx", index=False)
    print(f"\n✅ Concluído com avisos. Erros em: errored_workshops_v2.xlsx")
else:
    print("\n✅ Processamento concluído com 100% de sucesso.")