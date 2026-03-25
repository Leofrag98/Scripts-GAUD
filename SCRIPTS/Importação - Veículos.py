import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import os
import sys
import time

# Configurações
BASE_URL = "https://api-v2.gauderp.com/v1"
VEHICLE_URL = f"{BASE_URL}/fleet/vehicles"
FILE_PATH = 'Importação_Veículos.csv'
SEP = ';'

def criar_sessao(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    })
    # Estratégia de reentrada para evitar quedas em importações longas
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

def get_token(session, user, password):
    url = f"{BASE_URL}/authentication/login"
    try:
        res = session.post(url, json={"username": user, "password": password}, timeout=15)
        res.raise_for_status()
        return res.json().get("accessToken") or res.json().get("token")
    except Exception as e:
        print(f"Erro ao obter token: {e}")
        return None

def clean_str(value):
    if pd.isna(value) or str(value).strip() == '':
        return None
    return str(value).strip()

def to_float_or_zero(value):
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

def main():
    session = criar_sessao("") # Sessão inicial sem token
    token = get_token(session, "ekballo", "admin")
    
    if not token:
        sys.exit("Erro: Falha na autenticação.")

    # Atualiza a sessão com o token obtido
    session.headers.update({'Authorization': f'Bearer {token}'})
    
    errored_items = []
    print(f"Iniciando Importação de Veículos: {FILE_PATH}")

    try:
        # Leitura e padronização das colunas
        df = pd.read_csv(FILE_PATH, sep=SEP, dtype=str, encoding='latin1')
        df.columns = df.columns.str.strip().str.lower()
        df = df.where(pd.notnull(df), None) # Troca NaNs por None globalmente
        
        total = len(df)

        for index, row in df.iterrows():
            placa = clean_str(row.get('placa'))
            customer_id = to_int_or_none(row.get('idcliente'))

            if not placa or not customer_id:
                msg = "Placa ou ID Cliente ausente"
                print(f"\n[PULADO] Linha {index+1}: {msg}")
                errored_items.append({"linha": index+1, "placa": placa, "erro": msg})
                continue

            payload = {
                "identification": placa,
                "mileage": to_float_or_zero(row.get('km')),
                "oilChangeMileage": to_float_or_zero(row.get('km_troca_oleo')),
                "oilChangeAlert": True,
                "renavam": clean_str(row.get('renavam')),
                "chassis": clean_str(row.get('chassi')),
                "yearOfManufacture": to_int_or_none(row.get('ano_fabricacao')),
                "modelYear": to_int_or_none(row.get('ano_modelo')),
                "maker": clean_str(row.get('montadora')), 
                "model": clean_str(row.get('modelo')),    
                "customer": {"id": customer_id},
                "version": clean_str(row.get('versao')),
                "observation": clean_str(row.get('observacao'))
            }

            # Limpa chaves nulas para enviar apenas dados preenchidos
            payload = {k: v for k, v in payload.items() if v is not None}

            try:
                res = session.post(VEHICLE_URL, json=payload)
                if res.status_code in [200, 201]:
                    print(f"Progresso: {index+1}/{total} - Placa {placa}: OK", end='\r')
                else:
                    msg = f"Erro {res.status_code}: {res.text[:100]}"
                    print(f"\n[ERRO] Linha {index+1} | Placa {placa}: {msg}")
                    errored_items.append({"linha": index+1, "placa": placa, "erro": msg})
            except Exception as e:
                print(f"\n[FALHA REDE] Linha {index+1}: {e}")
                errored_items.append({"linha": index+1, "placa": placa, "erro": str(e)})

    except FileNotFoundError:
        print(f"Erro: Arquivo {FILE_PATH} não encontrado.")
    except Exception as e:
        print(f"Erro crítico: {e}")

    # Relatório Final
    if errored_items:
        pd.DataFrame(errored_items).to_excel("erros_v2_veiculos.xlsx", index=False)
        print(f"\n\nConcluído com {len(errored_items)} falhas. Detalhes em 'erros_v2_veiculos.xlsx'.")
    else:
        print("\n\nImportação finalizada com 100% de sucesso!")

if __name__ == "__main__":
    main()