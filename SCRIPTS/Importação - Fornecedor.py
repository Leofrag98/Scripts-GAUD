import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import math
import os
import sys

BASE_URL = "https://api-v2.gauderp.com/v1"
ARQUIVO_CSV = 'Importação_Fornecedor.csv'

def criar_sessao(token: str = "") -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

def get_token(session: requests.Session, user, password) -> str:
    url = f"{BASE_URL}/authentication/login"
    payload = {"username": user, "password": password}
    res = session.post(url, json=payload)
    res.raise_for_status()
    data = res.json()
    return data.get("accessToken") or data.get("token")

def remove_non_numbers(text):
    return ''.join(filter(str.isdigit, str(text))) if text else ""

def clean_value(value):
    if value is None: return None
    if isinstance(value, float) and math.isnan(value): return None
    if isinstance(value, str):
        v = value.strip()
        return None if v == "" or v.upper() == "EMPTY" else v
    return value

def search_city(session: requests.Session, state, city, cache: dict) -> int | None:
    state, city = clean_value(state), clean_value(city)
    if not state or not city: return None

    manual_map = {'SANTA BARBARA DOESTE': 5300, 'Sumare': 5370, 'RONDONOPOLIS': 2391}
    if city in manual_map: return manual_map[city]

    cache_key = f"{city}-{state}".upper()
    if cache_key in cache: return cache[cache_key]

    try:
        params = {
            "name$match": city,
            "state$match": state,
            "page": 0,
            "limit": 1,
            "condition": "AND"
        }
        res = session.get(f"{BASE_URL}/ibge/cities", params=params)
        if res.status_code == 200:
            data = res.json().get("data")
            if data:
                city_id = data[0]["id"]
                cache[cache_key] = city_id
                return city_id
    except Exception:
        pass
    return None

def main():
    user = os.environ.get("GAUD_USER", "admin.eletro.ar")
    password = os.environ.get("GAUD_PASSWORD", "123456")

    session = criar_sessao()
    
    try:
        token = get_token(session, user, password)
        session.headers["Authorization"] = f"Bearer {token}"
        print("Autenticação realizada com sucesso.")
    except Exception as e:
        sys.exit(f"Erro na autenticação: {e}")

    city_cache = {}
    print(f"Iniciando Importação: {ARQUIVO_CSV}")

    try:
        reader = pd.read_csv(
            ARQUIVO_CSV, sep=';', dtype=str, encoding='latin1', chunksize=1000
        )

        for chunk in reader:
            chunk = chunk.where(pd.notnull(chunk), None)
            
            for _, row in chunk.iterrows():
                nome = clean_value(row.get("RazaoSocial_NomeFornecedor"))
                fantasia = clean_value(row.get("NomeFantasia"))
                
                if not nome and not fantasia:
                    continue

                uf = clean_value(row.get("UF"))
                cidade_nome = clean_value(row.get("Cidade"))
                city_id = search_city(session, uf, cidade_nome, city_cache)

                jur_fis = clean_value(row.get("Juridica_Fisica"))
                
                body = {
                    "active": True,
                    "documentNumber": remove_non_numbers(row.get("CNPJ_CPF")),
                    "stateRegistration": remove_non_numbers(row.get("RG_InsEstadual")),
                    "name": nome or fantasia,
                    "fantasyName": fantasia or nome,
                    "type": "INDIVIDUAL" if jur_fis == "FISICA" else "COMPANY",
                    "address": {
                        "address": clean_value(row.get("Endereco")),
                        "number": clean_value(row.get("Numero")),
                        "neighborhood": clean_value(row.get("Bairro")),
                        "addressComplement": clean_value(row.get("Complemento")),
                        "city": {"id": city_id} if city_id else None,
                        "state": uf,
                        "zipCode": remove_non_numbers(row.get("Cep")),
                        "country": "BR"
                    },
                    "contacts": [
                        {
                            "name": clean_value(row.get("Contato")),
                            "phone": remove_non_numbers(row.get("Telefone")),
                            "email": clean_value(row.get("E_Mail")),
                        }
                    ],
                    "observation": clean_value(row.get("Observacoes")),
                    "exemptStateRegistration": False
                }

                if not any(body["contacts"][0].values()):
                    body["contacts"] = []

                try:
                    res_p = session.post(f"{BASE_URL}/inventory/providers", json=body)
                    if res_p.status_code in [200, 201]:
                        print(f"[OK] {body['name']}")
                    else:
                        print(f"[ERRO] {body['name']}: {res_p.status_code} - {res_p.text}")
                except Exception as e:
                    print(f"[FALHA DE REDE] {body['name']}: {e}")

        print("\nProcessamento concluído.")

    except FileNotFoundError:
        print(f"Erro: O arquivo {ARQUIVO_CSV} não foi encontrado.")
    except Exception as e:
        print(f"Erro fatal: {e}")

if __name__ == "__main__":
    main()