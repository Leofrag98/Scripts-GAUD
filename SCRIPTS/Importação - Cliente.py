import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import math
import os
import sys

BASE_URL_V2 = "https://api-v2.gauderp.com/v1"
FIXED_PRICE_LIST_ID = 1
FIXED_PRICE_LIST_OBJECT = {"id": FIXED_PRICE_LIST_ID}
ARQUIVO_CSV = 'Importação_Cliente.csv'

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
    url = f"{BASE_URL_V2}/authentication/login"
    payload = {"username": user, "password": password}
    res = session.post(url, json=payload)
    res.raise_for_status()
    data = res.json()
    return data.get("accessToken") or data.get("token")

def remove_non_numbers(text):
    return ''.join(filter(str.isdigit, str(text))) if text else ""

def clean_value(value):
    if pd.isna(value) or str(value).strip() == "":
        return None
    return str(value).strip()

def search_city(session: requests.Session, state, city, cache: dict) -> int | None:
    if not state or not city: return None
    
    cache_key = f"{city.lower()}|{state.lower()}"
    if cache_key in cache: return cache[cache_key]
        
    try:
        params = {
            "name$match": city,
            "state$match": state,
            "page": 0,
            "limit": 1,
            "condition": "AND"
        }
        res = session.get(f"{BASE_URL_V2}/ibge/cities", params=params)
        if res.status_code == 200:
            data = res.json().get("data")
            if data:
                city_id = data[0]["id"]
                cache[cache_key] = city_id
                return city_id
    except:
        pass
    return None

def main():
    user = os.environ.get("GAUD_USER", "leonardo")
    password = os.environ.get("GAUD_PASSWORD", "123456")

    session = criar_sessao()
    
    try:
        token = get_token(session, user, password)
        session.headers["Authorization"] = f"Bearer {token}"
        print("Autenticação realizada com sucesso.")
    except Exception as e:
        sys.exit(f"Falha na autenticação: {e}")

    city_cache = {}
    print(f"Iniciando Importação de Clientes: {ARQUIVO_CSV}")

    try:
        file_reader = pd.read_csv(
            ARQUIVO_CSV, sep=';', dtype=str, encoding='latin1', chunksize=1000
        )

        for chunk in file_reader:
            chunk = chunk.where(pd.notnull(chunk), None)
            
            for _, customer in chunk.iterrows():
                name = clean_value(customer.get("RazaoSocial_NomeCliente"))
                fantasy_name = clean_value(customer.get("NomeFantasia"))
                
                if not name and not fantasy_name:
                    continue

                doc = remove_non_numbers(customer.get("CNPJ_CPF") or "")
                state_uf = clean_value(customer.get("UF"))
                city_name = clean_value(customer.get("Cidade"))
                city_id = search_city(session, state_uf, city_name, city_cache)

                body = {
                    "active": True,
                    "documentNumber": doc,
                    "stateRegistration": remove_non_numbers(customer.get("RG_InscEst") or ""),
                    "name": name or fantasy_name,
                    "fantasyName": fantasy_name or name,
                    "address": {
                        "address": clean_value(customer.get("Endereco")),
                        "number": remove_non_numbers(customer.get("NUMERO") or ""),
                        "neighborhood": clean_value(customer.get("Bairro")),
                        "addressComplement": clean_value(customer.get("pontoreferencia")),
                        "city": {"id": city_id} if city_id else None,
                        "state": state_uf,
                        "zipCode": remove_non_numbers(customer.get("Cep") or ""),
                        "country": "BR"
                    },
                    "type": "INDIVIDUAL" if len(doc) <= 11 else "COMPANY",
                    "contacts": [
                        {
                            "name": clean_value(customer.get("Contato")),
                            "phone": remove_non_numbers(customer.get("Telefone") or ""),
                            "email": clean_value(customer.get('E_Mail'))
                        }
                    ],
                    "reference": clean_value(customer.get("CodigoDoCliente")),
                    "priceList": FIXED_PRICE_LIST_OBJECT 
                }

                if not any(body["contacts"][0].values()):
                    body["contacts"] = []
                
                try:
                    res_c = session.post(f"{BASE_URL_V2}/sales/customers", json=body)
                    if res_c.status_code in [200, 201]:
                        print(f"[OK] Cliente: {body['name']}")
                    else:
                        print(f"[ERRO] Cliente {body['name']}: {res_c.status_code} - {res_p.text}")
                except Exception as e:
                    print(f"[FALHA] Conexão no cliente {body['name']}: {e}")

        print("\nProcessamento concluído.")

    except FileNotFoundError:
        print(f"Arquivo {ARQUIVO_CSV} não encontrado.")
    except Exception as e:
        print(f"Erro geral: {e}")

if __name__ == "__main__":
    main()