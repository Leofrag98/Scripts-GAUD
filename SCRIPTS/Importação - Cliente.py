import pandas as pd
import requests
import time
import math

BASE_URL_V2 = "https://api-v2.gauderp.com/v1"
FIXED_PRICE_LIST_ID = 311
FIXED_PRICE_LIST_OBJECT = {"id": FIXED_PRICE_LIST_ID} 

def get_token(user, password):
    url = f"{BASE_URL_V2}/authentication/login"
    payload = {"username": user, "password": password}
    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            return response.json().get("accessToken") or response.json().get("token")
        return None
    except Exception as e:
        print(f"Erro na conexão de login: {e}")
        return None

token = get_token("ekballo", "admin")
if not token:
    print("Falha na autenticação. Verifique usuário e senha.")
    exit()

headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

cities = {}

def remove_non_numbers(text):
    return ''.join(filter(str.isdigit, str(text))) if text else ""

def clean_value(value):
    if pd.isna(value) or str(value).strip() == "" or (isinstance(value, float) and math.isnan(value)):
        return None
    return str(value).strip()

def search_city(state, city):
    if not state or not city: return None
    
    cache_key = f"{city.lower()}|{state.lower()}"
    if cache_key in cities: return cities[cache_key]
        
    try:
        url = f"{BASE_URL_V2}/ibge/cities?name$match={city}&state$match={state}&page=0&limit=1&condition=AND"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json().get("data")
            if data:
                city_id = data[0]["id"]
                cities[cache_key] = city_id
                return city_id
        elif response.status_code == 503:
            time.sleep(1)
            return search_city(state, city)
    except:
        pass
    return None

def create_customer(params):
    url = f"{BASE_URL_V2}/sales/customers"
    try:
        response = requests.post(url, headers=headers, json=params)
        
        if response.status_code in [200, 201]:
            print(f"Cliente {params.get('name')} OK!")
            return True
        elif response.status_code == 503:
            time.sleep(1)
            return create_customer(params)
        else:
            print(f"Erro no cliente {params.get('name')}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Erro de conexão: {e}")
        return False

file_path = 'Importação_Cliente.csv'
print(f"Iniciando Importação de Clientes v2: {file_path}")

try:
    file_reader = pd.read_csv(
        file_path, sep=';', dtype=str, encoding='latin1', chunksize=5000, engine='python', on_bad_lines='skip'
    )

    for chunk in file_reader:
        customers = chunk.to_dict(orient='records')
        for customer in customers:
            name = clean_value(customer.get("RazaoSocial_NomeCliente"))
            fantasy_name = clean_value(customer.get("NomeFantasia"))
            
            if not name and not fantasy_name:
                continue

            doc = remove_non_numbers(customer.get("CNPJ_CPF") or "")
            state_uf = clean_value(customer.get("UF"))
            city_name = clean_value(customer.get("Cidade"))

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
                    "city": {"id": search_city(state_uf, city_name)} if state_uf and city_name else None,
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
                
            create_customer(body)

    print("\nProcessamento concluído.")

except FileNotFoundError:
    print(f"Arquivo {file_path} não encontrado.")
except Exception as e:
    print(f"Erro geral: {e}")