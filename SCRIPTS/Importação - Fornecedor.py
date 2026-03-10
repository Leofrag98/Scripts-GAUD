import pandas as pd
import requests
import math
import time

BASE_URL = "https://api-v2.gauderp.com/v1"

def get_token(user, password):
    url = f"{BASE_URL}/authentication/login"
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
    print("Erro: Falha na autenticação v2.")
    exit()

headers = {
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
}

cities = {}

def remove_non_numbers(text):
    return ''.join(filter(str.isdigit, str(text))) if text else ""

def clean_value(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        v = value.strip()
        if v == "" or v.upper() == "EMPTY":
            return None
        return v
    return value

def search_city(state, city):
    state, city = clean_value(state), clean_value(city)
    if not state or not city:
        return None

    manual_map = {
        'SANTA BARBARA DOESTE': 5300,
        'Sumare': 5370,
        'RONDONOPOLIS': 2391
    }
    if city in manual_map: return manual_map[city]

    cache_key = f"{city}-{state}"
    if cache_key in cities: return cities[cache_key]

    try:
        url = f"{BASE_URL}/ibge/cities?name$match={city}&state$match={state}&page=0&limit=1&condition=AND"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json().get("data")
            if data:
                cities[cache_key] = data[0]["id"]
                return cities[cache_key]
        elif response.status_code == 503:
            time.sleep(1)
            return search_city(state, city)
    except:
        pass
    return None

def create_provider(params):
    url = f"{BASE_URL}/inventory/providers"
    try:
        response = requests.post(url, headers=headers, json=params)
        if response.status_code in [200, 201]:
            print(f"Fornecedor {params['name']} - OK!")
            return True
        elif response.status_code == 503:
            time.sleep(2)
            return create_provider(params)
        else:
            print(f"Erro no fornecedor {params['name']}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Erro de conexão no fornecedor {params['name']}: {e}")
        return False

file_path = 'Importação_Fornecedor.csv'
print(f"Iniciando Importação de Fornecedores v2: {file_path}")

try:
    reader = pd.read_csv(
        file_path, sep=';', dtype=str, encoding='latin1', chunksize=5000, engine='python', on_bad_lines='skip'
    )

    for chunk in reader:
        items = chunk.to_dict(orient='records')
        for it in items:
            jur_fis = clean_value(it.get("Juridica_Fisica"))
            provider_type = "INDIVIDUAL" if jur_fis == "FISICA" else "COMPANY"
            
            cnpj_cpf = remove_non_numbers(it.get("CNPJ_CPF"))
            nome = clean_value(it.get("RazaoSocial_NomeFornecedor"))
            fantasia = clean_value(it.get("NomeFantasia"))
            
            if not nome and not fantasia: continue

            uf = clean_value(it.get("UF"))
            cidade_nome = clean_value(it.get("Cidade"))
            city_id = search_city(uf, cidade_nome)

            body = {
                "active": True,
                "documentNumber": cnpj_cpf,
                "stateRegistration": remove_non_numbers(it.get("RG_InsEstadual")),
                "municipalRegistration": None,
                "name": nome or fantasia,
                "fantasyName": fantasia or nome,
                "address": {
                    "address": clean_value(it.get("Endereco")),
                    "number": clean_value(it.get("Numero")),
                    "neighborhood": clean_value(it.get("Bairro")),
                    "addressComplement": clean_value(it.get("Complemento")),
                    "city": {"id": city_id} if city_id else None,
                    "state": uf,
                    "zipCode": remove_non_numbers(it.get("Cep")),
                    "country": "BR"
                },
                "contacts": [
                    {
                        "name": clean_value(it.get("Contato")),
                        "phone": remove_non_numbers(it.get("Telefone")),
                        "email": clean_value(it.get("E_Mail")),
                    }
                ],
                "observation": clean_value(it.get("Observacoes")),
                "type": provider_type,
                "exemptStateRegistration": False
            }

            if not any(body["contacts"][0].values()):
                body["contacts"] = []

            create_provider(body)

    print("\nProcessamento concluído com sucesso.")

except FileNotFoundError:
    print(f"Erro: Arquivo {file_path} não encontrado.")
except Exception as e:
    print(f"Erro fatal: {e}")