import pandas as pd
import requests
import json

BASE_URL = "https://api-v2.gauderp.com/v1"

session = requests.Session()

def authentication(user, password):
    url = f"{BASE_URL}/authentication/login"
    payload = {"username": user, "password": password}
    try:
        response = session.post(url, json=payload, timeout=30)
        if response.status_code == 200:
            data = response.json()
            return data.get("accessToken") or data.get("token")
        return None
    except Exception as e:
        print(f"Erro na autenticação: {e}")
        return None

token = authentication("leonardo", "123456")
if not token:
    print("Erro: Não foi possível obter o token na V2.")
    exit()

session.headers.update({
    'Authorization': f'Bearer {token}',
    'Content-Type': 'application/json'
})

errored_groups = []

def clean(value):
    if value is None:
        return None
    value = str(value).strip()
    return value if value and value.lower() != "nan" else None

def create_group(name, code=None, description=None, reference=None, tax=None):
    url = f"{BASE_URL}/inventory/groups"

    body = {
        "name": clean(name),
        "code": clean(code),
        "description": clean(description),
        "reference": clean(reference),
        "tax": {"id": int(tax)} if tax and str(tax).strip() != "" else None
    }

    body = {k: v for k, v in body.items() if v is not None}

    if not body.get("name"):
        return None

    try:
        response = session.post(url, json=body, timeout=30)
        
        if response.status_code in [200, 201]:
            group_data = response.json()
            print(f"Grupo '{body['name']}' criado com sucesso! ID: {group_data.get('id')}")
            return group_data
        elif response.status_code == 409:
            print(f"Grupo '{body['name']}' já existe.")
            return None
        else:
            print(f"Erro ao criar grupo '{body['name']}': {response.status_code} - {response.text}")
            errored_groups.append(body)
            return None
    except requests.exceptions.RequestException as e:
        print(f"Erro de rede: {e}")
        errored_groups.append(body)
        return None

file_path = "Importação_Grupo.csv"

try:
    df = pd.read_csv(file_path, sep=';', dtype=str, encoding='latin1', engine='python')
    df.fillna("", inplace=True)
except Exception as e:
    print(f"Erro ao ler o arquivo CSV: {e}")
    exit()

total_groups = len(df)
print(f"Iniciando V2 - Total de grupos: {total_groups}")

for idx, row in df.iterrows():
    create_group(
        name=row.get("NomeGrupo", ""),
        code=row.get("CodigoGrupo", ""),
        description=row.get("Descricao", ""),
        reference=row.get("Referencia", ""),
        tax=row.get("Imposto", None)
    )
    print(f"Progresso: {idx+1}/{total_groups}", end='\r')

print("\n\nProcessamento V2 concluído!")

if errored_groups:
    try:
        pd.DataFrame(errored_groups).to_excel("errored_groups_v2.xlsx", index=False)
        print("Relatório de erros: errored_groups_v2.xlsx")
    except:
        print("Erro ao gerar Excel de log.")