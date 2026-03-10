import pandas as pd
import requests
import logging
import os
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://api-v2.gauderp.com/v1" 
ENDPOINT = "/service/types"
API_URL = f"{BASE_URL}{ENDPOINT}"

try:
    from import_data.authentication import authentication
except ImportError:
    def authentication(user, password):
        return "seu_token_v2_aqui"

TOKEN = authentication("Grupo machado", "admin")
HEADERS = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}

DURATION_MAP = {
    "30min": "THIRTY_MINUTES",
    "00:30": "THIRTY_MINUTES",
    "1_HOUR": "ONE_HOUR",
    "ONE_HOUR": "ONE_HOUR",
    "60:00": "ONE_HOUR",
    "2_HOUR": "TWO_HOURS",
    "120:00": "TWO_HOURS",
    "3_HOUR": "THREE_HOURS",
    "180:00": "THREE_HOURS",
    "4_HOUR": "FOUR_HOURS",
    "240:00": "FOUR_HOURS",
    "1_DAY": "ONE_DAY",
    "2_DAY": "TWO_DAYS",
    "3_DAY": "THREE_DAYS",
    "4_DAY": "FOUR_DAYS",
    "150:00": "TWO_HOURS_HALF" 
}

errored_services = []

def clean_payload(payload):
    if isinstance(payload, dict):
        return {k: clean_payload(v) for k, v in payload.items() if v is not None and v != ""}
    return payload

def tratar_duracao(valor_csv):
    valor = str(valor_csv).strip()
    if valor in ["000:00", "00:00", "0:00", ""]:
        return None
    
    res = DURATION_MAP.get(valor)
    if not res:
        if "600:00" in valor or ":" in valor and int(valor.split(':')[0]) > 96:
            return "FOUR_DAYS"
        return None
    return res

def create_service(service_payload):
    body = clean_payload(service_payload.copy())
    
    try:
        response = requests.post(API_URL, headers=HEADERS, json=body)
        if response.status_code in [200, 201]:
            logging.info(f"Criado: {body['name']} | Preço: {body.get('price')}")
            return response.json()
        elif response.status_code == 409:
            logging.warning(f"Serviço '{body['name']}' já existe.")
            return None
        else:
            logging.error(f"Erro {response.status_code} em {body['name']}: {response.text}")
            body['error_detail'] = response.text
            errored_services.append(body)
    except Exception as e:
        logging.error(f"Erro de conexão: {e}")

file_path = "Importação_Servicos.csv"

if not os.path.exists(file_path):
    logging.critical("Arquivo CSV não encontrado!")
    exit()

try:
    df = pd.read_csv(file_path, sep=';', dtype=str, encoding='latin1', engine='python')
    df.fillna("", inplace=True)
    logging.info(f"Iniciando importação V2 de {len(df)} serviços.")
except Exception as e:
    logging.critical(f"Erro ao ler CSV: {e}")
    exit()

for idx, row in df.iterrows():
    name = row.get("Nome", row.get("Serviço", "")).strip()
    if not name: continue

    raw_duration = row.get("Duracao", row.get("Duração", "")).strip()
    
    price_str = row.get("Preco", row.get("Preço", "0")).strip()
    try:
        price = float(price_str.replace('.', '').replace(',', '.'))
    except:
        price = 0.0

    service_payload = {
        "name": name,
        "price": price, 
        "description": row.get("Observacao", row.get("Descrição", "")).strip(),
        "duration": tratar_duracao(raw_duration) 
    }

    create_service(service_payload)
    time.sleep(0.1)

if errored_services:
    pd.DataFrame(errored_services).to_excel("errored_services_v2.xlsx", index=False)
    logging.info("Verifique os erros em 'errored_services_v2.xlsx'")
else:
    logging.info("Processamento concluído com sucesso!")