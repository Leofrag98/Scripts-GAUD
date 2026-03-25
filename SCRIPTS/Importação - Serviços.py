import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import logging
import os
import sys
import time

# Configurações de Log
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_URL = "https://api-v2.gauderp.com/v1" 
API_URL = f"{BASE_URL}/service/types"
FILE_PATH = "Importação_Servicos.csv"

# Mapeamento de Durações (Padrão API v2)
DURATION_MAP = {
    "30min": "THIRTY_MINUTES", "00:30": "THIRTY_MINUTES",
    "1_HOUR": "ONE_HOUR", "ONE_HOUR": "ONE_HOUR", "60:00": "ONE_HOUR",
    "2_HOUR": "TWO_HOURS", "120:00": "TWO_HOURS",
    "3_HOUR": "THREE_HOURS", "180:00": "THREE_HOURS",
    "4_HOUR": "FOUR_HOURS", "240:00": "FOUR_HOURS",
    "1_DAY": "ONE_DAY", "2_DAY": "TWO_DAYS",
    "3_DAY": "THREE_DAYS", "4_DAY": "FOUR_DAYS",
    "150:00": "TWO_HOURS_HALF" 
}

def criar_sessao(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    })
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

def tratar_duracao(valor_csv):
    valor = str(valor_csv).strip()
    if valor in ["000:00", "00:00", "0:00", ""]:
        return "ONE_HOUR" # Default caso esteja vazio
    
    res = DURATION_MAP.get(valor)
    if not res:
        # Lógica para durações muito longas
        if ":" in valor and int(valor.split(':')[0]) > 96:
            return "FOUR_DAYS"
        return "ONE_HOUR"
    return res

def main():
    # Autenticação
    try:
        from import_data.authentication import authentication
        token = authentication("admin.eletro.ar", "123456")
    except ImportError:
        token = "seu_token_v2_aqui"

    session = criar_sessao(token)
    errored_services = []

    if not os.path.exists(FILE_PATH):
        logging.critical("Arquivo CSV não encontrado!")
        sys.exit()

    try:
        # Leitura otimizada
        df = pd.read_csv(FILE_PATH, sep=';', dtype=str, encoding='latin1', engine='python')
        df = df.where(pd.notnull(df), None)
        logging.info(f"Iniciando importação V2 de {len(df)} serviços.")
    except Exception as e:
        logging.critical(f"Erro ao ler CSV: {e}")
        sys.exit()

    for idx, row in df.iterrows():
        name = (row.get("Nome") or row.get("Serviço") or "").strip()
        if not name: continue

        # Tratamento de Preço
        price_raw = str(row.get("Preco") or row.get("Preço") or "0").strip()
        try:
            price = float(price_raw.replace('.', '').replace(',', '.'))
        except:
            price = 0.0

        payload = {
            "name": name,
            "price": price, 
            "description": (row.get("Observacao") or row.get("Descrição") or "").strip(),
            "duration": tratar_duracao(row.get("Duracao") or row.get("Duração")) 
        }

        # Limpeza de campos vazios
        payload = {k: v for k, v in payload.items() if v is not None and v != ""}

        try:
            res = session.post(API_URL, json=payload)
            if res.status_code in [200, 201]:
                logging.info(f"[OK] {name}")
            elif res.status_code == 409:
                logging.warning(f"[JÁ EXISTE] {name}")
            else:
                logging.error(f"[ERRO] {name}: {res.status_code} - {res.text[:100]}")
                payload['error_detail'] = res.text
                errored_services.append(payload)
        except Exception as e:
            logging.error(f"[FALHA CONEXÃO] {name}: {e}")
            errored_services.append({"name": name, "error_detail": str(e)})

    # Relatório Final
    if errored_services:
        pd.DataFrame(errored_services).to_excel("errored_services_v2.xlsx", index=False)
        logging.info("Processamento finalizado com alguns erros. Veja 'errored_services_v2.xlsx'")
    else:
        logging.info("Processamento concluído com 100% de sucesso!")

if __name__ == "__main__":
    main()