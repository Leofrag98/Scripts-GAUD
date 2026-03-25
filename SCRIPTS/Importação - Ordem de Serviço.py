import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import json
import sys
import os

# Configurações de URL
BASE_URL = "https://api-v2.gauderp.com/v1"
URL_MAINTENANCE = f"{BASE_URL}/service/maintenances"
URL_VEHICLES = f"{BASE_URL}/fleet/vehicles"
FILE_PATH = 'Importação_OS.csv'

def criar_sessao(token: str = "") -> requests.Session:
    session = requests.Session()
    if token:
        session.headers.update({'Authorization': f'Bearer {token}'})
    session.headers.update({'Content-Type': 'application/json'})
    
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

def clean_num(v):
    if pd.isna(v) or str(v).strip().lower() in ["nan", ""]: return 0.0
    try: 
        return float(str(v).replace(',', '.'))
    except: 
        return 0.0

def clean_int(v):
    if pd.isna(v) or str(v).strip().lower() in ["nan", ""]: return None
    try: 
        return int(float(str(v).replace(',', '.')))
    except: 
        return None

def get_v2_id_by_plate(session, plate, vehicle_cache):
    if pd.isna(plate) or str(plate).strip() == "": return None
    clean_plate = "".join(filter(str.isalnum, str(plate))).upper()
    
    if clean_plate in vehicle_cache: 
        return vehicle_cache[clean_plate]
    
    params = {
        "sort.attribute": "id",
        "sort.order": "DESC",
        "identification$match": clean_plate,
        "page": 0, "limit": 1, "condition": "AND"
    }
    try:
        res = session.get(URL_VEHICLES, params=params, timeout=10)
        data = res.json().get('data', [])
        if data:
            v_id = data[0]['id']
            vehicle_cache[clean_plate] = v_id
            return v_id
    except: 
        pass
    return None

def main():
    session = criar_sessao()
    token = get_token(session, "admin.eletro.ar", "123456")
    
    if not token: 
        sys.exit("Erro Crítico: Não foi possível obter o Token.")
    
    session.headers.update({'Authorization': f'Bearer {token}'})
    vehicle_cache = {}

    try:
        df = pd.read_csv(FILE_PATH, sep=';', dtype=str, encoding='latin1')
        df.columns = df.columns.str.strip()
        grouped = df.groupby('NumeroOS')

        print(f"Iniciando processamento de {len(grouped)} Ordens de Serviço...\n")

        for os_ref, group in grouped:
            header = group.iloc[0]
            v_id = get_v2_id_by_plate(session, header.get('idveiculo'), vehicle_cache)
            
            prods, servs = [], []
            total_p, total_s = 0.0, 0.0

            for _, row in group.iterrows():
                # Processamento de Produtos
                p_id = clean_int(row.get('IdProduto'))
                if p_id:
                    qty = clean_num(row.get('QtdProduto')) or 1.0
                    prc = clean_num(row.get('PrecoProduto'))
                    prods.append({
                        "product": {"id": p_id}, 
                        "quantity": qty, 
                        "price": prc, 
                        "total": round(qty * prc, 2)
                    })
                    total_p += (qty * prc)

                # Processamento de Serviços
                s_id = clean_int(row.get('IdServico'))
                if s_id:
                    s_prc = clean_num(row.get('PrecoServico'))
                    servs.append({
                        "service": {"id": s_id}, 
                        "duration": "ONE_HOUR", 
                        "price": s_prc, 
                        "box": {"id": 3}
                    })
                    total_s += s_prc

            # Montagem do Payload
            payload = {
                "customer": {"id": clean_int(header.get('IdCliente'))},
                "status": {"id": clean_int(header.get('IdStatus')) or 218},
                "observation": str(header.get('Observacao')) if not pd.isna(header.get('Observacao')) else None,
                "products": prods,
                "services": servs,
                "total": round(total_p + total_s, 2),
                "reference": str(os_ref)  # <-- O NumeroOS vai aqui
            }

            emp_id = clean_int(header.get('IdVendedor'))
            if emp_id: payload["employee"] = {"id": emp_id}

            if v_id: payload["vehicles"] = [{"id": int(v_id)}] 

            f_pagto = clean_int(header.get('IdFormaPagamento'))
            if total_p > 0:
                payload["productPayments"] = [{"paymentMethod": {"id": f_pagto}, "value": round(total_p, 2)}]
            if total_s > 0:
                payload["servicePayments"] = [{"paymentMethod": {"id": f_pagto}, "value": round(total_s, 2)}]

            payload = {k: v for k, v in payload.items() if v is not None}

            # Envio
            try:
                res = session.post(URL_MAINTENANCE, json=payload, timeout=20)
                if res.status_code in [200, 201]:
                    print(f"[OK] OS {os_ref} - Enviada.")
                else:
                    print(f"[ERRO] OS {os_ref}: Status {res.status_code} - {res.text}")
                    with open(f'erro_os_{os_ref}.json', 'w', encoding='utf-8') as f:
                        json.dump(payload, f, indent=4, ensure_ascii=False)
            except Exception as e:
                print(f"[FALHA REDE] OS {os_ref}: {e}")

    except Exception as e:
        print(f"ERRO CRÍTICO NO SCRIPT: {e}")

    print("\nProcesso finalizado.")

if __name__ == "__main__":
    main()