from datetime import datetime
import time
import pandas as pd
import requests
import math
import os
from import_data.authentication import authentication

BASE_URL = "https://api-v2.gauderp.com/v1"
PAYMENT_METHOD_ID = 159

try:
    token = authentication("Grupo machado", "admin")
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    print("Autenticação V2 realizada com sucesso.")
except Exception as e:
    print(f"Erro na autenticação: {e}")
    exit()

def parse_datetime(dt_str):
    if not dt_str or pd.isna(dt_str):
        return None
    clean_dt_str = str(dt_str).strip()
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%Y-%m-%d', '%d/%m/%y'):
        try:
            return datetime.strptime(clean_dt_str, fmt)
        except:
            continue
    return None

def parse_total(value):
    if not value or pd.isna(value): return 0.0
    try:
        val_str = str(value).replace('.', '').replace(',', '.')
        return float(val_str)
    except: return 0.0

def safe_str(value):
    return str(value).strip() if value and not pd.isna(value) else None

def sanitize_dict(d: dict):
    clean = {}
    for k, v in d.items():
        if isinstance(v, dict): clean[k] = sanitize_dict(v)
        elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)): clean[k] = 0.0
        else: clean[k] = v if not pd.isna(v) else None
    return clean

csv_file_path = 'Importação_Contas_a_Pagar.csv'
customer_cache = {}

def search_provider(provider_id_ref):
    if not provider_id_ref: return None
    ref = str(provider_id_ref).strip()
    if ref in customer_cache: return customer_cache[ref]

    try:
        url = f"{BASE_URL}/inventory/providers/{ref}"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            p_id = response.json().get("id")
            customer_cache[ref] = p_id
            return p_id
        
        url_filter = f"{BASE_URL}/inventory/providers?id$eq={ref}"
        response = requests.get(url_filter, headers=headers)
        if response.status_code == 200:
            data = response.json().get("data", [])
            if data:
                p_id = data[0]["id"]
                customer_cache[ref] = p_id
                return p_id
    except:
        pass
    return None

def create_invoice_payable(params):
    params = sanitize_dict(params)
    url = f"{BASE_URL}/financial/invoices/payable"
    try:
        response = requests.post(url, headers=headers, json=params)
        if response.status_code in (200, 201):
            print(f"Fatura {params.get('number')} - OK")
            return True
        else:
            print(f"Erro Fatura {params.get('number')}: {response.status_code} - {response.text}")
            return False
    except Exception as e:
        print(f"Erro de conexão: {e}")
        return False

not_found = []

try:
    print(f"Lendo '{csv_file_path}'...")
    file_iter = pd.read_csv(csv_file_path, sep=';', dtype=str, encoding='latin1', chunksize=500)
    
    for i, chunk in enumerate(file_iter):
        chunk.columns = chunk.columns.str.strip()
        items = chunk.to_dict(orient='records')
        
        for it in items:
            doc_number = safe_str(it.get("NumeroDocumento"))
            if not doc_number: continue

            issued_at = parse_datetime(it.get("DataDocumento")) or datetime.now()
            due_at = parse_datetime(it.get("Vencimento")) or issued_at
            
            provider_ref = safe_str(it.get("IDFornecedor"))
            provider_id = search_provider(provider_ref)

            if not provider_id:
                not_found.append({
                    "id_fornecedor": provider_ref,
                    "numero_documento": doc_number,
                    "valor": it.get("ValorDocumento")
                })
                continue

            status_raw = safe_str(it.get("Status"))
            final_status = status_raw.upper() if status_raw and status_raw.upper() in ['PAID', 'PENDING', 'CANCELED'] else 'PENDING'

            body = {
                "provider": {"id": int(provider_id)},
                "total": parse_total(it.get("ValorDocumento")),
                "dueDate": due_at.strftime('%Y-%m-%d'),
                "documentIssueDate": issued_at.strftime('%Y-%m-%dT%H:%M:%S'),
                "status": final_status,
                "barcode": safe_str(it.get("CodigoBarras")),
                "number": doc_number,
                "paymentMethod": {"id": PAYMENT_METHOD_ID},
                "description": safe_str(it.get("Mensagem")) or safe_str(it.get("Instrucoes1")) or "Importação Automática"
            }

            create_invoice_payable(body)

except Exception as e:
    print(f"ERRO FATAL: {e}")

if not_found:
    pd.DataFrame(not_found).to_csv("fornecedores_nao_encontrados.csv", sep=";", index=False, encoding="utf-8-sig")
    print(f"\nProcesso concluído com {len(not_found)} erros de fornecedor.")
else:
    print("\nProcesso concluído com sucesso!")