from datetime import datetime
import pandas as pd
import requests
from import_data.authentication import authentication
import time 
import math

BASE_URL = "https://api-v2.gauderp.com/v1"
PAYMENT_METHOD_INVOICE = 124 
PAYMENT_METHOD_DETAIL = 182 

try:
    token = authentication("ponto ford", "admin")
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    print("Autenticação V2 realizada com sucesso.")
except Exception as e:
    print(f"Erro na autenticação: {e}")
    exit()

error_records = []

def parse_date(date_str):
    if not date_str or pd.isna(date_str) or str(date_str).lower() == 'empty':
        return None
    for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f'):
        try:
            return datetime.strptime(str(date_str).split('.')[0], fmt) 
        except ValueError:
            continue
    return None

def safe_float(value):
    if not value or pd.isna(value): return 0.0
    try:
        val = str(value).replace('.', '').replace(',', '.')
        return float(val)
    except: return 0.0

customer_cache = {}

def search_customer(reference_id, retries=3):
    if not reference_id: return None
    
    id_clean = str(reference_id).strip()
    if id_clean in customer_cache:
        return customer_cache[id_clean]

    try:
        url = f"{BASE_URL}/sales/customers/{id_clean}"
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            c_id = response.json().get("id")
            customer_cache[id_clean] = c_id
            return c_id
        
        url_filter = f"{BASE_URL}/sales/customers?id$eq={id_clean}"
        response = requests.get(url_filter, headers=headers)
        if response.status_code == 200:
            data = response.json().get("data", [])
            if data:
                c_id = data[0]["id"]
                customer_cache[id_clean] = c_id
                return c_id
        
        if response.status_code == 503 and retries > 0:
            time.sleep(2)
            return search_customer(reference_id, retries - 1)
            
    except Exception as e:
        if retries > 0:
            time.sleep(2)
            return search_customer(reference_id, retries - 1)
    return None

def create_invoice_receivable(params, retries=3):
    url = f"{BASE_URL}/financial/invoices/receivable"
    try:
        response = requests.post(url, headers=headers, json=params)
        if response.status_code in (200, 201):
            print(f"Fatura {params['number']} - OK!")
            return True
        elif response.status_code == 503 and retries > 0:
            time.sleep(2)
            return create_invoice_receivable(params, retries - 1)
        else:
            print(f"Erro Fatura {params['number']}: {response.status_code} {response.text}")
            return False
    except Exception as e:
        if retries > 0:
            time.sleep(2)
            return create_invoice_receivable(params, retries - 1)
        return False

print("Iniciando Importação Contas a Receber V2...")

try:
    file_iter = pd.read_csv(
        'Importação_Contas_a_Receber.csv',
        sep=';', dtype=str, encoding='latin1', chunksize=500
    )

    for chunk in file_iter:
        chunk.columns = chunk.columns.str.strip()
        chunk = chunk.fillna("")
        items = chunk.to_dict(orient='records')

        for it in items:
            doc_number = it.get("NumeroDocumento")
            if not doc_number: continue

            issued_at = parse_date(it.get("DataDocumento")) or datetime.now()
            due_at = parse_date(it.get("Vencimento")) or issued_at
            payment_at = parse_date(it.get("DataPagamento"))
            
            total_value = safe_float(it.get("ValorDocumento"))
            
            id_cliente_csv = str(it.get("IDCliente", "")).strip()
            customer_id = search_customer(id_cliente_csv)
            
            if not customer_id:
                print(f"Cliente {id_cliente_csv} não encontrado. Fatura {doc_number} ignorada.")
                error_records.append(it)
                continue
                
            status = str(it.get("Status", 'PENDING')).upper()
            if status == 'PAID' and not payment_at:
                print(f"Fatura {doc_number} marcada como PAGA sem data de pagamento.")
                error_records.append(it)
                continue

            body = {
                "customer": {"id": int(customer_id)},
                "total": total_value, 
                "dueDate": due_at.strftime('%Y-%m-%d'),
                "documentIssueDate": issued_at.strftime('%Y-%m-%dT%H:%M:%S'),
                "status": status, 
                "barcode": None,
                "number": str(doc_number),
                "paymentMethod": {"id": int(PAYMENT_METHOD_INVOICE)},
                "description": it.get("Mensagem") or "Importação V2"
            }

            if status == 'PAID':
                body["payments"] = [{
                    "amount": total_value, 
                    "paidAt": payment_at.strftime('%Y-%m-%dT%H:%M:%S'),
                    "paymentMethod": {"id": int(PAYMENT_METHOD_DETAIL)}
                }]

            if not create_invoice_receivable(body):
                error_records.append(it)

except Exception as e:
    print(f"ERRO FATAL NO LOOP: {e}")

if error_records:
    pd.DataFrame(error_records).to_excel('registros_com_erro_receber.xlsx', index=False)
    print(f"{len(error_records)} registros com erro salvos em 'registros_com_erro_receber.xlsx'")
else:
    print("Processamento concluído sem erros!")