import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import math
import os
import sys
from datetime import datetime
from import_data.authentication import authentication

# Configurações Globais
BASE_URL = "https://api-v2.gauderp.com/v1"
PAYMENT_METHOD_INVOICE = 622 
PAYMENT_METHOD_DETAIL = 622 
ARQUIVO_CSV = 'Importação_Contas_a_Receber.csv'

def criar_sessao(token: str) -> requests.Session:
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

def parse_date(date_str):
    if not date_str or pd.isna(date_str) or str(date_str).lower() == 'empty':
        return None
    clean_dt_str = str(date_str).strip()
    # Tenta vários formatos comuns de data
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y', '%Y-%m-%d', '%d/%m/%y'):
        try:
            return datetime.strptime(clean_dt_str.split('.')[0], fmt) 
        except:
            continue
    return None

def parse_total(value):
    if not value or pd.isna(value): return 0.0
    try:
        # Remove pontos de milhar e troca vírgula por ponto decimal
        val_str = str(value).replace('.', '').replace(',', '.')
        return float(val_str)
    except: return 0.0

def search_customer(session: requests.Session, reference_id, cache: dict) -> int | None:
    if not reference_id: return None
    ref = str(reference_id).strip()
    
    if ref in cache: return cache[ref]

    try:
        # Tenta busca direta por ID ou por filtro de referência
        res = session.get(f"{BASE_URL}/sales/customers/{ref}")
        if res.status_code == 200:
            c_id = res.json().get("id")
            cache[ref] = c_id
            return c_id
        
        res_filter = session.get(f"{BASE_URL}/sales/customers", params={"id$eq": ref})
        if res_filter.status_code == 200:
            data = res_filter.json().get("data", [])
            if data:
                c_id = data[0]["id"]
                cache[ref] = c_id
                return c_id
    except:
        pass
    return None

def main():
    try:
        # Autenticação conforme sua estrutura
        token = authentication("cia do mar", "admin")
        session = criar_sessao(token)
        print("Autenticação V2 realizada com sucesso.")
    except Exception as e:
        sys.exit(f"Erro na autenticação: {e}")

    customer_cache = {}
    error_records = []

    print(f"Iniciando Importação Contas a Receber V2: {ARQUIVO_CSV}")

    try:
        file_iter = pd.read_csv(
            ARQUIVO_CSV, sep=';', dtype=str, encoding='latin1', chunksize=500
        )

        for chunk in file_iter:
            chunk.columns = chunk.columns.str.strip()
            chunk = chunk.where(pd.notnull(chunk), None)
            
            for _, it in chunk.iterrows():
                doc_number = str(it.get("NumeroDocumento") or "").strip()
                if not doc_number: continue

                issued_at = parse_date(it.get("DataDocumento")) or datetime.now()
                due_at = parse_date(it.get("Vencimento")) or issued_at
                payment_at = parse_date(it.get("DataPagamento"))
                
                total_value = parse_total(it.get("ValorDocumento"))
                # Pega o valor pago (se houver coluna específica) ou usa o valor total se status for PAID
                paid_value = parse_total(it.get("ValorPago")) if it.get("ValorPago") else total_value
                
                id_cliente_csv = str(it.get("IDCliente") or "").strip()
                customer_id = search_customer(session, id_cliente_csv, customer_cache)
                
                if not customer_id:
                    print(f"[AVISO] Cliente {id_cliente_csv} não encontrado. Fatura {doc_number} ignorada.")
                    error_records.append(it.to_dict())
                    continue
                    
                status = str(it.get("Status") or 'PENDING').upper()
                
                # Validação de segurança para títulos pagos
                if status == 'PAID' and not payment_at:
                    print(f"[AVISO] Fatura {doc_number} marcada como PAGA mas sem Data de Pagamento. Ignorada.")
                    error_records.append(it.to_dict())
                    continue

                body = {
                    "customer": {"id": int(customer_id)},
                    "total": total_value, 
                    "dueDate": due_at.strftime('%Y-%m-%d'),
                    "documentIssueDate": issued_at.strftime('%Y-%m-%dT%H:%M:%S'),
                    "status": status, 
                    "number": doc_number,
                    "paymentMethod": {"id": int(PAYMENT_METHOD_INVOICE)},
                    "description": it.get("Mensagem") or it.get("Observacao") or "Importação Automática"
                }

                # Se estiver pago, adiciona o bloco de liquidação igual ao Contas a Pagar
                if status == 'PAID':
                    body["payments"] = [{
                        "amount": paid_value, 
                        "paidAt": payment_at.strftime('%Y-%m-%dT%H:%M:%S'),
                        "paymentMethod": {"id": int(PAYMENT_METHOD_DETAIL)}
                    }]

                try:
                    res = session.post(f"{BASE_URL}/financial/invoices/receivable", json=body)
                    if res.status_code in (200, 201):
                        print(f"[OK] Fatura {body['number']}")
                    else:
                        print(f"[ERRO] Fatura {body['number']}: {res.status_code} - {res.text}")
                        error_records.append(it.to_dict())
                except Exception as e:
                    print(f"[FALHA] Conexão na fatura {body['number']}: {e}")
                    error_records.append(it.to_dict())

    except Exception as e:
        print(f"ERRO FATAL NO PROCESSAMENTO: {e}")

    if error_records:
        pd.DataFrame(error_records).to_excel('registros_com_erro_receber.xlsx', index=False)
        print(f"\n{len(error_records)} registros com erro salvos em 'registros_com_erro_receber.xlsx'")
    else:
        print("\nProcessamento concluído com sucesso!")

if __name__ == "__main__":
    main()