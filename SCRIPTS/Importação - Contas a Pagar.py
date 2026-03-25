import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import math
import os
import sys
from datetime import datetime
from import_data.authentication import authentication

BASE_URL = "https://api-v2.gauderp.com/v1"
PAYMENT_METHOD_ID = 674
ARQUIVO_CSV = 'Importação_Contas_a_Pagar.csv'

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

def search_provider(session: requests.Session, provider_ref, cache: dict) -> int | None:
    if not provider_ref: return None
    ref = str(provider_ref).strip()
    
    if ref in cache: return cache[ref]

    try:
        res = session.get(f"{BASE_URL}/inventory/providers/{ref}")
        if res.status_code == 200:
            p_id = res.json().get("id")
            cache[ref] = p_id
            return p_id
        
        res_filter = session.get(f"{BASE_URL}/inventory/providers", params={"id$eq": ref})
        if res_filter.status_code == 200:
            data = res_filter.json().get("data", [])
            if data:
                p_id = data[0]["id"]
                cache[ref] = p_id
                return p_id
    except:
        pass
    return None

def main():
    try:
        token = authentication("admin.eletro.ar", "123456")
        session = criar_sessao(token)
        print("Autenticação V2 realizada com sucesso.")
    except Exception as e:
        sys.exit(f"Erro na autenticação: {e}")

    provider_cache = {}
    not_found = []

    try:
        print(f"Lendo '{ARQUIVO_CSV}'...")
        file_iter = pd.read_csv(ARQUIVO_CSV, sep=';', dtype=str, encoding='latin1', chunksize=500)
        
        for chunk in file_iter:
            chunk.columns = chunk.columns.str.strip()
            chunk = chunk.where(pd.notnull(chunk), None)
            
            for _, it in chunk.iterrows():
                doc_number = safe_str(it.get("NumeroDocumento"))
                if not doc_number: continue

                issued_at = parse_datetime(it.get("DataDocumento")) or datetime.now()
                due_at = parse_datetime(it.get("Vencimento")) or issued_at
                
                provider_ref = safe_str(it.get("IDFornecedor"))
                provider_id = search_provider(session, provider_ref, provider_cache)

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

                try:
                    res = session.post(f"{BASE_URL}/financial/invoices/payable", json=body)
                    if res.status_code in (200, 201):
                        print(f"[OK] Fatura {body['number']}")
                    else:
                        print(f"[ERRO] Fatura {body['number']}: {res.status_code} - {res.text}")
                except Exception as e:
                    print(f"[FALHA] Conexão na fatura {body['number']}: {e}")

    except Exception as e:
        print(f"ERRO FATAL NO PROCESSAMENTO: {e}")

    if not_found:
        pd.DataFrame(not_found).to_csv("fornecedores_nao_encontrados.csv", sep=";", index=False, encoding="utf-8-sig")
        print(f"\nConcluído com {len(not_found)} erros de fornecedor salvos em CSV.")
    else:
        print("\nProcesso concluído com sucesso!")

if __name__ == "__main__":
    main()