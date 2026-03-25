import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import math
import sys
import os
import time
from datetime import datetime

# Configurações
API_BASE_URL = "https://api-v2.gauderp.com/v1"
FILE_PATH = "Importação_Pedidos.csv"

def criar_sessao(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    })
    # Estratégia de reentrada para lidar com instabilidades de rede
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    return session

def strip_value(v):
    if pd.isna(v) or str(v).strip().lower() in ["nan", ""]: return None
    return str(v).strip()

def safe_float(v):
    val = strip_value(v)
    if not val: return 0.0
    try:
        return float(val.replace('.', '').replace(',', '.'))
    except ValueError:
        return 0.0

def to_int(v):
    try:
        return int(safe_float(v))
    except:
        return None

def parse_date(v):
    val = strip_value(v)
    if not val: return None
    try:
        dt = pd.to_datetime(val, dayfirst=True)
        return dt.strftime('%Y-%m-%dT%H:%M:%S.000')
    except:
        return None

def main():
    # Autenticação
    try:
        from import_data.authentication import authentication
        token = authentication("fa autopecas", "admin") 
    except ImportError:
        token = "seu_token_v2_aqui"

    session = criar_sessao(token)
    errored_items = [] # Lista para armazenar falhas

    try:
        # Leitura do CSV
        df = pd.read_csv(FILE_PATH, sep=";", dtype=str, encoding="latin1")
        df.columns = df.columns.str.strip()
        
        # Agrupamento por Número do Pedido
        grouped = df.groupby('NumeroDoPedido')
        total_pedidos = len(grouped)
        print(f"Iniciando importação V2 de {total_pedidos} pedidos...\n")
    except FileNotFoundError:
        sys.exit(f"Erro: Arquivo {FILE_PATH} não encontrado.")
    except Exception as e:
        sys.exit(f"Erro ao ler arquivo: {e}")

    for i, (pedido_num, group) in enumerate(grouped, 1):
        row = group.iloc[0] # Pega o cabeçalho do pedido
        
        status_v2 = (strip_value(row.get("Status")) or "DONE").upper()
        items = []
        total_pedido = 0.0
        
        # Consolida os produtos do grupo
        for _, item in group.iterrows():
            p_id = to_int(item.get("ID_Produto_ou_Codigo"))
            if not p_id: continue
            
            qtd = safe_float(item.get("Quantidade")) or 1.0
            preco_unitario = safe_float(item.get("ValorUnitario"))
            subtotal = round(qtd * preco_unitario, 2)
            total_pedido += subtotal
            
            items.append({
                "product": {"id": p_id},
                "quantity": qtd,
                "price": preco_unitario,
                "total": subtotal
            })

        if not items:
            msg = "Pedido sem produtos válidos (ID_Produto_ou_Codigo ausente)"
            print(f"[{i}/{total_pedidos}] Pedido {pedido_num}: {msg}")
            errored_items.append({"pedido": pedido_num, "erro": msg})
            continue

        # Montagem do JSON (Payload)
        payload = {
            "number": str(pedido_num),
            "status": status_v2,
            "customer": {"id": to_int(row.get("ID_Cliente"))},
            "seller": {"id": to_int(row.get("ID_Vendedor"))},
            "products": items,
            "payments": [{
                "paymentMethod": {"id": to_int(row.get("ID_FormaPagamento")) or 1},
                "value": round(total_pedido, 2)
            }],
            "total": round(total_pedido, 2),
            "totalDiscounts": 0.0,
            "createdAt": parse_date(row.get("CreatedAt")),
            "observation": strip_value(row.get("Observacao"))
        }

        # Remove chaves nulas para evitar erros de validação na API
        payload = {k: v for k, v in payload.items() if v is not None}

        try:
            r = session.post(f"{API_BASE_URL}/sales/orders", json=payload)
            
            if r.status_code in [200, 201]:
                print(f"[{i}/{total_pedidos}] Pedido {pedido_num}: Sucesso!")
            else:
                msg_erro = f"Status {r.status_code}: {r.text[:150]}"
                print(f"[{i}/{total_pedidos}] Pedido {pedido_num}: {msg_erro}")
                errored_items.append({"pedido": pedido_num, "erro": msg_erro})
        except Exception as e:
            msg_falha = f"Falha de conexão: {str(e)}"
            print(f"[{i}/{total_pedidos}] Pedido {pedido_num}: {msg_falha}")
            errored_items.append({"pedido": pedido_num, "erro": msg_falha})

    # Geração do Relatório de Erros
    if errored_items:
        pd.DataFrame(errored_items).to_excel("erros_v2_pedidos.xlsx", index=False)
        print(f"\n--- Processo finalizado com {len(errored_items)} erros. Veja 'erros_v2_pedidos.xlsx' ---")
    else:
        print("\n--- Processo finalizado com 100% de sucesso! ---")

if __name__ == "__main__":
    main()