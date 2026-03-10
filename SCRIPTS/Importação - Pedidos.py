import pandas as pd
import requests
import math
import json
import time

try:
    from import_data.authentication import authentication
    token = authentication("fa autopecas", "admin") 
except:
    token = "seu_token_v2_aqui"

headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
API_BASE_URL = "https://api-v2.gauderp.com/v1"

def strip(v):
    if v is None or (isinstance(v, float) and math.isnan(v)): return None
    return str(v).strip()

def safe_float(v):
    val = strip(v)
    if not val: return 0.0
    try:
        return float(val.replace(',', '.'))
    except ValueError:
        return 0.0

def to_int(v):
    try:
        val = safe_float(v)
        return int(val)
    except:
        return None

def parse_date(v):
    try:
        dt = pd.to_datetime(strip(v), dayfirst=True)
        return dt.strftime('%Y-%m-%dT%H:%M:%S.000')
    except: return None

file_path = "Importação_Pedidos.csv"
try:
    df = pd.read_csv(file_path, sep=";", dtype=str, encoding="latin1")
    grouped = df.groupby('NumeroDoPedido')
    total_pedidos = len(grouped)
    print(f"Iniciando importação V2 de {total_pedidos} pedidos...\n")
except Exception as e:
    print(f"Erro ao ler arquivo: {e}")
    exit()

for i, (pedido_num, group) in enumerate(grouped, 1):
    row = group.iloc[0]
    
    status_planilha = strip(row.get("Status")) or "DONE"
    status_planilha = status_planilha.upper()

    items = []
    total_pedido = 0.0
    
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

    payload = {
        "number": str(pedido_num),
        "status": status_planilha,
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
        "observation": strip(row.get("Observacao"))
    }

    payload = {k: v for k, v in payload.items() if v is not None}

    try:
        r = requests.post(f"{API_BASE_URL}/sales/orders", headers=headers, json=payload)
        
        if r.status_code in [200, 201]:
            print(f"[{i}/{total_pedidos}] Pedido {pedido_num}: Sucesso!")
        else:
            print(f"[{i}/{total_pedidos}] Pedido {pedido_num}: Erro {r.status_code} - {r.text[:150]}")
    except Exception as e:
        print(f"[{i}/{total_pedidos}] Falha de conexão no pedido {pedido_num}: {e}")

    time.sleep(0.1)

print("\n--- Processo finalizado ---")