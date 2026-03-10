import pandas as pd
import requests

BASE_URL = "https://api-v2.gauderp.com/v1"
ID_TABELA_1 = 285 
ID_TABELA_2 = 320
ID_TABELA_3 = 321
ID_TABELA_4 = 322

relatorio_erros = []

def get_token(user, password):
    url = f"{BASE_URL}/authentication/login"
    payload = {"username": user, "password": password}
    try:
        response = requests.post(url, json=payload)
        return response.json().get("accessToken") or response.json().get("token")
    except: return None

token = get_token("autopecas souza car", "admin")
headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

def to_float(v):
    if v is None or str(v).strip() == '': return 0.0
    try: return float(str(v).replace(',', '.'))
    except: return 0.0

def search_brand(name):
    name = str(name).strip() if name and not pd.isna(name) else "GERAL"
    try:
        res = requests.get(f"{BASE_URL}/inventory/brands?name$eq={name}", headers=headers)
        data = res.json().get('data', [])
        if data: return data[0]["id"]
        res_c = requests.post(f"{BASE_URL}/inventory/brands", headers=headers, json={"name": name})
        return res_c.json().get("id")
    except: return None

try:
    df = pd.read_csv('Importação_Produtos.csv', sep=';', dtype=str, encoding='latin1')
    df = df.where(pd.notnull(df), None)
    total = len(df)

    for i, item in df.iterrows():
        codigo = str(item.get("CodigoDoProduto")).strip()
        nome = str(item.get("NomeProduto")).strip()
        
        try:
            brand_id = search_brand(item.get("Marca"))
            custo = to_float(item.get('PrecoCusto'))
            estoque = to_float(item.get('Estoque'))
            
            p1 = to_float(item.get('Preco1'))
            p2 = to_float(item.get('Preco2'))
            p3 = to_float(item.get('Preco3'))
            p4 = to_float(item.get('Preco4'))

            body_create = {
                "name": nome,
                "code": codigo,
                "unit": str(item.get("Unidade") or 'UN').strip(),
                "productBrands": [
                    {
                        "brand": {"id": brand_id},
                        "stock": estoque,
                        "lastPurchasePrice": custo
                    }
                ]
            }
            
            res_create = requests.post(f"{BASE_URL}/catalog/products", headers=headers, json=body_create)
            
            if res_create.status_code in [200, 201]:
                data_res = res_create.json()
                pb_id = data_res.get("productBrands")[0].get("id") if data_res.get("productBrands") else None

                if pb_id:
                    url_price = f"{BASE_URL}/inventory/products/{pb_id}/price-list"
                    
                    payload_price = {
                        "lastPurchasePrice": custo,
                        "lockSalePrice": True,
                        "priceList": [
                            {"priceList": {"id": ID_TABELA_1}, "price": p1, "markup": 0},
                            {"priceList": {"id": ID_TABELA_2}, "price": p2, "markup": 0},
                            {"priceList": {"id": ID_TABELA_3}, "price": p3, "markup": 0},
                            {"priceList": {"id": ID_TABELA_4}, "price": p4, "markup": 0}
                        ]
                    }
                    
                    res_put = requests.put(url_price, headers=headers, json=payload_price)
                    
                    if res_put.status_code in [200, 204]:
                        print(f"Progresso: {i+1}/{total} - {codigo} OK!", end='\r')
                    else:
                        relatorio_erros.append({"Codigo": codigo, "Erro": f"Erro Preço: {res_put.text}"})
                else:
                    relatorio_erros.append({"Codigo": codigo, "Erro": "PB_ID não encontrado"})
            else:
                relatorio_erros.append({"Codigo": codigo, "Erro": f"Erro Criação: {res_create.text}"})

        except Exception as e_loop:
            relatorio_erros.append({"Codigo": codigo, "Erro": f"Erro Interno: {str(e_loop)}"})

except Exception as e:
    print(f"\nERRO CRÍTICO: {e}")

print("\n\nProcessamento concluído.")

if relatorio_erros:
    df_err = pd.DataFrame(relatorio_erros)
    df_err.to_csv('Erros_Importacao.csv', sep=';', index=False, encoding='latin1')
    print(f"Atenção: {len(relatorio_erros)} itens apresentaram erro. Veja 'Erros_Importacao.csv'.")
else:
    print("Sucesso! Todos os produtos e preços foram importados.")