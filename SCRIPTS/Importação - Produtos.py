import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import os
import sys

BASE_URL = "https://api-v2.gauderp.com/v1"
ID_TABELA_1 = 1
ID_TABELA_2 = 1
ARQUIVO_CSV = "Importação_Produtos.csv"


def criar_sessao(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def get_token(session: requests.Session, user: str, password: str) -> str | None:
    res = session.post(f"{BASE_URL}/authentication/login", json={"username": user, "password": password})
    if res.status_code == 200:
        data = res.json()
        return data.get("accessToken") or data.get("token")
    return None


def to_float(v) -> float:
    if v is None or str(v).strip() == "" or str(v).lower() == "nan":
        return 0.0
    try:
        return float(str(v).replace(",", "."))
    except ValueError:
        return 0.0


def get_or_create_brand(name: str, session: requests.Session, cache: dict[str, int]) -> int | None:
    name = str(name).strip() if name and not pd.isna(name) else "GERAL"

    if name in cache:
        return cache[name]

    res = session.get(f"{BASE_URL}/inventory/brands", params={"name$eq": name})
    res.raise_for_status()
    data = res.json().get("data", [])

    if data:
        brand_id = int(data[0]["id"])
    else:
        res_c = session.post(f"{BASE_URL}/inventory/brands", json={"name": name})
        res_c.raise_for_status()
        brand_id = int(res_c.json()["id"])

    cache[name] = brand_id
    return brand_id


def importar_produto(row: pd.Series, session: requests.Session, brand_cache: dict[str, int]) -> str | None:
    """Retorna mensagem de erro ou None se sucesso."""
    code = str(row.get("CodigoDoProduto") or "").strip()
    nome = str(row.get("NomeProduto") or "").strip()
    if not code or not nome:
        return None  # pula silenciosamente

    internal = str(row.get("Codigointerno") or "").strip()
    barcode = str(row.get("Codigo_barra") or "").strip()
    location = str(row.get("Location") or row.get("Localizacao") or "").strip()
    unit = str(row.get("Unidade") or "UN").strip()
    custo = to_float(row.get("PrecoCusto"))
    estoque = to_float(row.get("Estoque"))
    preco = to_float(row.get("Preco1"))

    brand_id = get_or_create_brand(row.get("Marca"), session, brand_cache)

    body = {
        "name": nome,
        "code": code,
        "unit": unit,
        "location": location,
        "productBrands": [{
            "brandId": brand_id,
            "brand": {"id": brand_id},
            "stock": estoque,
            "lastPurchasePrice": custo,
            "barcode": barcode,
            "internalCode": internal,
            "code": code,
            "location": location,
        }],
    }

    res = session.post(f"{BASE_URL}/catalog/products", params={"ignoreDuplicateBrand": "true"}, json=body)
    if res.status_code not in (200, 201):
        return res.text

    product_brands = res.json().get("productBrands", [])
    if not product_brands:
        return None

    pb_id = product_brands[0]["id"]
    session.put(f"{BASE_URL}/inventory/products/{pb_id}/price-list", json={
        "lastPurchasePrice": custo,
        "lockSalePrice": True,
        "priceList": [{"priceList": {"id": ID_TABELA_1}, "price": preco, "markup": 0}],
        "priceList": [{"priceList": {"id": ID_TABELA_2}, "price": preco, "markup": 0}],
    })
    return None


def main():
    user = os.environ.get("GAUD_USER", "leonardo")
    password = os.environ.get("GAUD_PASSWORD", "123456")

    session = criar_sessao("")
    token = get_token(session, user, password)
    if not token:
        sys.exit("Falha na autenticação.")

    session.headers["Authorization"] = f"Bearer {token}"

    df = pd.read_csv(ARQUIVO_CSV, sep=";", dtype=str, encoding="latin1")
    df = df.where(pd.notnull(df), None)
    total = len(df)

    brand_cache: dict[str, int] = {}
    erros: list[dict] = []

    for i, row in df.iterrows():
        try:
            erro = importar_produto(row, session, brand_cache)
            if erro:
                erros.append({"Codigo": str(row.get("CodigoDoProduto", "")), "Erro": erro})
        except requests.RequestException as e:
            erros.append({"Codigo": str(row.get("CodigoDoProduto", "")), "Erro": str(e)})
        print(f"{i + 1}/{total}", end="\r")

    print(f"\nConcluído: {total} itens, {len(erros)} erros.")

    if erros:
        pd.DataFrame(erros).to_csv("Erros_Importacao.csv", sep=";", index=False, encoding="latin1")
        print("Erros salvos em Erros_Importacao.csv")


if __name__ == "__main__":
    main()