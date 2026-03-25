# Importação de Produtos - GAUD ERP

## Descrição Geral

Este script realiza a importação em massa de produtos para o sistema GAUD ERP a partir de um arquivo CSV. O processo inclui autenticação na API, criação automática de marcas quando necessário, configuração de preços e estoque, e geração de relatório de erros.

## Dependências

- **pandas**: Manipulação de dados CSV
- **requests**: Comunicação HTTP com a API
- **requests.adapters**: Configuração de retry estratégico
- **os**: Acesso a variáveis de ambiente
- **sys**: Tratamento de erros e saída do programa

## Configurações Globais

```python
BASE_URL = "https://api-v2.gauderp.com/v1"  # URL base da API
ID_TABELA_1 = 1                              # ID fixo da lista de preços (Cada Produto pode ter varias listas, dependendo da quantidade de valores, o ID é obtido pelo Metabase)
ID_TABELA_2 = 1                              # ID da segunda tabela de preços
ARQUIVO_CSV = "Importação_Produtos.csv"      # Nome do arquivo CSV
```

## Funções

### `criar_sessao(token: str) -> requests.Session`
Cria uma sessão HTTP com configurações de retry e headers padrão.

**Parâmetros:**
- `token`: Token de autenticação Bearer

**Retorno:** Objeto `requests.Session` configurado

### `get_token(session: requests.Session, user: str, password: str) -> str | None`
Realiza autenticação na API e obtém token de acesso.

**Parâmetros:**
- `session`: Sessão HTTP configurada
- `user`: Nome de usuário
- `password`: Senha

**Retorno:** String com o token de acesso ou `None` em caso de falha

### `to_float(v) -> float`
Converte valores para float, tratando valores nulos ou inválidos.

**Parâmetros:**
- `v`: Valor a ser convertido

**Retorno:** Float (0.0 para valores inválidos)

### `get_or_create_brand(name: str, session: requests.Session, cache: dict[str, int]) -> int | None`
Busca ou cria marca no sistema, utilizando cache para otimização.

**Parâmetros:**
- `name`: Nome da marca
- `session`: Sessão HTTP autenticada
- `cache`: Dicionário para cache de marcas

**Retorno:** ID da marca ou `None`

**Comportamento:**
- Se a marca não existir, cria automaticamente com nome "GERAL"
- Utiliza cache para evitar consultas repetidas
- Retorna ID da marca existente ou recém-criada

### `importar_produto(row: pd.Series, session: requests.Session, brand_cache: dict[str, int]) -> str | None`
Importa um único produto para o sistema.

**Parâmetros:**
- `row`: Linha do DataFrame com dados do produto
- `session`: Sessão HTTP autenticada
- `brand_cache`: Cache de marcas

**Retorno:** Mensagem de erro ou `None` se sucesso

**Processo:**
1. Valida campos obrigatórios (código e nome)
2. Extrai e limpa dados do produto
3. Obtém ou cria marca
4. Monta payload JSON
5. Envia produto para API
6. Configura tabelas de preços

## Fluxo de Execução

1. **Autenticação:**
   - Obtém credenciais das variáveis de ambiente
   - Realiza login na API
   - Valida token obtido

2. **Leitura do CSV:**
   - Lê arquivo `Importação_Produtos.csv`
   - Usa encoding `latin1` e separador `;`
   - Converte valores nulos para `None`

3. **Processamento:**
   - Itera sobre cada linha do DataFrame
   - Processa produto individualmente
   - Atualiza contador de progresso
   - Captura erros e armazena em lista

4. **Relatório Final:**
   - Exibe estatísticas de processamento
   - Gera arquivo `Erros_Importacao.csv` com falhas

## Estrutura do Payload - Produto

```json
{
    "name": "Nome do produto",
    "code": "Código do produto",
    "unit": "UN",
    "location": "Localização",
    "productBrands": [{
        "brandId": 123,
        "brand": {"id": 123},
        "stock": 100.0,
        "lastPurchasePrice": 50.0,
        "barcode": "123456789",
        "internalCode": "INT001",
        "code": "COD001",
        "location": "A1"
    }]
}
```

## Estrutura do Payload - Preços

```json
{
    "lastPurchasePrice": 50.0,
    "lockSalePrice": true,
    "priceList": [
        {"priceList": {"id": 1}, "price": 75.0, "markup": 0},
        {"priceList": {"id": 1}, "price": 75.0, "markup": 0}
    ]
}
```

## Mapeamento de Colunas CSV

| Coluna CSV | Campo API | Tratamento |
|------------|-----------|------------|
| CodigoDoProduto | code | Obrigatório |
| NomeProduto | name | Obrigatório |
| Codigointerno | productBrands.internalCode | Limpeza |
| Codigo_barra | productBrands.barcode | Limpeza |
| Location/Localizacao | location | Limpeza |
| Unidade | unit | Default "UN" |
| Marca | productBrands.brandId | Busca/criação automática |
| PrecoCusto | productBrands.lastPurchasePrice | Conversão float |
| Estoque | productBrands.stock | Conversão float |
| Preco1 | priceList.price | Conversão float |

## Variáveis de Ambiente

- `GAUD_USER`: Usuário para autenticação (default: "leonardo")
- `GAUD_PASSWORD`: Senha para autenticação (default: "123456")

## Tratamento de Erros

- **Falha de autenticação:** Encerramento do programa
- **Produtos sem código/nome:** Ignorados silenciosamente
- **Erros de API:** Capturados e registrados
- **Falhas de conexão:** Capturadas e registradas

## Arquivos Gerados

- **Erros_Importacao.csv**: Relatório de produtos com falha na importação
  - Colunas: `Codigo`, `Erro`

## Otimizações

- **Cache de marcas:** Evita consultas repetidas à API
- **Processamento individual:** Isolamento de falhas
- **Progresso em tempo real:** Contador atualizado a cada produto
- **Tratamento robusto:** Conversão segura para valores numéricos

## Observações

- Marcas não encontradas são criadas automaticamente
- Produtos sem marca recebem marca "GERAL"
- Preço de venda bloqueado (`lockSalePrice: true`)
- Duas tabelas de preços configuradas (mesmo preço para ambas)
- Barras de progresso exibidas no console

## Como Usar

1. Configure as variáveis de ambiente ou use os valores padrão
2. Prepare o arquivo `Importação_Produtos.csv` com as colunas corretas
3. Execute o script: `python "Importação - Produtos.py"`
4. Monitore o progresso no console
5. Verifique arquivo `Erros_Importacao.csv` para falhas

## Estrutura do Arquivo CSV

O arquivo CSV deve conter as seguintes colunas (separador `;`):
- CodigoDoProduto (obrigatório)
- NomeProduto (obrigatório)
- Codigointerno
- Codigo_barra
- Location ou Localizacao
- Unidade
- Marca
- PrecoCusto
- Estoque
- Preco1

## IDs de Tabelas de Preços

- `ID_TABELA_1`: Configurar com ID correto da primeira tabela
- `ID_TABELA_2`: Configurar com ID correto da segunda tabela
- IDs podem ser obtidos através do Metabase ou API

## Exemplo de Uso

```bash
# Configurar variáveis de ambiente (opcional)
export GAUD_USER="seu_usuario"
export GAUD_PASSWORD="sua_senha"

# Executar importação
python "Importação - Produtos.py"
```

## Saída Esperada

```
1000/1000
Concluído: 1000 itens, 5 erros.
Erros salvos em Erros_Importacao.csv
```
