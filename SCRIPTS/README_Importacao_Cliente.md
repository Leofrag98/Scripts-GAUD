# Importação de Clientes - GAUD ERP

## Descrição Geral

Este script realiza a importação em massa de clientes para o sistema GAUD ERP a partir de um arquivo CSV. O processo inclui autenticação na API, leitura do arquivo CSV em chunks, tratamento de dados e envio dos clientes para a API REST.

## Dependências

- **pandas**: Manipulação de dados CSV
- **requests**: Comunicação HTTP com a API
- **requests.adapters**: Configuração de retry estratégico
- **os**: Acesso a variáveis de ambiente
- **sys**: Tratamento de erros e saída do programa

## Configurações Globais

```python
BASE_URL_V2 = "https://api-v2.gauderp.com/v1"  # URL base da API
FIXED_PRICE_LIST_ID = 1                         # ID fixo da lista de preços (Cada Cliente tem a sua lista padrão, o ID é obtido pelo Metabase)
FIXED_PRICE_LIST_OBJECT = {"id": FIXED_PRICE_LIST_ID}
ARQUIVO_CSV = 'Importação_Cliente.csv'         # Nome do arquivo CSV
```

## Funções

### `criar_sessao(token: str = "") -> requests.Session`
Cria uma sessão HTTP com configurações de retry e headers padrão.

**Parâmetros:**
- `token` (opcional): Token de autenticação Bearer

**Retorno:** Objeto `requests.Session` configurado

### `get_token(session: requests.Session, user, password) -> str`
Realiza autenticação na API e obtém token de acesso.

**Parâmetros:**
- `session`: Sessão HTTP configurada
- `user`: Nome de usuário
- `password`: Senha

**Retorno:** String com o token de acesso

### `remove_non_numbers(text)`
Remove caracteres não numéricos de uma string.

**Parâmetros:**
- `text`: Texto a ser limpo

**Retorno:** String contendo apenas dígitos

### `clean_value(value)`
Limpa valores nulos ou vazios, retornando `None` para dados inválidos.

**Parâmetros:**
- `value`: Valor a ser verificado

**Retorno:** Valor limpo ou `None`

### `search_city(session: requests.Session, state, city, cache: dict) -> int | None`
Busca ID da cidade na API do IBGE com cache para otimização.

**Parâmetros:**
- `session`: Sessão HTTP autenticada
- `state`: Sigla do estado (UF)
- `city`: Nome da cidade
- `cache`: Dicionário para cache de consultas

**Retorno:** ID da cidade ou `None` se não encontrada

### `main()`
Função principal que orquestra todo o processo de importação.

## Fluxo de Execução

1. **Autenticação:**
   - Obtém credenciais das variáveis de ambiente (`GAUD_USER`, `GAUD_PASSWORD`)
   - Realiza login na API
   - Atualiza headers da sessão com token

2. **Leitura do CSV:**
   - Lê arquivo `Importação_Cliente.csv` em chunks de 1000 registros
   - Usa encoding `latin1` e separador `;`
   - Converte valores nulos para `None`

3. **Processamento de Clientes:**
   - Para cada cliente, extrai e limpa os dados
   - Busca ID da cidade usando cache
   - Monta payload JSON com estrutura completa
   - Envia para endpoint `/sales/customers`

4. **Tratamento de Erros:**
   - Captura erros de autenticação
   - Trata falhas de conexão
   - Reporta status de cada operação

## Estrutura do Payload

```json
{
    "active": true,
    "documentNumber": "CNPJ/CPF limpo",
    "stateRegistration": "Inscrição estadual",
    "name": "Razão social ou nome",
    "fantasyName": "Nome fantasia",
    "address": {
        "address": "Endereço",
        "number": "Número",
        "neighborhood": "Bairro",
        "addressComplement": "Complemento",
        "city": {"id": 123},
        "state": "UF",
        "zipCode": "CEP",
        "country": "BR"
    },
    "type": "INDIVIDUAL|COMPANY",
    "contacts": [
        {
            "name": "Nome do contato",
            "phone": "Telefone",
            "email": "E-mail"
        }
    ],
    "reference": "Código do cliente",
    "priceList": {"id": 1}
}
```

## Mapeamento de Colunas CSV

| Coluna CSV | Campo API | Tratamento |
|------------|-----------|------------|
| RazaoSocial_NomeCliente | name/fantasyName | Limpeza de espaços |
| NomeFantasia | fantasyName/name | Limpeza de espaços |
| CNPJ_CPF | documentNumber | Remove não-numéricos |
| RG_InscEst | stateRegistration | Remove não-numéricos |
| Endereco | address.address | Limpeza |
| NUMERO | address.number | Remove não-numéricos |
| Bairro | address.neighborhood | Limpeza |
| pontoreferencia | address.addressComplement | Limpeza |
| Cidade | address.city (via busca) | Busca ID na API |
| UF | address.state | Limpeza |
| Cep | address.zipCode | Remove não-numéricos |
| Contato | contacts[0].name | Limpeza |
| Telefone | contacts[0].phone | Remove não-numéricos |
| E_Mail | contacts[0].email | Limpeza |
| CodigoDoCliente | reference | Limpeza |

## Variáveis de Ambiente

- `GAUD_USER`: Usuário para autenticação (default: "leonardo")
- `GAUD_PASSWORD`: Senha para autenticação (default: "123456")

## Tratamento de Erros

- **Arquivo não encontrado:** Mensagem informativa e continuação
- **Falha de autenticação:** Encerramento do programa com `sys.exit()`
- **Erros de API:** Log detalhado com status code e resposta
- **Falhas de conexão:** Captura e log sem interromper processo

## Otimizações

- **Cache de cidades:** Evita consultas repetidas à API
- **Processamento em chunks:** Reduz consumo de memória
- **Retry estratégico:** 5 tentativas com backoff exponencial
- **Validação de contatos:** Remove contatos vazios antes do envio

## Observações

- Clientes sem nome ou nome fantasia são ignorados
- Tipo de cliente (INDIVIDUAL/COMPANY) determinado pelo tamanho do documento
- Lista de preços fixa (ID=1) atribuída a todos os clientes
- País fixo como "BR" (Brasil)

## Como Usar

1. Configure as variáveis de ambiente ou use os valores padrão
2. Prepare o arquivo `Importação_Cliente.csv` com as colunas corretas
3. Execute o script: `python "Importação - Cliente.py"`
4. Monitore o console para ver o progresso e possíveis erros

## Estrutura do Arquivo CSV

O arquivo CSV deve conter as seguintes colunas (separador `;`):
- RazaoSocial_NomeCliente
- NomeFantasia
- CNPJ_CPF
- RG_InscEst
- Endereco
- NUMERO
- Bairro
- pontoreferencia
- Cidade
- UF
- Cep
- Contato
- Telefone
- E_Mail
- CodigoDoCliente
