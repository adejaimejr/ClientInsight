# 🏆 ClientInsight: Sistema de Classificação de Clientes

<div align="center">

![ClientInsight Banner](banner.svg)

![Version](https://img.shields.io/badge/versão-1.2.0-blue)
![Python](https://img.shields.io/badge/Python-3.12+-yellow?logo=python)
![MongoDB](https://img.shields.io/badge/MongoDB-4.4+-green?logo=mongodb)
![Linx](https://img.shields.io/badge/Linx-e--Millennium-red)

</div>

> **Sistema de classificação de clientes em níveis (Diamante, Ouro, Prata, Bronze) baseado em análise de dados de compras, pagamentos e comportamento.**

## 📋 Visão Geral

Este sistema analisa o comportamento de compra dos clientes e cria um sistema de classificação baseado em indicadores de desempenho. O objetivo é categorizar os clientes em diferentes níveis com base em seus padrões de compra, fidelidade e valor.

O **ClientInsight** integra-se com o MongoDB para extrair dados transacionais e de cadastro, processando-os para gerar insights valiosos sobre o comportamento dos clientes.

### ✨ Benefícios

- **🔍 Identificação de clientes de alto valor**: Reconheça seus melhores clientes e ofereça tratamento diferenciado
- **📊 Segmentação estratégica**: Crie estratégias de marketing personalizadas para cada categoria de cliente
- **📈 Análise de comportamento**: Entenda os padrões de compra e preferências dos diferentes segmentos
- **💡 Tomada de decisão baseada em dados**: Utilize informações concretas para direcionar esforços de vendas e marketing
- **⚡ Processamento otimizado**: Análise apenas de clientes com movimentações reais, reduzindo o tempo de processamento
- **📅 Ajustes inteligentes de datas**: Consideração de finais de semana e feriados nacionais para títulos a pagar

## 📊 Indicadores Analisados

O sistema coleta e analisa os seguintes indicadores para cada cliente:

1. **📅 Histórico Completo**: Data de cadastro, primeira e última compra
2. **💰 Limites de Crédito**: Limite total e utilizado
3. **💼 Volume de Negócios**: Faturamento total nos últimos 12 meses
4. **🔄 Frequência de Compra**: Número de ciclos mensais em que o cliente realizou compras
5. **🛍️ Quantidade de Itens**: Total de produtos adquiridos
6. **⏱️ Pontualidade de Pagamento**: Percentual de títulos pagos dentro do prazo
7. **🔀 Diversidade de Produtos**: Variedade de marcas adquiridas
8. **📊 Valor por Categoria**: Distribuição de gastos entre marcas

## 🚀 Como Usar

### ⚙️ Configuração

1. Clone o repositório:
```bash
git clone https://github.com/adejaimejr/ClientInsight.git
```

2. Instale as dependências: 
```bash
pip install pymongo python-dotenv
```

3. Configure o arquivo `.env` com os parâmetros necessários:

```ini
# Configurações de conexão com o banco de dados
MONGODB_URI=mongodb://usuario:senha@servidor:porta/database
MONGODB_DATABASE=nome_do_banco

# Cliente para validação
CLIENTE_TESTE=0000000826

# Configurações de processamento
PROCESSAR_TODOS=false
TAMANHO_LOTE=20
USAR_CACHE=false

# Configurações de processamento paralelo
USAR_PARALELO=true
NUM_THREADS=4
```

### 🔄 Modos de Execução

#### 🌐 Processamento Completo

Para processar todos os clientes e gerar a classificação completa:

1. Defina `PROCESSAR_TODOS=true` no arquivo `.env`
2. Execute: `python main.py`

#### 🚀 Processamento Paralelo

Para processar todos os clientes em paralelo (mais rápido):

1. Defina `USAR_PARALELO=true` e `NUM_THREADS=4` (ou mais, dependendo do seu hardware)
2. Execute: `python processar_paralelo.py`

#### 👤 Teste com Cliente Específico

Para analisar um cliente específico:

1. Defina o código do cliente em `CLIENTE_TESTE` no arquivo `.env`
2. Execute: `python main.py`

## 📂 Arquivos de Saída

O sistema gera arquivos JSON com os resultados da análise:

- `resultados_completos.json`: Dados de todos os clientes processados
- `resultados_completos_paralelo.json`: Dados processados em modo paralelo
- `resultados_parciais_lote_X.json`: Resultados parciais por lote
- `resultados_grupo_X.json`: Resultados parciais por grupo (processamento paralelo)
- `resultado_XXXXXXXXXX_YYYYMMDD_HHMMSS.json`: Análise detalhada de um cliente específico com timestamp

### 📊 Exemplo de Resultado

Abaixo está um exemplo do resultado da análise para um cliente:

```json
{
  "id": {
    "$oid": "60a7b2c8d9e8f7g6h5i4j3k2"
  },
  "codigo_cliente": "0000001234",
  "nome_completo": "EMPRESA MODELO COMERCIAL LTDA",
  "data_cadastro": "2019-06-15",
  "data_cadastro_timestamp": 1560571200,
  "data_primeira_compra": "2023-09-10",
  "data_primeira_compra_timestamp": 1694304000,
  "data_ultima_compra": "2025-03-22",
  "data_ultima_compra_timestamp": 1742889600,
  "limite_credito": 15000,
  "limite_credito_utilizado": 8750.25,
  "faturamento_ultimos_12_meses": {
    "total_vendas": 42500.75,
    "total_devolucoes": 7625.30,
    "faturamento_liquido": 34875.45
  },
  "ciclos_compra_ultimos_6_meses": 5,
  "ciclo_atual": true,
  "meses_compra": [
    "2024-10",
    "2024-11",
    "2024-12",
    "2025-01",
    "2025-03"
  ],
  "total_pecas": {
    "compradas": 385,
    "devolvidas": 62,
    "liquido": 323
  },
  "titulos_pagos_em_dia": {
    "total_lancamentos": 12,
    "total_pagos": 9,
    "total_a_vencer": 2,
    "total_vencido": 1,
    "percentual_pagos_total": 75.0,
    "percentual_a_vencer": 16.7,
    "percentual_vencido": 8.3,
    "pagos_em_dia": 8,
    "percentual_pagos_em_dia": 88.9
  },
  "valor_por_marca": {
    "MARCA PREMIUM": {
      "valor_vendas": 16250.50,
      "valor_devolucoes": 2100.75,
      "valor_liquido": 14149.75
    },
    "MARCA CASUAL": {
      "valor_vendas": 12450.25,
      "valor_devolucoes": 3200.50,
      "valor_liquido": 9249.75
    },
    "MARCA FASHION": {
      "valor_vendas": 13800.00,
      "valor_devolucoes": 2324.05,
      "valor_liquido": 11475.95
    }
  },
  "numero_marcas_diferentes": 12,
  "lista_marcas": [
    "MARCA PREMIUM", "MARCA CASUAL", "MARCA FASHION", "MARCA JEANS", "MARCA ESPORTE"
  ],
  "classificacao": "Ouro"
}
```

## 🛠️ Aspectos Técnicos

### ⚡ Recursos Implementados

- **📦 Processamento em Lotes**: Capacidade de processar grandes volumes de dados em lotes configuráveis
- **💾 Sistema de Cache**: Armazenamento de resultados parciais para evitar reprocessamento
- **📝 Logs Detalhados**: Acompanhamento em tempo real do progresso de processamento
- **⚡ Consultas Otimizadas**: Extração eficiente de dados de múltiplas coleções
- **🛡️ Tratamento de Exceções**: Sistema robusto com tratamento adequado de erros
- **⚡ Processamento Paralelo**: Capacidade de processar clientes em múltiplas threads simultaneamente
- **🔍 Filtragem Inteligente**: Processamento apenas de clientes com movimentações reais
- **📅 Ajuste de Datas**: Consideração de finais de semana e feriados nacionais para datas de vencimento
- **💰 Validação de Transações**: Filtragem de operações inválidas ou canceladas

### 📁 Estrutura do Projeto

- `main.py`: Script principal com implementação do processamento
- `processar_paralelo.py`: Script para processamento paralelo de clientes
- `consultas/`: Pacote com módulos de consultas específicas
  - `__init__.py`: Inicialização do pacote
  - `base.py`: Funções e constantes base compartilhadas
  - `ciclos_compra.py`: Cálculo de ciclos de compra
  - `faturamento.py`: Análise de faturamento
  - `pecas_compradas.py`: Contagem de peças
  - `titulos_pagos.py`: Análise de títulos e pagamentos

## 📄 Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para detalhes.

## 👨‍💻 Autor

Implementado por Andre Dejaime Jr.
