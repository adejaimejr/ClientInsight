# 🏆 ClientInsight: Sistema de Classificação de Clientes

<div align="center">

![ClientInsight Banner](banner.svg)

![Version](https://img.shields.io/badge/versão-1.0.0-blue)
![Python](https://img.shields.io/badge/Python-3.12+-yellow?logo=python)
![MongoDB](https://img.shields.io/badge/MongoDB-4.4+-green?logo=mongodb)
![Linx](https://img.shields.io/badge/Linx-e--Millennium-red)

</div>

> **Sistema de classificação de clientes em níveis (Diamante, Ouro, Prata, Bronze) baseado em análise de dados de compras, pagamentos e comportamento.**

## 📋 Visão Geral

Este sistema analisa o comportamento de compra dos clientes e cria um sistema de classificação baseado em indicadores de desempenho. O objetivo é categorizar os clientes em diferentes níveis com base em seus padrões de compra, fidelidade e valor.

O **ClientInsight** integra-se com a API Linx e-Millennium para extrair dados transacionais e de cadastro, processando-os para gerar insights valiosos sobre o comportamento dos clientes.

### ✨ Benefícios

- **🔍 Identificação de clientes de alto valor**: Reconheça seus melhores clientes e ofereça tratamento diferenciado
- **📊 Segmentação estratégica**: Crie estratégias de marketing personalizadas para cada categoria de cliente
- **📈 Análise de comportamento**: Entenda os padrões de compra e preferências dos diferentes segmentos
- **💡 Tomada de decisão baseada em dados**: Utilize informações concretas para direcionar esforços de vendas e marketing

## 📊 Indicadores Analisados

O sistema coleta e analisa os seguintes indicadores para cada cliente:

1. **📅 Histórico de Compras**: Data da primeira compra e regularidade das transações
2. **💰 Volume de Negócios**: Faturamento total nos últimos 12 meses
3. **🔄 Frequência de Compra**: Número de ciclos mensais em que o cliente realizou compras
4. **🛍️ Quantidade de Itens**: Total de produtos adquiridos
5. **⏱️ Pontualidade de Pagamento**: Percentual de títulos pagos dentro do prazo
6. **🔀 Diversidade de Produtos**: Variedade de marcas adquiridas
7. **📊 Valor por Categoria**: Distribuição de gastos entre marcas

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
CLIENTE_TESTE=0000000000

# Configurações de processamento
PROCESSAR_TODOS=false
TAMANHO_LOTE=20
USAR_CACHE=true

# Configurações da API Linx e-Millennium
LINX_API_URL=https://api.exemplo.com
LINX_API_KEY=sua_chave_api
```

### 🔄 Modos de Execução

#### 🌐 Processamento Completo

Para processar todos os clientes e gerar a classificação completa:

1. Defina `PROCESSAR_TODOS=true` no arquivo `.env`
2. Execute: `python consultas.py`

#### 👤 Teste com Cliente Específico

Para analisar um cliente específico:

1. Defina o código do cliente em `CLIENTE_TESTE` no arquivo `.env`
2. Execute: `python testar_cliente.py`

#### 🔍 Verificação de Transações

Para verificar detalhes das transações de um cliente:

1. Defina o código do cliente em `CLIENTE_TESTE` no arquivo `.env`
2. Execute: `python verificar_lancamentos.py`

## 📂 Arquivos de Saída

O sistema gera arquivos JSON com os resultados da análise:

- `resultados_completos.json`: Dados de todos os clientes processados
- `cliente_XXXXXXXXXX_teste.json`: Análise detalhada de um cliente específico

### 📊 Exemplo de Resultado

Abaixo está um exemplo do resultado da análise para um cliente:

```json
{
  "codigo_cliente": "0000001234",
  "nome_completo": "Cliente Exemplo S.A.",
  "data_primeira_compra": "2024-08-15",
  "faturamento_ultimos_12_meses": {
    "total_vendas": 15750.45,
    "total_devolucoes": 1250.30,
    "faturamento_liquido": 14500.15
  },
  "ciclos_compra_ultimos_6_meses": 4,
  "ciclo_atual": true,
  "meses_compra": [
    "2024-11",
    "2024-12",
    "2025-01",
    "2025-03"
  ],
  "total_pecas": {
    "compradas": 145,
    "devolvidas": 12,
    "liquido": 133
  },
  "titulos_pagos_em_dia": {
    "total_lancamentos": 8,
    "total_pagos": 6,
    "total_a_vencer": 1,
    "total_vencido": 1,
    "percentual_pagos_total": 75.0,
    "percentual_a_vencer": 12.5,
    "percentual_vencido": 12.5,
    "pagos_em_dia": 5,
    "percentual_pagos_em_dia": 83.3,
    "pagos_em_ate_7d": 1,
    "percentual_pagos_em_ate_7d": 16.7,
    "pagos_em_ate_15d": 0,
    "percentual_pagos_em_ate_15d": 0.0,
    "pagos_em_ate_30d": 0,
    "percentual_pagos_em_ate_30d": 0.0,
    "pagos_com_mais_30d": 0,
    "percentual_pagos_com_mais_30d": 0.0,
    "usa_boleto": true
  },
  "valor_por_marca": {
    "Marca A": {
      "valor_vendas": 8500.25,
      "valor_devolucoes": 750.30,
      "valor_liquido": 7749.95
    },
    "Marca B": {
      "valor_vendas": 4250.10,
      "valor_devolucoes": 350.00,
      "valor_liquido": 3900.10
    },
    "Marca C": {
      "valor_vendas": 3000.10,
      "valor_devolucoes": 150.00,
      "valor_liquido": 2850.10
    }
  },
  "numero_marcas_diferentes": 3,
  "lista_marcas": [
    "Marca A",
    "Marca B",
    "Marca C"
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
- **🔌 Integração API**: Conexão com a API Linx e-Millennium para obtenção de dados em tempo real

### 📁 Estrutura do Projeto

- `consultas.py`: Script principal com implementação das consultas e processamento
- `testar_cliente.py`: Script para análise individual de clientes
- `verificar_lancamentos.py`: Ferramenta para verificação detalhada de transações
- `.env`: Arquivo de configuração com parâmetros do sistema

### 📋 Requisitos

- Python 3.12 ou superior
- PyMongo
- python-dotenv
- Acesso a um banco de dados MongoDB
- Credenciais para API Linx e-Millennium

## 🔮 Próximos Passos

- 📊 Implementação de um sistema de pontuação para classificação automática
- 🖥️ Desenvolvimento de interface gráfica para visualização dos resultados
- 📆 Criação de relatórios periódicos automatizados
- 🔄 Integração com sistemas de ERP adicionando no cadastro do cliente
- 🔔 Implementação de alertas para mudanças de categoria

---

<div align="center">
  
  📊 **ClientInsight: Desenvolvido para análise avançada de clientes** 📊
  
  <sub>©i92Tech 2025 - Todos os direitos reservados</sub>
  
</div>
