# ClientInsight - Documentação do Sistema

## Documentação do Formato JSON de Saída

Este documento fornece uma explicação detalhada de cada campo presente no JSON gerado pelo sistema ClientInsight, que analisa dados de clientes para fornecer insights sobre comportamento de compra, padrões de pagamento e relacionamento com marcas.

### Campos Básicos de Identificação

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id.oid` | String | Identificador único do cliente no banco de dados MongoDB, no formato ObjectId. |
| `codigo_cliente` | String | Código único do cliente no sistema ERP, geralmente formatado com zeros à esquerda (ex: "0000000826"). |
| `nome_completo` | String | Nome ou razão social completa do cliente. |

### Campos de Datas

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `data_cadastro` | String | Data de cadastro do cliente no formato ISO (YYYY-MM-DD). |
| `data_cadastro_timestamp` | Number | Timestamp Unix (segundos desde 01/01/1970) da data de cadastro. |
| `data_primeira_compra` | String | Data da primeira compra realizada pelo cliente no formato ISO. |
| `data_primeira_compra_timestamp` | Number | Timestamp Unix da data da primeira compra. |
| `data_ultima_compra` | String | Data da compra mais recente realizada pelo cliente. |
| `data_ultima_compra_timestamp` | Number | Timestamp Unix da data da última compra. |

### Limites de Crédito

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `limite_credito` | Number | Valor total do limite de crédito concedido ao cliente, em moeda local. |
| `limite_credito_utilizado` | Number | Montante do limite de crédito que já está comprometido com compras pendentes de pagamento. |

### Faturamento

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `faturamento_ultimos_12_meses.total_vendas` | Number | Valor total bruto de todas as vendas nos últimos 12 meses. |
| `faturamento_ultimos_12_meses.total_devolucoes` | Number | Valor total das devoluções nos últimos 12 meses. |
| `faturamento_ultimos_12_meses.faturamento_liquido` | Number | Valor líquido (vendas - devoluções) do faturamento nos últimos 12 meses. |

### Ciclos de Compra

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `ciclos_compra_ultimos_6_meses` | Number | Número de meses distintos em que o cliente realizou compras nos últimos 6 meses. |
| `ciclo_atual` | Boolean | Indica se o cliente realizou compras no mês atual (`true`) ou não (`false`). |
| `meses_compra` | Array | Lista de meses (formato "YYYY-MM") em que o cliente realizou compras nos últimos 6 meses. |

### Quantidade de Peças

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `total_pecas.compradas` | Number | Número total de peças/itens comprados pelo cliente. |
| `total_pecas.devolvidas` | Number | Número total de peças/itens devolvidos pelo cliente. |
| `total_pecas.liquido` | Number | Número líquido de peças (trocadas - devolvidas). |

### Análise de Pagamentos

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `titulos_pagos_em_dia.codigo_cliente` | String | Código do cliente para referência na análise de pagamentos. |
| `titulos_pagos_em_dia.total_lancamentos` | Number | Número total de títulos/faturas emitidos para o cliente. |
| `titulos_pagos_em_dia.total_pagos` | Number | Número de títulos que já foram pagos pelo cliente. |
| `titulos_pagos_em_dia.total_a_vencer` | Number | Número de títulos que ainda não venceram. |
| `titulos_pagos_em_dia.total_vencido` | Number | Número de títulos vencidos e não pagos. |
| `titulos_pagos_em_dia.percentual_pagos_total` | Number | Percentual de títulos pagos em relação ao total (total_pagos/total_lancamentos * 100). |
| `titulos_pagos_em_dia.percentual_a_vencer` | Number | Percentual de títulos a vencer em relação ao total. |
| `titulos_pagos_em_dia.percentual_vencido` | Number | Percentual de títulos vencidos em relação ao total. |
| `titulos_pagos_em_dia.inadimplente` | Boolean | Indica se o cliente está inadimplente (`true`) ou não (`false`). |
| `titulos_pagos_em_dia.inadimplente_dias` | Number | Maior número de dias de atraso em títulos vencidos, se houver. |
| `titulos_pagos_em_dia.inadimplente_valor` | Number | Valor total dos títulos vencidos (inadimplência). |
| `titulos_pagos_em_dia.total_a_vencer_valor` | Number | Valor total dos títulos a vencer. |
| `titulos_pagos_em_dia.usa_boleto` | Boolean | Indica se o cliente utiliza boletos como forma de pagamento. |

### Detalhamento da Pontualidade de Pagamento

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `titulos_pagos_em_dia.pagos_em_dia` | Number | Número de títulos pagos até a data de vencimento. |
| `titulos_pagos_em_dia.percentual_pagos_em_dia` | Number | Percentual de títulos pagos em dia em relação ao total de pagos. |
| `titulos_pagos_em_dia.pagos_em_ate_7d` | Number | Número de títulos pagos com até 7 dias de atraso. |
| `titulos_pagos_em_dia.percentual_pagos_em_ate_7d` | Number | Percentual de títulos pagos com até 7 dias de atraso. |
| `titulos_pagos_em_dia.pagos_em_ate_15d` | Number | Número de títulos pagos com até 15 dias de atraso. |
| `titulos_pagos_em_dia.percentual_pagos_em_ate_15d` | Number | Percentual de títulos pagos com até 15 dias de atraso. |
| `titulos_pagos_em_dia.pagos_em_ate_30d` | Number | Número de títulos pagos com até 30 dias de atraso. |
| `titulos_pagos_em_dia.percentual_pagos_em_ate_30d` | Number | Percentual de títulos pagos com até 30 dias de atraso. |
| `titulos_pagos_em_dia.pagos_com_mais_30d` | Number | Número de títulos pagos com mais de 30 dias de atraso. |
| `titulos_pagos_em_dia.percentual_pagos_com_mais_30d` | Number | Percentual de títulos pagos com mais de 30 dias de atraso. |

### Análise de Marcas

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `valor_por_marca.codigo_cliente` | String | Código do cliente para referência na análise de marcas. |
| `valor_por_marca.valor_por_marca` | Object | Dicionário com os valores gastos em cada marca. Cada chave é o nome da marca e o valor é o total gasto naquela marca. |
| `numero_marcas_diferentes` | Number | Número total de marcas diferentes que o cliente comprou. |
| `lista_marcas` | Array | Lista dos nomes de todas as marcas adquiridas pelo cliente. |

## Cálculos e Considerações Importantes

1. **Faturamento Líquido**: É calculado pela diferença entre o total de vendas e o total de devoluções.

2. **Ciclos de Compra**: Um valor alto indica cliente com compras frequentes e regular. Cliente que compra em todos os 6 meses tem valor 6, indicando alta regularidade.

3. **Inadimplência**: É determinada quando há pelo menos um título vencido. O campo `inadimplente_dias` mostra a gravidade do atraso.

4. **Uso de Limites**: A comparação entre `limite_credito` e `limite_credito_utilizado` indica quanto do potencial de compra do cliente ainda está disponível.

5. **Pontualidade de Pagamento**: Os percentuais de pagamentos em dia e com diferentes níveis de atraso permitem avaliar o comportamento de pagamento do cliente.

6. **Diversidade de Marcas**: O número e lista de marcas diferentes compradas indicam a abrangência de interesse do cliente no portfólio.

7. **Ajustes de Data**: O sistema considera finais de semana e feriados nacionais ao calcular atrasos em pagamentos, para uma análise mais justa.

## Próximos Passos: Desenvolvimento da Classificação de Clientes

Os dados extraídos e processados pelo ClientInsight serão a base para o futuro sistema de classificação de clientes, que ainda precisa ser implementado. O objetivo será categorizar os clientes em diferentes níveis (Diamante, Ouro, Prata, Bronze) com base nos seguintes fatores principais:

1. Volume de faturamento líquido
2. Regularidade de compras (ciclos de compra)
3. Pontualidade nos pagamentos
4. Diversidade de marcas adquiridas
5. Longevidade do relacionamento (desde a primeira compra)

Será necessário desenvolver algoritmos e lógicas de pontuação que analisem esses indicadores de forma ponderada para determinar a classificação final do cliente. Este desenvolvimento representa uma das próximas fases do projeto, permitindo no futuro estratégias de marketing e atendimento diferenciadas para cada segmento.

## Notas Técnicas

- Todas as datas são armazenadas em dois formatos: string ISO para legibilidade e timestamp Unix para cálculos.
- Valores monetários são representados na moeda local, sem símbolo de moeda.
- Em caso de valores ausentes em campos numéricos, o valor padrão é geralmente zero.
- Os campos booleanos (`ciclo_atual`, `inadimplente`, `usa_boleto`) são utilizados para decisões rápidas sem necessidade de analisar os dados detalhados.
