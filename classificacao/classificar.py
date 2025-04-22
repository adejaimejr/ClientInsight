"""
Módulo para classificação de clientes com base em critérios específicos.

Este módulo implementa um algoritmo de pontuação ponderada para classificar clientes
nas categorias: Diamante, Ouro, Prata e Bronze.
"""
import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# Configurações de classificação - Pesos dos critérios (configuráveis via .env)
PESO_FATURAMENTO = float(os.getenv("PESO_FATURAMENTO", "0.40"))
PESO_FREQUENCIA = float(os.getenv("PESO_FREQUENCIA", "0.25"))
PESO_PONTUALIDADE = float(os.getenv("PESO_PONTUALIDADE", "0.15"))
PESO_VOLUME_PECAS = float(os.getenv("PESO_VOLUME_PECAS", "0.10"))
PESO_DIVERSIFICACAO = float(os.getenv("PESO_DIVERSIFICACAO", "0.10"))

# Constantes para as categorias
CATEGORIA_DIAMANTE = os.getenv("CATEGORIA_DIAMANTE", "Diamante")
CATEGORIA_OURO = os.getenv("CATEGORIA_OURO", "Ouro")
CATEGORIA_PRATA = os.getenv("CATEGORIA_PRATA", "Prata")
CATEGORIA_BRONZE = os.getenv("CATEGORIA_BRONZE", "Bronze")

# Limites de pontuação para cada categoria (configuráveis via .env)
LIMITE_DIAMANTE = float(os.getenv("LIMITE_DIAMANTE", "9.1"))
LIMITE_OURO = float(os.getenv("LIMITE_OURO", "7.5"))
LIMITE_PRATA = float(os.getenv("LIMITE_PRATA", "5.0"))
LIMITE_BRONZE = float(os.getenv("LIMITE_BRONZE", "0.0"))

# Configurações de faixas de pontuação para cada critério
# Faturamento
FATURAMENTO_FAIXA_10 = float(os.getenv("FATURAMENTO_FAIXA_10", "50000"))
FATURAMENTO_FAIXA_8 = float(os.getenv("FATURAMENTO_FAIXA_8", "30001"))
FATURAMENTO_FAIXA_6 = float(os.getenv("FATURAMENTO_FAIXA_6", "15001"))
FATURAMENTO_FAIXA_4 = float(os.getenv("FATURAMENTO_FAIXA_4", "5001"))

# Volume de peças
PECAS_FAIXA_10 = float(os.getenv("PECAS_FAIXA_10", "500"))
PECAS_FAIXA_8 = float(os.getenv("PECAS_FAIXA_8", "201"))
PECAS_FAIXA_6 = float(os.getenv("PECAS_FAIXA_6", "101"))
PECAS_FAIXA_4 = float(os.getenv("PECAS_FAIXA_4", "50"))

# Diversificação de marcas
MARCAS_PARA_10_PONTOS = int(os.getenv("MARCAS_PARA_10_PONTOS", "6"))

# Para as faixas com intervalos (formato "4-5"), extraímos os valores mínimo e máximo
def extrair_faixa(variavel_env, padrao="0-0"):
    """Extrai valores mínimo e máximo de uma string no formato 'min-max'"""
    faixa = os.getenv(variavel_env, padrao)
    if "-" in faixa:
        partes = faixa.split("-")
        if len(partes) == 2:
            try:
                return int(partes[0]), int(partes[1])
            except ValueError:
                return 0, 0
    return 0, 0

# Extrai os limites das faixas de pontuação para marcas
MARCAS_PARA_8_PONTOS_MIN, MARCAS_PARA_8_PONTOS_MAX = extrair_faixa("MARCAS_PARA_8_PONTOS", "4-5")
MARCAS_PARA_6_PONTOS_MIN, MARCAS_PARA_6_PONTOS_MAX = extrair_faixa("MARCAS_PARA_6_PONTOS", "2-3")
MARCAS_PARA_4_PONTOS = int(os.getenv("MARCAS_PARA_4_PONTOS", "1"))

# Pontualidade
PONTUALIDADE_FAIXA_10 = float(os.getenv("PONTUALIDADE_FAIXA_10", "95"))
PONTUALIDADE_FAIXA_8 = float(os.getenv("PONTUALIDADE_FAIXA_8", "85"))
PONTUALIDADE_FAIXA_6 = float(os.getenv("PONTUALIDADE_FAIXA_6", "75"))
PONTUALIDADE_FAIXA_4 = float(os.getenv("PONTUALIDADE_FAIXA_4", "60"))

def pontuar_faturamento(valor_faturamento):
    """
    Pontua o cliente com base no faturamento líquido dos últimos 12 meses.
    
    Args:
        valor_faturamento: Valor do faturamento líquido em reais
    
    Returns:
        Pontuação de 0 a 10
    """
    if valor_faturamento >= FATURAMENTO_FAIXA_10:
        return 10
    elif valor_faturamento >= FATURAMENTO_FAIXA_8:
        return 8
    elif valor_faturamento >= FATURAMENTO_FAIXA_6:
        return 6
    elif valor_faturamento >= FATURAMENTO_FAIXA_4:
        return 4
    else:
        return 2

def pontuar_frequencia(ciclos):
    """
    Pontua o cliente com base na frequência de compras nos últimos 6 meses.
    
    Args:
        ciclos: Número de meses com compras nos últimos 6 meses
    
    Returns:
        Pontuação de 0 a 10
    """
    if ciclos == 6:
        return 10
    elif ciclos == 5:
        return 8
    elif ciclos == 4:
        return 6
    elif ciclos in [2, 3]:
        return 4
    elif ciclos == 1:
        return 2
    else:
        return 0

def pontuar_pontualidade(percentual_pagos_em_dia, percentual_pagos_ate_7_dias=0):
    """
    Pontua o cliente com base na pontualidade de pagamentos (em dia ou até 7 dias).
    
    Args:
        percentual_pagos_em_dia: Percentual de títulos pagos em dia
        percentual_pagos_ate_7_dias: Percentual de títulos pagos com até 7 dias de atraso
    
    Returns:
        Pontuação de 0 a 10
    """
    # Calcula o percentual total considerando pagamentos em dia e pagamentos até 7 dias como equivalentes
    # conforme especificado nos critérios "Percentual pago em dia (ou até 7 dias)"
    percentual_total = percentual_pagos_em_dia + percentual_pagos_ate_7_dias
    
    if percentual_total >= PONTUALIDADE_FAIXA_10:
        return 10
    elif percentual_total >= PONTUALIDADE_FAIXA_8:
        return 8
    elif percentual_total >= PONTUALIDADE_FAIXA_6:
        return 6
    elif percentual_total >= PONTUALIDADE_FAIXA_4:
        return 4
    else:
        return 2

def pontuar_volume_pecas(total_pecas):
    """
    Pontua o cliente com base no volume líquido de peças compradas.
    
    Args:
        total_pecas: Número líquido de peças compradas (compras - devoluções)
    
    Returns:
        Pontuação de 0 a 10
    """
    if total_pecas >= PECAS_FAIXA_10:
        return 10
    elif total_pecas >= PECAS_FAIXA_8:
        return 8
    elif total_pecas >= PECAS_FAIXA_6:
        return 6
    elif total_pecas >= PECAS_FAIXA_4:
        return 4
    else:
        return 2

def pontuar_diversificacao(numero_marcas):
    """
    Pontua o cliente com base na diversificação de marcas adquiridas.
    
    Args:
        numero_marcas: Número de marcas diferentes adquiridas
    
    Returns:
        Pontuação de 0 a 10
    """
    if numero_marcas >= MARCAS_PARA_10_PONTOS:
        return 10
    elif MARCAS_PARA_8_PONTOS_MIN <= numero_marcas <= MARCAS_PARA_8_PONTOS_MAX:
        return 8
    elif MARCAS_PARA_6_PONTOS_MIN <= numero_marcas <= MARCAS_PARA_6_PONTOS_MAX:
        return 6
    elif numero_marcas == MARCAS_PARA_4_PONTOS:
        return 4
    else:
        return 0

def definir_categoria(pontuacao):
    """
    Define a categoria do cliente com base na pontuação final.
    
    Args:
        pontuacao: Pontuação final do cliente (0 a 10)
    
    Returns:
        Categoria do cliente (Diamante, Ouro, Prata ou Bronze)
    """
    if pontuacao >= LIMITE_DIAMANTE:
        return CATEGORIA_DIAMANTE
    elif pontuacao >= LIMITE_OURO:
        return CATEGORIA_OURO
    elif pontuacao >= LIMITE_PRATA:
        return CATEGORIA_PRATA
    elif pontuacao >= LIMITE_BRONZE:
        return CATEGORIA_BRONZE
    else:
        return CATEGORIA_BRONZE  # Categoria padrão para qualquer pontuação abaixo de LIMITE_BRONZE

def calcular_pontuacao(
    faturamento_liquido,
    ciclos_compra,
    percentual_pagos_em_dia,
    percentual_pagos_ate_7_dias,
    total_pecas_liquido,
    numero_marcas
):
    """
    Calcula a pontuação ponderada final do cliente.
    
    Args:
        faturamento_liquido: Valor do faturamento líquido em reais
        ciclos_compra: Número de meses com compras nos últimos 6 meses
        percentual_pagos_em_dia: Percentual de títulos pagos em dia
        percentual_pagos_ate_7_dias: Percentual de títulos pagos com até 7 dias de atraso
        total_pecas_liquido: Número líquido de peças compradas
        numero_marcas: Número de marcas diferentes adquiridas
    
    Returns:
        Pontuação final ponderada (0 a 10)
    """
    # Calcula as pontuações individuais
    pontos_faturamento = pontuar_faturamento(faturamento_liquido)
    pontos_frequencia = pontuar_frequencia(ciclos_compra)
    pontos_pontualidade = pontuar_pontualidade(percentual_pagos_em_dia, percentual_pagos_ate_7_dias)
    pontos_volume = pontuar_volume_pecas(total_pecas_liquido)
    pontos_diversificacao = pontuar_diversificacao(numero_marcas)
    
    # Calcula a pontuação final ponderada
    pontuacao_final = (
        (pontos_faturamento * PESO_FATURAMENTO) +
        (pontos_frequencia * PESO_FREQUENCIA) +
        (pontos_pontualidade * PESO_PONTUALIDADE) +
        (pontos_volume * PESO_VOLUME_PECAS) +
        (pontos_diversificacao * PESO_DIVERSIFICACAO)
    )
    
    return round(pontuacao_final, 2)

def classificar_cliente(cliente_data):
    """
    Classifica um cliente com base nos dados fornecidos pelo sistema ClientInsight.
    
    Args:
        cliente_data: Dicionário com os dados do cliente processados pelo ClientInsight
    
    Returns:
        Dicionário com os resultados da classificação:
            - pontuacao_final: Pontuação final do cliente (0-10)
            - categoria: Categoria do cliente (Diamante, Ouro, Prata ou Bronze)
            - pontuacoes_criterios: Dicionário com as pontuações por critério
    """
    try:
        # Extrai os dados necessários do dicionário do cliente
        faturamento_liquido = cliente_data.get('faturamento_ultimos_12_meses', {}).get('faturamento_liquido', 0)
        ciclos_compra = cliente_data.get('ciclos_compra_ultimos_6_meses', 0)
        
        # Dados de pontualidade
        titulos = cliente_data.get('titulos_pagos_em_dia', {})
        percentual_pagos_em_dia = titulos.get('percentual_pagos_em_dia', 0)
        percentual_pagos_ate_7_dias = titulos.get('percentual_pagos_em_ate_7d', 0)
        
        # Volume de peças e marcas
        total_pecas_liquido = cliente_data.get('total_pecas', {}).get('liquido', 0)
        numero_marcas = cliente_data.get('numero_marcas_diferentes', 0)
        
        # Se numero_marcas for 0 mas há marcas na lista, usa o tamanho da lista
        if numero_marcas == 0 and 'lista_marcas' in cliente_data and cliente_data['lista_marcas']:
            numero_marcas = len(cliente_data['lista_marcas'])
        
        # Calcula as pontuações individuais
        pontos_faturamento = pontuar_faturamento(faturamento_liquido)
        pontos_frequencia = pontuar_frequencia(ciclos_compra)
        pontos_pontualidade = pontuar_pontualidade(percentual_pagos_em_dia, percentual_pagos_ate_7_dias)
        pontos_volume = pontuar_volume_pecas(total_pecas_liquido)
        pontos_diversificacao = pontuar_diversificacao(numero_marcas)
        
        # Calcula a pontuação final
        pontuacao_final = calcular_pontuacao(
            faturamento_liquido,
            ciclos_compra,
            percentual_pagos_em_dia,
            percentual_pagos_ate_7_dias,
            total_pecas_liquido,
            numero_marcas
        )
        
        # Define a categoria
        categoria = definir_categoria(pontuacao_final)
        
        # Constrói o resultado
        resultado = {
            'pontuacao_final': pontuacao_final,
            'categoria': categoria,
            'pontuacoes_criterios': {
                'faturamento': {
                    'valor': faturamento_liquido,
                    'pontuacao': pontos_faturamento,
                    'peso': PESO_FATURAMENTO,
                    'ponderado': round(pontos_faturamento * PESO_FATURAMENTO, 2)
                },
                'frequencia': {
                    'valor': ciclos_compra,
                    'pontuacao': pontos_frequencia,
                    'peso': PESO_FREQUENCIA,
                    'ponderado': round(pontos_frequencia * PESO_FREQUENCIA, 2)
                },
                'pontualidade': {
                    'valor_pagos_em_dia': percentual_pagos_em_dia,
                    'valor_pagos_ate_7d': percentual_pagos_ate_7_dias,
                    'pontuacao': pontos_pontualidade,
                    'peso': PESO_PONTUALIDADE,
                    'ponderado': round(pontos_pontualidade * PESO_PONTUALIDADE, 2)
                },
                'volume_pecas': {
                    'valor': total_pecas_liquido,
                    'pontuacao': pontos_volume,
                    'peso': PESO_VOLUME_PECAS,
                    'ponderado': round(pontos_volume * PESO_VOLUME_PECAS, 2)
                },
                'diversificacao': {
                    'valor': numero_marcas,
                    'pontuacao': pontos_diversificacao,
                    'peso': PESO_DIVERSIFICACAO,
                    'ponderado': round(pontos_diversificacao * PESO_DIVERSIFICACAO, 2)
                }
            }
        }
        
        return resultado
        
    except Exception as e:
        # Em caso de erro, retorna uma classificação padrão
        return {
            'pontuacao_final': 0,
            'categoria': CATEGORIA_BRONZE,
            'erro': str(e)
        }

def obter_descricao_categoria(categoria):
    """
    Retorna a descrição para cada categoria de cliente.
    
    Args:
        categoria: Nome da categoria (Diamante, Ouro, Prata, Bronze)
    
    Returns:
        Descrição textual da categoria
    """
    descricoes = {
        CATEGORIA_DIAMANTE: "Cliente de alto volume, frequência exemplar e excelente pontualidade. Representa o mais alto valor para o negócio.",
        CATEGORIA_OURO: "Cliente frequente e fiel, com bom volume de compras e pontualidade nos pagamentos.",
        CATEGORIA_PRATA: "Cliente recorrente com volume médio de compras e oportunidade de desenvolvimento.",
        CATEGORIA_BRONZE: "Cliente eventual ou novato, com potencial de crescimento a ser desenvolvido."
    }
    
    return descricoes.get(categoria, "Categoria não identificada.")

def obter_sugestoes_categoria(categoria):
    """
    Retorna sugestões de benefícios e ações para cada categoria de cliente.
    
    Args:
        categoria: Nome da categoria (Diamante, Ouro, Prata, Bronze)
    
    Returns:
        Lista de sugestões para a categoria
    """
    sugestoes = {
        CATEGORIA_DIAMANTE: [
            "Oferecer brindes exclusivos a cada compra",
            "Garantir prioridade no atendimento e nas entregas",
            "Proporcionar crédito extra com condições especiais",
            "Programa de cashback exclusivo",
            "Convite para eventos exclusivos da marca"
        ],
        CATEGORIA_OURO: [
            "Inclusão em sorteios exclusivos",
            "Concessão de crédito extra",
            "Descontos especiais em determinadas marcas",
            "Atendimento personalizado",
            "Acesso antecipado a novos lançamentos"
        ],
        CATEGORIA_PRATA: [
            "Descontos exclusivos em categorias específicas",
            "Promoções direcionadas com base no histórico de compras",
            "Incentivos para aumentar frequência de compras",
            "Sugestões personalizadas de produtos"
        ],
        CATEGORIA_BRONZE: [
            "Ofertas especiais de entrada para novas categorias",
            "Campanhas específicas para aumentar frequência",
            "Incentivos para primeira compra em novas marcas",
            "Programas de fidelização iniciais"
        ]
    }
    
    return sugestoes.get(categoria, ["Nenhuma sugestão disponível para esta categoria."])
