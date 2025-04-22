"""
Script para analisar a distribuição das outras métricas importantes para a classificação.
"""
import os
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans

def carregar_arquivo(caminho_arquivo="resultados/resultados_completos.json"):
    """Carrega o arquivo JSON com os dados dos clientes."""
    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Erro ao carregar o arquivo: {e}")
        return None

def extrair_metricas(dados):
    """Extrai as métricas dos clientes: volume de peças, marcas e pontualidade."""
    metricas = []
    
    # Verifica se os dados são uma lista de clientes ou um objeto único
    if isinstance(dados, list):
        # É uma lista de clientes
        clientes = dados
    else:
        # É um único objeto que pode conter uma lista de clientes
        clientes = dados if isinstance(dados, list) else [dados]
    
    for cliente in clientes:
        try:
            # Extrai volume de peças
            total_pecas = cliente.get('total_pecas', {}).get('liquido', 0)
            
            # Extrai número de marcas
            num_marcas = cliente.get('numero_marcas_diferentes', 0)
            
            # Extrai pontualidade
            pontualidade = cliente.get('titulos_pagos_em_dia', {}).get('percentual_pagos_em_dia', 0)
            pontualidade_7d = cliente.get('titulos_pagos_em_dia', {}).get('percentual_pagos_em_ate_7d', 0)
            pontualidade_total = pontualidade + pontualidade_7d  # Consideram-se pagamentos até 7 dias como pontuais
            
            # Só adiciona clientes com dados válidos em pelo menos uma das métricas
            if total_pecas > 0 or num_marcas > 0 or pontualidade_total > 0:
                metricas.append({
                    'codigo': cliente.get('codigo_cliente', 'N/A'),
                    'nome': cliente.get('nome_completo', 'N/A'),
                    'total_pecas': total_pecas,
                    'num_marcas': num_marcas,
                    'pontualidade': pontualidade_total
                })
        except Exception as e:
            print(f"Erro ao extrair métricas do cliente: {str(e)}")
    
    return metricas

def analisar_volume_pecas(metricas):
    """Analisa a distribuição do volume de peças."""
    df = pd.DataFrame(metricas)
    
    # Filtra apenas clientes com dados de peças
    df_pecas = df[df['total_pecas'] > 0].copy()
    
    if len(df_pecas) == 0:
        print("Nenhum dado válido de volume de peças encontrado.")
        return
    
    print("\n=== ANÁLISE DE VOLUME DE PEÇAS ===")
    print(f"Total de clientes com dados: {len(df_pecas)}")
    print(f"Mínimo: {df_pecas['total_pecas'].min():.0f} peças")
    print(f"Máximo: {df_pecas['total_pecas'].max():.0f} peças")
    print(f"Média: {df_pecas['total_pecas'].mean():.2f} peças")
    print(f"Mediana: {df_pecas['total_pecas'].median():.0f} peças")
    
    # Análise de percentis
    percentis = [10, 25, 50, 75, 90, 95]
    valores_percentis = [df_pecas['total_pecas'].quantile(p/100) for p in percentis]
    
    print("\nPercentis do Volume de Peças:")
    for p, v in zip(percentis, valores_percentis):
        print(f"Percentil {p}%: {v:.0f} peças")
    
    # Sugestão de configuração
    print("\nSugestão de configuração para o arquivo .env:")
    print(f"PECAS_FAIXA_10={int(valores_percentis[5])}  # Percentil 95")
    print(f"PECAS_FAIXA_8={int(valores_percentis[4])}   # Percentil 90")
    print(f"PECAS_FAIXA_6={int(valores_percentis[3])}   # Percentil 75")
    print(f"PECAS_FAIXA_4={int(valores_percentis[1])}   # Percentil 25")
    
    try:
        # Gera o histograma
        if not os.path.exists('resultados'):
            os.makedirs('resultados')
            
        plt.figure(figsize=(10, 6))
        plt.hist(df_pecas['total_pecas'].clip(upper=df_pecas['total_pecas'].quantile(0.99)), bins=30)
        plt.title('Distribuição de Volume de Peças (99º percentil)')
        plt.xlabel('Número de Peças')
        plt.ylabel('Quantidade de Clientes')
        plt.savefig('resultados/distribuicao_pecas.png')
        print("Histograma salvo em 'resultados/distribuicao_pecas.png'")
    except Exception as e:
        print(f"Erro ao gerar histograma: {e}")

def analisar_diversificacao_marcas(metricas):
    """Analisa a distribuição da diversificação de marcas."""
    df = pd.DataFrame(metricas)
    
    # Filtra apenas clientes com dados de marcas
    df_marcas = df[df['num_marcas'] > 0].copy()
    
    if len(df_marcas) == 0:
        print("Nenhum dado válido de diversificação de marcas encontrado.")
        return
    
    print("\n=== ANÁLISE DE DIVERSIFICAÇÃO DE MARCAS ===")
    print(f"Total de clientes com dados: {len(df_marcas)}")
    print(f"Mínimo: {df_marcas['num_marcas'].min():.0f} marcas")
    print(f"Máximo: {df_marcas['num_marcas'].max():.0f} marcas")
    print(f"Média: {df_marcas['num_marcas'].mean():.2f} marcas")
    print(f"Mediana: {df_marcas['num_marcas'].median():.0f} marcas")
    
    # Análise da distribuição
    contagem = df_marcas['num_marcas'].value_counts().sort_index()
    
    print("\nDistribuição de marcas:")
    for n_marcas, qtd in contagem.items():
        print(f"{n_marcas} marcas: {qtd} clientes ({qtd/len(df_marcas)*100:.1f}%)")
    
    # Recomendações atuais são boas, então não precisamos mudar muito
    print("\nRecomendação para configuração (atual já parece boa):")
    print("MARCAS_PARA_10_PONTOS=6        # 6 ou mais marcas = 10 pontos")
    print("MARCAS_PARA_8_PONTOS=4-5       # 4 ou 5 marcas = 8 pontos")
    print("MARCAS_PARA_6_PONTOS=2-3       # 2 ou 3 marcas = 6 pontos")
    print("MARCAS_PARA_4_PONTOS=1         # 1 marca = 4 pontos")
    
    try:
        # Gera o gráfico de barras
        plt.figure(figsize=(10, 6))
        # Limita a 10 marcas para melhor visualização
        contagem_limitada = contagem[contagem.index <= 10] 
        plt.bar(contagem_limitada.index, contagem_limitada.values)
        plt.title('Distribuição de Diversificação de Marcas')
        plt.xlabel('Número de Marcas')
        plt.ylabel('Quantidade de Clientes')
        plt.xticks(range(1, min(11, contagem_limitada.index.max() + 1)))
        plt.savefig('resultados/distribuicao_marcas.png')
        print("Gráfico salvo em 'resultados/distribuicao_marcas.png'")
    except Exception as e:
        print(f"Erro ao gerar gráfico: {e}")

def analisar_pontualidade(metricas):
    """Analisa a distribuição da pontualidade nos pagamentos."""
    df = pd.DataFrame(metricas)
    
    # Filtra apenas clientes com dados de pontualidade
    df_pont = df[df['pontualidade'] > 0].copy()
    
    if len(df_pont) == 0:
        print("Nenhum dado válido de pontualidade encontrado.")
        return
    
    print("\n=== ANÁLISE DE PONTUALIDADE ===")
    print(f"Total de clientes com dados: {len(df_pont)}")
    print(f"Mínimo: {df_pont['pontualidade'].min():.2f}%")
    print(f"Máximo: {df_pont['pontualidade'].max():.2f}%")
    print(f"Média: {df_pont['pontualidade'].mean():.2f}%")
    print(f"Mediana: {df_pont['pontualidade'].median():.2f}%")
    
    # Análise de percentis
    percentis = [10, 25, 50, 75, 90, 95]
    valores_percentis = [df_pont['pontualidade'].quantile(p/100) for p in percentis]
    
    print("\nPercentis da Pontualidade:")
    for p, v in zip(percentis, valores_percentis):
        print(f"Percentil {p}%: {v:.2f}%")
    
    # Sugestão de configuração
    print("\nSugestão de configuração para o arquivo .env:")
    print(f"PONTUALIDADE_FAIXA_10={valores_percentis[4]:.0f}  # Percentil 90")
    print(f"PONTUALIDADE_FAIXA_8={valores_percentis[3]:.0f}   # Percentil 75")
    print(f"PONTUALIDADE_FAIXA_6={valores_percentis[2]:.0f}   # Percentil 50")
    print(f"PONTUALIDADE_FAIXA_4={valores_percentis[1]:.0f}   # Percentil 25")
    
    try:
        # Gera o histograma
        plt.figure(figsize=(10, 6))
        plt.hist(df_pont['pontualidade'].clip(0, 100), bins=20, range=(0, 100))
        plt.title('Distribuição de Pontualidade nos Pagamentos')
        plt.xlabel('Pontualidade (%)')
        plt.ylabel('Quantidade de Clientes')
        plt.savefig('resultados/distribuicao_pontualidade.png')
        print("Histograma salvo em 'resultados/distribuicao_pontualidade.png'")
    except Exception as e:
        print(f"Erro ao gerar histograma: {e}")

def main():
    """Função principal."""
    print("=== ANALISADOR DE MÉTRICAS DE CLASSIFICAÇÃO ===")
    
    # Carrega os dados
    dados = carregar_arquivo()
    if not dados:
        print("Não foi possível carregar os dados.")
        return
    
    # Extrai as métricas
    metricas = extrair_metricas(dados)
    if not metricas:
        print("Nenhuma métrica válida encontrada.")
        return
    
    print(f"Dados de {len(metricas)} clientes carregados com sucesso.")
    
    # Analisa cada métrica
    analisar_volume_pecas(metricas)
    analisar_diversificacao_marcas(metricas)
    analisar_pontualidade(metricas)

if __name__ == "__main__":
    main()
