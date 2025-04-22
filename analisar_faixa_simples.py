"""
Script para analisar a distribuição de faturamento a partir de um arquivo JSON existente.
"""
import os
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans

def carregar_arquivo(caminho_arquivo):
    """Carrega um arquivo JSON."""
    try:
        with open(caminho_arquivo, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Erro ao carregar o arquivo: {e}")
        return None

def extrair_faturamentos(dados):
    """Extrai os dados de faturamento de cada cliente."""
    faturamentos = []
    
    # Verifica se os dados são uma lista de clientes ou um objeto único
    if isinstance(dados, list):
        # É uma lista de clientes
        clientes = dados
    else:
        # É um único objeto que pode conter uma lista de clientes
        clientes = dados if isinstance(dados, list) else [dados]
    
    for cliente in clientes:
        try:
            # Extrai o faturamento líquido
            fat = cliente.get('faturamento_ultimos_12_meses', {}).get('faturamento_liquido', 0)
            if fat > 0:  # Ignora faturamentos nulos ou negativos
                faturamentos.append({
                    'codigo': cliente.get('codigo_cliente', 'N/A'),
                    'nome': cliente.get('nome_completo', 'N/A'),
                    'faturamento': fat
                })
        except Exception as e:
            print(f"Erro ao extrair faturamento do cliente: {str(e)}")
    
    return faturamentos

def analisar_distribuicao(faturamentos):
    """Analisa a distribuição dos faturamentos e sugere faixas ideais."""
    if not faturamentos:
        print("Nenhum dado de faturamento disponível para análise")
        return
    
    # Converte para DataFrame para facilitar a análise
    df = pd.DataFrame(faturamentos)
    
    # Estatísticas básicas
    print("\n=== ESTATÍSTICAS DE FATURAMENTO ===")
    print(f"Total de clientes com faturamento: {len(df)}")
    print(f"Faturamento mínimo: R$ {df['faturamento'].min():.2f}")
    print(f"Faturamento máximo: R$ {df['faturamento'].max():.2f}")
    print(f"Faturamento médio: R$ {df['faturamento'].mean():.2f}")
    print(f"Faturamento mediana: R$ {df['faturamento'].median():.2f}")
    
    # Análise de percentis
    percentis = [10, 25, 50, 75, 90, 95]
    valores_percentis = [df['faturamento'].quantile(p/100) for p in percentis]
    
    print("\n=== ANÁLISE DE PERCENTIS ===")
    for p, v in zip(percentis, valores_percentis):
        print(f"Percentil {p}%: R$ {v:.2f}")
    
    # Sugestão baseada em percentis - Usando distribuição normal
    print("\n=== SUGESTÃO DE FAIXAS BASEADA EM PERCENTIS ===")
    print(f"Faixa para 10 pontos: R$ {valores_percentis[4]:.2f} ou mais (Percentil 90+)")
    print(f"Faixa para 8 pontos: R$ {valores_percentis[3]:.2f} a R$ {valores_percentis[4]:.2f} (Percentil 75-90)")
    print(f"Faixa para 6 pontos: R$ {valores_percentis[2]:.2f} a R$ {valores_percentis[3]:.2f} (Percentil 50-75)")
    print(f"Faixa para 4 pontos: R$ {valores_percentis[1]:.2f} a R$ {valores_percentis[2]:.2f} (Percentil 25-50)")
    print(f"Faixa para 2 pontos: R$ {valores_percentis[0]:.2f} a R$ {valores_percentis[1]:.2f} (Percentil 10-25)")
    print(f"Faixa para 0 pontos: Abaixo de R$ {valores_percentis[0]:.2f} (Percentil <10)")
    
    # Análise com K-means para encontrar clusters naturais
    try:
        if len(df) >= 5:  # Só tenta fazer clustering se houver pelo menos 5 clientes
            # Preparação dos dados para clustering
            X = np.array(df['faturamento']).reshape(-1, 1)
            
            # K-means com 5 clusters (para 5 níveis de pontuação)
            kmeans = KMeans(n_clusters=5, random_state=42, n_init=10).fit(X)
            
            # Obtém os centroides e ordena
            centroides = [c[0] for c in kmeans.cluster_centers_]
            centroides.sort()
            
            print("\n=== SUGESTÃO DE FAIXAS BASEADA EM CLUSTERS (K-MEANS) ===")
            print(f"Faixa para 10 pontos: R$ {centroides[4]:.2f} ou mais")
            print(f"Faixa para 8 pontos: R$ {centroides[3]:.2f} a R$ {centroides[4]:.2f}")
            print(f"Faixa para 6 pontos: R$ {centroides[2]:.2f} a R$ {centroides[3]:.2f}")
            print(f"Faixa para 4 pontos: R$ {centroides[1]:.2f} a R$ {centroides[2]:.2f}")
            print(f"Faixa para 2 pontos: Abaixo de R$ {centroides[1]:.2f}")
        else:
            print("\nDados insuficientes para análise de clusters (mínimo de 5 clientes necessários).")
    except Exception as e:
        print(f"Erro na análise de clusters: {e}")
    
    # Sugestão de configuração para o arquivo .env
    print("\n=== SUGESTÃO DE CONFIGURAÇÃO PARA .ENV ===")
    print("# Com base nos percentis")
    print(f"FATURAMENTO_FAIXA_10={int(valores_percentis[4])}")
    print(f"FATURAMENTO_FAIXA_8={int(valores_percentis[3])}")
    print(f"FATURAMENTO_FAIXA_6={int(valores_percentis[2])}")
    print(f"FATURAMENTO_FAIXA_4={int(valores_percentis[1])}")
    
    # Tenta criar um histograma para visualização
    try:
        if not os.path.exists('resultados'):
            os.makedirs('resultados')
            
        plt.figure(figsize=(10, 6))
        plt.hist(df['faturamento'], bins=min(30, len(df)//2 + 1))
        plt.title('Distribuição de Faturamento')
        plt.xlabel('Faturamento (R$)')
        plt.ylabel('Quantidade de Clientes')
        
        for i, (p, v) in enumerate(zip(percentis[2:], valores_percentis[2:])):
            cor = ['r', 'g', 'b', 'm'][i % 4]
            plt.axvline(v, color=cor, linestyle='--', label=f'{p}º Percentil: R$ {v:.2f}')
        
        plt.legend()
        plt.savefig('resultados/distribuicao_faturamento.png')
        print("\nHistograma gerado e salvo em 'resultados/distribuicao_faturamento.png'")
    except Exception as e:
        print(f"Não foi possível gerar o histograma: {e}")

def main():
    """Função principal."""
    print("=== ANALISADOR DE FAIXAS DE FATURAMENTO ===")
    
    # Caminho direto para o arquivo
    caminho_arquivo = "resultados/resultados_completos.json"
    print(f"Analisando arquivo: {caminho_arquivo}")
    
    # Carrega os dados
    dados = carregar_arquivo(caminho_arquivo)
    if not dados:
        print("Não foi possível carregar os dados. Verifique o caminho e formato do arquivo.")
        return
    
    # Extrai e analisa os faturamentos
    faturamentos = extrair_faturamentos(dados)
    if faturamentos:
        print(f"Foram encontrados dados de {len(faturamentos)} clientes com faturamento válido.")
        analisar_distribuicao(faturamentos)
    else:
        print("Nenhum dado de faturamento válido encontrado no arquivo.")

if __name__ == "__main__":
    main()
