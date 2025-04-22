"""
Script para analisar os dados de faturamento e determinar faixas de classificação ideais.

Este script analisa um conjunto de resultados de clientes para determinar 
as faixas de faturamento mais adequadas para classificação.
"""
import os
import json
import glob
import numpy as np
import pandas as pd
from dotenv import load_dotenv
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans

# Carrega as variáveis de ambiente
load_dotenv()

def carregar_resultados(diretorio='resultados'):
    """Carrega todos os arquivos de resultado JSON do diretório especificado."""
    dados = []
    arquivos = glob.glob(f"{diretorio}/resultado_*.json")
    
    # Se não encontrou arquivos de resultado, verifica se há um arquivo de resultados completos
    if not arquivos:
        if os.path.exists(f"{diretorio}/resultados_completos.json"):
            try:
                with open(f"{diretorio}/resultados_completos.json", 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Erro ao carregar resultados completos: {e}")
                return []
        else:
            print("Nenhum arquivo de resultado encontrado")
            return []
    
    # Carrega cada arquivo de resultado individual
    for arquivo in arquivos:
        try:
            with open(arquivo, 'r', encoding='utf-8') as f:
                dados_cliente = json.load(f)
                dados.append(dados_cliente)
        except Exception as e:
            print(f"Erro ao carregar {arquivo}: {e}")
    
    return dados

def extrair_faturamentos(dados):
    """Extrai os dados de faturamento de cada cliente."""
    faturamentos = []
    
    for cliente in dados:
        try:
            # Verifica se é um cliente individual ou lista
            if isinstance(cliente, list):
                # Se for uma lista de clientes
                for c in cliente:
                    fat = c.get('faturamento_ultimos_12_meses', {}).get('faturamento_liquido', 0)
                    if fat > 0:  # Ignora faturamentos nulos ou negativos
                        faturamentos.append({
                            'codigo': c.get('codigo_cliente', 'N/A'),
                            'nome': c.get('nome_completo', 'N/A'),
                            'faturamento': fat
                        })
            else:
                # Cliente individual
                fat = cliente.get('faturamento_ultimos_12_meses', {}).get('faturamento_liquido', 0)
                if fat > 0:  # Ignora faturamentos nulos ou negativos
                    faturamentos.append({
                        'codigo': cliente.get('codigo_cliente', 'N/A'),
                        'nome': cliente.get('nome_completo', 'N/A'),
                        'faturamento': fat
                    })
        except Exception as e:
            print(f"Erro ao extrair faturamento: {e}")
    
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
    percentis = [25, 50, 75, 90, 95, 99]
    valores_percentis = [df['faturamento'].quantile(p/100) for p in percentis]
    
    print("\n=== ANÁLISE DE PERCENTIS ===")
    for p, v in zip(percentis, valores_percentis):
        print(f"Percentil {p}%: R$ {v:.2f}")
    
    # Sugestão baseada em percentis
    print("\n=== SUGESTÃO DE FAIXAS BASEADA EM PERCENTIS ===")
    print(f"Faixa para 10 pontos: R$ {valores_percentis[-2]:.2f} ou mais")
    print(f"Faixa para 8 pontos: R$ {valores_percentis[-3]:.2f} a R$ {valores_percentis[-2]:.2f}")
    print(f"Faixa para 6 pontos: R$ {valores_percentis[-4]:.2f} a R$ {valores_percentis[-3]:.2f}")
    print(f"Faixa para 4 pontos: R$ {valores_percentis[-5]:.2f} a R$ {valores_percentis[-4]:.2f}")
    print(f"Faixa para 2 pontos: Abaixo de R$ {valores_percentis[-5]:.2f}")
    
    # Análise com K-means para encontrar clusters naturais
    try:
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
    except Exception as e:
        print(f"Erro na análise de clusters: {e}")
    
    # Sugestão de configuração para o arquivo .env
    print("\n=== SUGESTÃO DE CONFIGURAÇÃO PARA .ENV ===")
    print("# Com base nos percentis")
    print(f"FATURAMENTO_FAIXA_10={int(valores_percentis[-2])}")
    print(f"FATURAMENTO_FAIXA_8={int(valores_percentis[-3])}")
    print(f"FATURAMENTO_FAIXA_6={int(valores_percentis[-4])}")
    print(f"FATURAMENTO_FAIXA_4={int(valores_percentis[-5])}")
    
    print("\n# Com base nos clusters")
    try:
        print(f"FATURAMENTO_FAIXA_10={int(centroides[4])}")
        print(f"FATURAMENTO_FAIXA_8={int(centroides[3])}")
        print(f"FATURAMENTO_FAIXA_6={int(centroides[2])}")
        print(f"FATURAMENTO_FAIXA_4={int(centroides[1])}")
    except:
        pass
    
    # Tenta criar um histograma para visualização
    try:
        plt.figure(figsize=(10, 6))
        plt.hist(df['faturamento'], bins=30)
        plt.title('Distribuição de Faturamento')
        plt.xlabel('Faturamento (R$)')
        plt.ylabel('Quantidade de Clientes')
        plt.axvline(valores_percentis[-2], color='r', linestyle='--', label=f'95º Percentil: R$ {valores_percentis[-2]:.2f}')
        plt.axvline(valores_percentis[-3], color='g', linestyle='--', label=f'90º Percentil: R$ {valores_percentis[-3]:.2f}')
        plt.axvline(valores_percentis[-4], color='b', linestyle='--', label=f'75º Percentil: R$ {valores_percentis[-4]:.2f}')
        plt.legend()
        plt.savefig('resultados/distribuicao_faturamento.png')
        print("\nHistograma gerado e salvo em 'resultados/distribuicao_faturamento.png'")
    except Exception as e:
        print(f"Não foi possível gerar o histograma: {e}")

def processar_todos_os_clientes():
    """Altera a configuração do .env para processar todos os clientes."""
    # Verifica se o diretório 'resultados' existe
    if not os.path.exists('resultados'):
        os.makedirs('resultados')
        print("Diretório 'resultados' criado")
    
    # Altera o arquivo .env temporariamente
    env_original = None
    try:
        # Faz backup das configurações originais
        with open('.env', 'r', encoding='utf-8') as f:
            env_original = f.read()
        
        # Atualiza para processar todos os clientes
        env_conteudo = env_original.replace('PROCESSAR_TODOS=false', 'PROCESSAR_TODOS=true')
        with open('.env', 'w', encoding='utf-8') as f:
            f.write(env_conteudo)
        
        print("Configuração alterada para processar todos os clientes")
        
        # Executa o script principal
        print("\nExecutando o processamento de todos os clientes...")
        os.system('python main.py')
        
    finally:
        # Restaura a configuração original
        if env_original:
            with open('.env', 'w', encoding='utf-8') as f:
                f.write(env_original)
            print("\nConfigurações originais do .env restauradas")

def main():
    """Função principal."""
    print("=== ANALISADOR DE FAIXAS DE FATURAMENTO ===")
    
    # Verifica se existem dados suficientes para análise
    dados = carregar_resultados()
    if not dados:
        resposta = input("Nenhum conjunto completo de resultados encontrado. Deseja processar todos os clientes agora? (s/n): ")
        if resposta.lower() == 's':
            processar_todos_os_clientes()
            # Recarrega os resultados após o processamento
            dados = carregar_resultados()
        else:
            print("Operação cancelada pelo usuário")
            return
    
    # Extrai e analisa os faturamentos
    faturamentos = extrair_faturamentos(dados)
    analisar_distribuicao(faturamentos)

if __name__ == "__main__":
    main()
