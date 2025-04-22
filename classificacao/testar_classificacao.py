"""
Script para testar a funcionalidade de classificação de clientes.

Este módulo permite testar o algoritmo de classificação com dados artificiais ou reais.
"""
import json
from .classificar import classificar_cliente

def testar_com_cliente_exemplo():
    """
    Testa a classificação com um cliente exemplo fictício.
    """
    # Cliente exemplo com valores fictícios
    cliente_exemplo = {
        "codigo_cliente": "0000001234",
        "nome_completo": "Cliente Exemplo S.A.",
        "faturamento_ultimos_12_meses": {
            "total_vendas": 45000.00,
            "total_devolucoes": 10000.00,
            "faturamento_liquido": 35000.00
        },
        "ciclos_compra_ultimos_6_meses": 5,
        "ciclo_atual": True,
        "titulos_pagos_em_dia": {
            "percentual_pagos_em_dia": 90.0,
            "percentual_pagos_em_ate_7d": 0.0
        },
        "total_pecas": {
            "compradas": 300,
            "devolvidas": 50,
            "liquido": 250
        },
        "numero_marcas_diferentes": 5,
        "lista_marcas": ["Marca A", "Marca B", "Marca C", "Marca D", "Marca E"]
    }
    
    # Classifica o cliente exemplo
    resultado = classificar_cliente(cliente_exemplo)
    
    # Exibe os resultados
    print("\n=== TESTE DE CLASSIFICAÇÃO - CLIENTE EXEMPLO ===")
    print(f"Cliente: {cliente_exemplo['nome_completo']} ({cliente_exemplo['codigo_cliente']})")
    print(f"Pontuação Final: {resultado['pontuacao_final']}")
    print(f"Categoria: {resultado['categoria']}")
    print("\nPontuações por Critério:")
    
    for criterio, dados in resultado['pontuacoes_criterios'].items():
        print(f"- {criterio.capitalize()}: {dados['pontuacao']} (peso: {dados['peso']}) → {dados['ponderado']} pontos")
    
    print("\nDetalhes da Categoria:")
    print(f"- {resultado['detalhes']['descricao_categoria']}")
    
    print("\nSugestões:")
    for sugestao in resultado['detalhes']['sugestoes']:
        print(f"- {sugestao}")
    
    return resultado

def testar_com_cliente_real(arquivo_json):
    """
    Testa a classificação com dados reais de um cliente.
    
    Args:
        arquivo_json: Caminho para um arquivo JSON com dados de um cliente real
    """
    try:
        # Carrega os dados do cliente do arquivo JSON
        with open(arquivo_json, 'r', encoding='utf-8') as file:
            cliente_real = json.load(file)
        
        # Classifica o cliente
        resultado = classificar_cliente(cliente_real)
        
        # Exibe os resultados
        print(f"\n=== TESTE DE CLASSIFICAÇÃO - CLIENTE REAL ({arquivo_json}) ===")
        print(f"Cliente: {cliente_real.get('nome_completo', 'N/A')} ({cliente_real.get('codigo_cliente', 'N/A')})")
        print(f"Pontuação Final: {resultado['pontuacao_final']}")
        print(f"Categoria: {resultado['categoria']}")
        
        print("\nPontuações por Critério:")
        for criterio, dados in resultado['pontuacoes_criterios'].items():
            print(f"- {criterio.capitalize()}: {dados['pontuacao']} (peso: {dados['peso']}) → {dados['ponderado']} pontos")
        
        print("\nDetalhes da Categoria:")
        print(f"- {resultado['detalhes']['descricao_categoria']}")
        
        print("\nSugestões:")
        for sugestao in resultado['detalhes']['sugestoes']:
            print(f"- {sugestao}")
        
        return resultado
        
    except Exception as e:
        print(f"Erro ao testar com cliente real: {e}")
        return None

if __name__ == "__main__":
    # Testa com cliente exemplo
    resultado_exemplo = testar_com_cliente_exemplo()
    
    # Arquivo de exemplo para teste com dados reais
    # Substitua pelo caminho de um arquivo JSON válido
    arquivo_exemplo = "../resultados/resultado_0000000826_20250421_185912.json"
    
    try:
        resultado_real = testar_com_cliente_real(arquivo_exemplo)
    except Exception as e:
        print(f"Não foi possível testar com arquivo exemplo: {e}")
