"""
Script para testar um cliente específico definido no arquivo .env
"""
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import json
from bson import json_util
from consultas import (
    obter_data_primeira_compra,
    obter_faturamento_ultimos_12_meses,
    obter_ciclos_compra_ultimos_6_meses,
    obter_total_pecas_compradas,
    obter_titulos_pagos_em_dia,
    obter_valor_por_marca,
    obter_numero_marcas_diferentes
)

# Carrega as variáveis de ambiente
load_dotenv()

# Configuração da conexão com o MongoDB
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")
CLIENTE_TESTE = os.getenv("CLIENTE_TESTE")

def conectar_mongodb():
    """Estabelece conexão com o MongoDB."""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DATABASE]
        print(f"Conexão estabelecida com o banco de dados: {MONGODB_DATABASE}")
        return db
    except Exception as e:
        print(f"Erro ao conectar ao MongoDB: {e}")
        return None

def testar_cliente_especifico():
    """Testa todas as consultas para um cliente específico."""
    db = conectar_mongodb()
    if db is None:
        print("Falha ao conectar ao MongoDB.")
        return
    
    if not CLIENTE_TESTE:
        print("Cliente de teste não definido no arquivo .env")
        return
    
    print(f"Testando cliente: {CLIENTE_TESTE}")
    
    # Obtém os dados do cliente
    cliente = db.geradores.find_one({"cod_cliente": CLIENTE_TESTE})
    if not cliente:
        print(f"Cliente {CLIENTE_TESTE} não encontrado.")
        return
    
    cliente_id = cliente.get("_id")
    nome_cliente = cliente.get("razao_social", "")
    
    print(f"Cliente encontrado: {nome_cliente}")
    print("\nExecutando consultas...")
    
    # Consulta 1: Data da primeira compra
    print("\n1. Data da primeira compra:")
    data_primeira_compra = obter_data_primeira_compra(db, cod_cliente=CLIENTE_TESTE)
    if data_primeira_compra:
        print(f"   Data: {data_primeira_compra.get('data_formatada')}")
        print(f"   Timestamp: {data_primeira_compra.get('data')}")
    else:
        print("   Nenhuma compra encontrada.")
    
    # Consulta 2: Faturamento total nos últimos 12 meses
    print("\n2. Faturamento total nos últimos 12 meses:")
    faturamento = obter_faturamento_ultimos_12_meses(db, cod_cliente=CLIENTE_TESTE)
    if faturamento:
        print(f"   Total de vendas: R$ {faturamento.get('total_vendas', 0):.2f}")
        print(f"   Total de devoluções: R$ {faturamento.get('total_devolucoes', 0):.2f}")
        print(f"   Faturamento líquido: R$ {faturamento.get('faturamento_liquido', 0):.2f}")
    else:
        print("   Nenhum faturamento encontrado.")
    
    # Consulta 3: Número de ciclos em que comprou nos últimos 6 meses
    print("\n3. Ciclos de compra nos últimos 6 meses:")
    ciclos = obter_ciclos_compra_ultimos_6_meses(db, cod_cliente=CLIENTE_TESTE)
    if ciclos:
        print(f"   Número de ciclos: {ciclos.get('num_ciclos_6_meses', 0)}")
        print(f"   Meses de compra: {', '.join(ciclos.get('meses_compra_6_meses', []))}")
        print(f"   Comprou no ciclo atual: {'Sim' if ciclos.get('comprou_ciclo_atual') else 'Não'}")
    else:
        print("   Nenhum ciclo de compra encontrado.")
    
    # Consulta 4: Número total de peças compradas
    print("\n4. Total de peças compradas:")
    pecas = obter_total_pecas_compradas(db, cod_cliente=CLIENTE_TESTE)
    if pecas:
        print(f"   Total bruto: {pecas.get('total_bruto', 0)}")
        print(f"   Total de devoluções: {pecas.get('total_devolucoes', 0)}")
        print(f"   Total líquido: {pecas.get('total_liquido', 0)}")
    else:
        print("   Nenhuma peça comprada encontrada.")
    
    # Consulta 5: Total de pedidos pagos em dia
    print("\n5. Títulos pagos em dia:")
    pagamentos = obter_titulos_pagos_em_dia(db, cod_cliente=CLIENTE_TESTE)
    if pagamentos:
        print(f"   Total de lançamentos: {pagamentos.get('total_lancamentos', 0)}")
        print(f"   Total de pagamentos realizados: {pagamentos.get('total_pagos', 0)} ({pagamentos.get('percentual_pagos_total', 0)}%)")
        print(f"   Total a vencer: {pagamentos.get('total_a_vencer', 0)} ({pagamentos.get('percentual_a_vencer', 0)}%)")
        print(f"   Total vencido: {pagamentos.get('total_vencido', 0)} ({pagamentos.get('percentual_vencido', 0)}%)")
        
        # Exibe informações de inadimplência se o cliente estiver inadimplente
        if pagamentos.get('inadimplente', False):
            print(f"   Inadimplente: True")
            print(f"     - Dias de atraso: {pagamentos.get('inadimplente_dias', 0)}")
            print(f"     - Valor vencido: R$ {pagamentos.get('inadimplente_valor', 0):.2f}")
        else:
            print(f"   Inadimplente: False")
        
        # Exibe valor total a vencer
        print(f"   Valor total a vencer: R$ {pagamentos.get('total_a_vencer_valor', 0):.2f}")
        
        print("   Dos pagamentos realizados:")
        print(f"     - Pagos em dia: {pagamentos.get('pagos_em_dia', 0)} ({pagamentos.get('percentual_pagos_em_dia', 0)}%)")
        print(f"     - Pagos com 1-7 dias de atraso: {pagamentos.get('pagos_em_ate_7d', 0)} ({pagamentos.get('percentual_pagos_em_ate_7d', 0)}%)")
        print(f"     - Pagos com 8-15 dias de atraso: {pagamentos.get('pagos_em_ate_15d', 0)} ({pagamentos.get('percentual_pagos_em_ate_15d', 0)}%)")
        print(f"     - Pagos com 16-30 dias de atraso: {pagamentos.get('pagos_em_ate_30d', 0)} ({pagamentos.get('percentual_pagos_em_ate_30d', 0)}%)")
        print(f"     - Pagos com mais de 30 dias de atraso: {pagamentos.get('pagos_com_mais_30d', 0)} ({pagamentos.get('percentual_pagos_com_mais_30d', 0)}%)")
        print(f"   Usa boleto: {pagamentos.get('usa_boleto', False)}")
        
        # Adiciona informações sobre limite de crédito
        limite_credito = cliente.get("limite_credito", 0)
        limite_credito_utilizado = pagamentos.get("total_a_vencer_valor", 0) + pagamentos.get("inadimplente_valor", 0)
        
        print(f"   Limite de crédito: R$ {limite_credito:.2f}")
        print(f"   Limite utilizado: R$ {limite_credito_utilizado:.2f}")
        print(f"   Disponível: R$ {max(0, limite_credito - limite_credito_utilizado):.2f}")
    else:
        print("   Nenhum pagamento encontrado.")
    
    # Consulta 6: Valor total comprado por marca
    print("\n6. Valor por marca:")
    valor_marcas = obter_valor_por_marca(db, cod_cliente=CLIENTE_TESTE)
    if valor_marcas:
        marcas = valor_marcas.get("valor_por_marca", {})
        print(f"   Total de marcas: {len(marcas)}")
        
        # Ordena as marcas pelo valor líquido (do maior para o menor)
        marcas_ordenadas = sorted(
            marcas.items(),
            key=lambda x: x[1]["valor_liquido"],
            reverse=True
        )
        
        for marca, valores in marcas_ordenadas:
            print(f"   - {marca}: R$ {valores['valor_liquido']:.2f}")
    else:
        print("   Nenhum valor por marca encontrado.")
    
    # Consulta 7: Número de marcas diferentes compradas
    print("\n7. Número de marcas diferentes:")
    marcas = obter_numero_marcas_diferentes(db, cod_cliente=CLIENTE_TESTE)
    if marcas:
        print(f"   Total de marcas: {marcas.get('total_marcas', 0)}")
        print(f"   Marcas: {', '.join(marcas.get('lista_marcas', []))}")
    else:
        print("   Nenhuma marca encontrada.")
    
    # Salva os resultados em um arquivo JSON
    resultado_completo = {
        "id": cliente_id,
        "codigo_cliente": CLIENTE_TESTE,
        "nome_completo": nome_cliente,
        "data_primeira_compra": data_primeira_compra.get("data_formatada") if data_primeira_compra else None,
        "data_primeira_compra_timestamp": data_primeira_compra.get("data") if data_primeira_compra else None,
        "data_ultima_compra_timestamp": data_primeira_compra.get("data_ultima_compra") if data_primeira_compra else None,
        "faturamento_ultimos_12_meses": {
            "total_vendas": faturamento.get("total_vendas", 0) if faturamento else 0,
            "total_devolucoes": faturamento.get("total_devolucoes", 0) if faturamento else 0,
            "faturamento_liquido": faturamento.get("faturamento_liquido", 0) if faturamento else 0
        },
        "ciclos_compra_ultimos_6_meses": ciclos.get("num_ciclos_6_meses", 0) if ciclos else 0,
        "ciclo_atual": ciclos.get("comprou_ciclo_atual", False) if ciclos else False,
        "meses_compra": ciclos.get("meses_compra_6_meses", []) if ciclos else [],
        "total_pecas": {
            "compradas": pecas.get("total_bruto", 0) if pecas else 0,
            "devolvidas": pecas.get("total_devolucoes", 0) if pecas else 0,
            "liquido": pecas.get("total_liquido", 0) if pecas else 0
        },
        "titulos_pagos_em_dia": {
            "total_lancamentos": pagamentos.get("total_lancamentos", 0) if pagamentos else 0,
            "total_pagos": pagamentos.get("total_pagos", 0) if pagamentos else 0,
            "total_a_vencer": pagamentos.get("total_a_vencer", 0) if pagamentos else 0,
            "total_vencido": pagamentos.get("total_vencido", 0) if pagamentos else 0,
            "percentual_pagos_total": pagamentos.get("percentual_pagos_total", 0) if pagamentos else 0,
            "percentual_a_vencer": pagamentos.get("percentual_a_vencer", 0) if pagamentos else 0,
            "percentual_vencido": pagamentos.get("percentual_vencido", 0) if pagamentos else 0,
            "pagos_em_dia": pagamentos.get("pagos_em_dia", 0) if pagamentos else 0,
            "percentual_pagos_em_dia": pagamentos.get("percentual_pagos_em_dia", 0) if pagamentos else 0,
            "pagos_em_ate_7d": pagamentos.get("pagos_em_ate_7d", 0) if pagamentos else 0,
            "percentual_pagos_em_ate_7d": pagamentos.get("percentual_pagos_em_ate_7d", 0) if pagamentos else 0,
            "pagos_em_ate_15d": pagamentos.get("pagos_em_ate_15d", 0) if pagamentos else 0,
            "percentual_pagos_em_ate_15d": pagamentos.get("percentual_pagos_em_ate_15d", 0) if pagamentos else 0,
            "pagos_em_ate_30d": pagamentos.get("pagos_em_ate_30d", 0) if pagamentos else 0,
            "percentual_pagos_em_ate_30d": pagamentos.get("percentual_pagos_em_ate_30d", 0) if pagamentos else 0,
            "pagos_com_mais_30d": pagamentos.get("pagos_com_mais_30d", 0) if pagamentos else 0,
            "percentual_pagos_com_mais_30d": pagamentos.get("percentual_pagos_com_mais_30d", 0) if pagamentos else 0,
            "inadimplente": pagamentos.get("inadimplente", False) if pagamentos else False,
            "inadimplente_dias": pagamentos.get("inadimplente_dias", 0) if pagamentos else 0,
            "inadimplente_valor": pagamentos.get("inadimplente_valor", 0) if pagamentos else 0,
            "total_a_vencer_valor": pagamentos.get("total_a_vencer_valor", 0) if pagamentos else 0,
            "usa_boleto": pagamentos.get("usa_boleto", False) if pagamentos else False,
            "limite_credito": cliente.get("limite_credito", 0),
            "limite_credito_utilizado": pagamentos.get("total_a_vencer_valor", 0) + pagamentos.get("inadimplente_valor", 0)
        },
        "valor_por_marca": valor_marcas.get("valor_por_marca", {}) if valor_marcas else {},
        "numero_marcas_diferentes": marcas.get("total_marcas", 0) if marcas else 0,
        "lista_marcas": marcas.get("lista_marcas", []) if marcas else []
    }
    
    # Salva os resultados em um arquivo JSON
    with open(f'cliente_{CLIENTE_TESTE}_teste.json', 'w', encoding='utf-8') as f:
        json.dump(resultado_completo, f, default=json_util.default, ensure_ascii=False, indent=2)
    
    print(f"\nResultados completos salvos em 'cliente_{CLIENTE_TESTE}_teste.json'")

if __name__ == "__main__":
    testar_cliente_especifico()
