"""
Funções base e utilitárias para as consultas.
"""
import os
import json
import time
import traceback
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import json_util

# Lista de eventos de venda válidos usados em várias consultas
EVENTOS_VENDA = [
            "S12 - PA - FAT.  TROCA SEMIJOIAS - FIS - EST",
            "S01 - PA - FAT. PEDIDO DE VENDA CONF - FIS - EST- FIN",
            "S22 - PA - FAT. PEDIDO DE VENDA COSMET - FIS - EST- FIN",
            "S02 - PA - FAT. PEDIDO VENDA SEMIJOIAS - FIS - EST- FIN",
            "S11 - PA - FAT. TROCA CONF - FIS - EST",
            "S13 - PA - FAT. TROCA COSMETICO - FIS - EST",
            "S03 - PA - FATURAMENTO PED. FUNCIONARIO - FIS - EST- FIN",
            "S05 - PA - RETORNO / VENDA CONSIGNADO CONF.",
            "S18 - PA - RETORNO / VENDA CONSIGNADO COSMETICO",
            "S16 - PA - RETORNO / VENDA CONSIGNADO SEMIJOIAS"
]

# Lista de eventos de devolução válidos
EVENTOS_DEVOLUCAO = [
            "E09 - PA - DEVOLUÇÃO DE VENDA",
            "E12 - PA - DEVOLUÇÃO TROCA - SISTEMA ANTIGO"
]

def verificar_cliente_tem_movimentacao(db, cod_cliente):
    """
    Verifica se um cliente tem movimentações de venda.
    
    Args:
        db: Conexão com o banco de dados
        cod_cliente: Código do cliente
        
    Returns:
        True se o cliente tem movimentações de venda, False caso contrário
    """
    # Filtro para vendas do cliente
    filtro_venda = {
        "codigo_cliente_fornecedor": cod_cliente,
        "evento": {"$in": EVENTOS_VENDA}
    }
    
    # Conta o número de movimentações de venda do cliente
    count_movimentacoes = db.movimentacao.count_documents(filtro_venda)
    
    return count_movimentacoes > 0

def obter_clientes_com_movimentacao(db):
    """
    Obtém a lista de códigos de clientes que têm movimentações de venda.
    
    Args:
        db: Conexão com o banco de dados
        
    Returns:
        Conjunto de códigos de clientes com movimentações de venda
    """
    try:
        # Primeiro, obtém a lista completa de códigos de clientes da collection geradores_cod_cliente
        documento = db.geradores_cod_cliente.find_one({})
        if documento and "codigo_cliente_fornecedor" in documento:
            todos_codigos_clientes = documento["codigo_cliente_fornecedor"]
            
            # Executa o pipeline de agregação para encontrar quais clientes têm movimentações de venda
            pipeline = [
                {"$match": {"codigo_cliente_fornecedor": {"$in": todos_codigos_clientes}, 
                            "evento": {"$in": EVENTOS_VENDA}}},
                {"$group": {"_id": "$codigo_cliente_fornecedor"}},
                {"$project": {"codigo_cliente": "$_id", "_id": 0}}
            ]
            
            resultado = list(db.movimentacao.aggregate(pipeline))
            clientes_com_movimentacao = set(doc.get("codigo_cliente") for doc in resultado if doc.get("codigo_cliente"))
            
            print(f"Encontrados {len(clientes_com_movimentacao)} clientes com movimentações de venda.")
            return clientes_com_movimentacao
            
    except Exception as e:
        print(f"Erro ao processar lista de clientes com movimentação: {e}")
        print("Utilizando método alternativo para obter clientes com movimentação...")
    
    # Método alternativo: caso haja algum problema 
    pipeline = [
        {"$match": {"evento": {"$in": EVENTOS_VENDA}}},
        {"$group": {"_id": "$codigo_cliente_fornecedor"}},
        {"$project": {"codigo_cliente_fornecedor": "$_id", "_id": 0}}
    ]
    
    clientes_com_movimentacao = list(db.movimentacao.aggregate(pipeline))
    return set(doc.get("codigo_cliente_fornecedor") for doc in clientes_com_movimentacao if doc.get("codigo_cliente_fornecedor"))
