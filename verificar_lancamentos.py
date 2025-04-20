"""
Script para verificar os lançamentos de um cliente específico
"""
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import json
from bson import json_util

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

def verificar_lancamentos():
    """Verifica os lançamentos de um cliente específico."""
    db = conectar_mongodb()
    if db is None:
        print("Falha ao conectar ao MongoDB.")
        return
    
    if not CLIENTE_TESTE:
        print("Cliente de teste não definido no arquivo .env")
        return
    
    print(f"Verificando lançamentos para o cliente: {CLIENTE_TESTE}")
    
    # Obtém os dados do cliente
    cliente = db.geradores.find_one({"cod_cliente": CLIENTE_TESTE})
    if not cliente:
        print(f"Cliente {CLIENTE_TESTE} não encontrado.")
        return
    
    nome_cliente = cliente.get("razao_social", "")
    print(f"Cliente encontrado: {nome_cliente}")
    
    # Busca todos os lançamentos do cliente (sem filtros)
    lancamentos = list(db.lancamentos.find({"cod_gerador": CLIENTE_TESTE}))
    
    print(f"\nTotal de lançamentos encontrados: {len(lancamentos)}")
    
    if not lancamentos:
        print("Nenhum lançamento encontrado para este cliente.")
        return
    
    # Conta os tipos de lançamentos
    tipos = {}
    tipos_pgto = {}
    
    for lancamento in lancamentos:
        tipo = lancamento.get("tipo", "Não definido")
        if tipo not in tipos:
            tipos[tipo] = 0
        tipos[tipo] += 1
        
        tipo_pgto = lancamento.get("tipo_pgto_descricao", "Não definido")
        if tipo_pgto not in tipos_pgto:
            tipos_pgto[tipo_pgto] = 0
        tipos_pgto[tipo_pgto] += 1
    
    print("\nTipos de lançamentos:")
    for tipo, quantidade in tipos.items():
        print(f"  - {tipo}: {quantidade}")
    
    print("\nTipos de pagamento:")
    for tipo, quantidade in tipos_pgto.items():
        print(f"  - {tipo}: {quantidade}")
    
    # Verifica os lançamentos de boletos
    boletos = [
        "BOLETO",
        "BOLETO BANCO DO BRASIL TBS",
        "BOLETO BRADESCO TBS",
        "BOLETO CAIXA TBS",
        "BOLETO ITAU TBS"
    ]
    
    lancamentos_boleto = [l for l in lancamentos if l.get("tipo_pgto_descricao") in boletos]
    
    print(f"\nLançamentos de boletos: {len(lancamentos_boleto)}")
    
    if lancamentos_boleto:
        print("\nDetalhes dos lançamentos de boleto:")
        for i, lancamento in enumerate(lancamentos_boleto[:5], 1):  # Mostra apenas os 5 primeiros
            print(f"\nLançamento {i}:")
            print(f"  Número: {lancamento.get('lancamento')}")
            print(f"  Tipo: {lancamento.get('tipo')}")
            print(f"  Título: {lancamento.get('titulo')}")
            print(f"  Tipo de pagamento: {lancamento.get('tipo_pgto_descricao')}")
            print(f"  Data de vencimento: {lancamento.get('data_vencimento_')}")
            print(f"  Data de pagamento: {lancamento.get('data_pagamento_')}")
    
    # Salva os resultados em um arquivo JSON
    with open(f'lancamentos_{CLIENTE_TESTE}.json', 'w', encoding='utf-8') as f:
        json.dump(lancamentos, f, default=json_util.default, ensure_ascii=False, indent=2)
    
    print(f"\nTodos os lançamentos salvos em 'lancamentos_{CLIENTE_TESTE}.json'")

if __name__ == "__main__":
    verificar_lancamentos()
