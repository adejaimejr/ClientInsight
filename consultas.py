"""
Sistema de Extração de Dados do ERP - Implementação das Consultas
Este módulo contém as implementações das consultas específicas para extração de dados do MongoDB.
"""
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime, timedelta
import json
from bson import json_util

# Carrega as variáveis de ambiente
load_dotenv()

# Configuração da conexão com o MongoDB
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")

# Configurações de processamento
CLIENTE_TESTE = os.getenv("CLIENTE_TESTE")
PROCESSAR_TODOS = os.getenv("PROCESSAR_TODOS", "false").lower() == "true"
TAMANHO_LOTE = int(os.getenv("TAMANHO_LOTE", "20"))
USAR_CACHE = os.getenv("USAR_CACHE", "true").lower() == "true"

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

def obter_codigo_cliente(db, cliente_id=None):
    """
    Obtém o código do cliente.
    
    Args:
        db: Conexão com o banco de dados
        cliente_id: ID do cliente (opcional)
        
    Returns:
        Lista de códigos de clientes ou código específico se cliente_id for fornecido
    """
    try:
        # Se cliente_id for fornecido, busca apenas esse cliente
        if cliente_id:
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente and "cod_cliente" in cliente:
                return cliente["cod_cliente"]
            return None
        
        # Se cliente_id não for fornecido, busca todos os clientes na collection geradores
        clientes = list(db.geradores.find(
            {"cod_cliente": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Formata os resultados
        codigos_clientes = []
        for cliente in clientes:
            codigos_clientes.append({
                "id": cliente.get("_id"),
                "codigo": cliente.get("cod_cliente"),
                "nome": cliente.get("razao_social", "")
            })
        
        return codigos_clientes
    
    except Exception as e:
        print(f"Erro ao obter código do cliente: {e}")
        return None

def obter_nome_completo(db, cliente_id=None):
    """
    Obtém o nome completo do cliente.
    
    Args:
        db: Conexão com o banco de dados
        cliente_id: ID do cliente (opcional)
        
    Returns:
        Nome completo do cliente ou lista de nomes se cliente_id não for fornecido
    """
    try:
        # Se cliente_id for fornecido, busca apenas esse cliente
        if cliente_id:
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente and "razao_social" in cliente:
                return cliente["razao_social"]
            return None
        
        # Se cliente_id não for fornecido, busca todos os clientes na collection geradores
        clientes = list(db.geradores.find(
            {"razao_social": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Formata os resultados
        nomes_clientes = []
        for cliente in clientes:
            nomes_clientes.append({
                "id": cliente.get("_id"),
                "codigo": cliente.get("cod_cliente"),
                "nome_completo": cliente.get("razao_social", "")
            })
        
        return nomes_clientes
    
    except Exception as e:
        print(f"Erro ao obter nome completo do cliente: {e}")
        return None

def obter_data_primeira_compra(db, cliente_id=None, cod_cliente=None):
    """
    Obtém a data da primeira compra do cliente.
    
    Args:
        db: Conexão com o banco de dados
        cliente_id: ID do cliente (opcional)
        cod_cliente: Código do cliente (opcional)
        
    Returns:
        Data da primeira compra ou lista de datas se cliente_id/cod_cliente não for fornecido
    """
    try:
        # Lista de eventos de venda válidos
        eventos_venda = [
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
        
        # Filtro base para vendas
        filtro_base = {
            "evento": {"$in": eventos_venda},
            "tipo_operacao": "S",
            "cancelada": False,
            "data": {"$exists": True, "$ne": None}
        }
        
        # Se cliente_id for fornecido, busca o código do cliente
        if cliente_id and not cod_cliente:
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente and "cod_cliente" in cliente:
                cod_cliente = cliente["cod_cliente"]
        
        # Se cod_cliente for fornecido, busca apenas para esse cliente
        if cod_cliente:
            # Adiciona o código do cliente ao filtro
            filtro_base["codigo_cliente_fornecedor"] = cod_cliente
            
            # Busca as vendas do cliente, ordenadas por data (ascendente para primeira compra)
            primeira_compra = list(db.movimentacao.find(filtro_base).sort("data", 1).limit(1))
            
            # Busca as vendas do cliente, ordenadas por data (descendente para última compra)
            ultima_compra = list(db.movimentacao.find(filtro_base).sort("data", -1).limit(1))
            
            if primeira_compra:
                return {
                    "codigo_cliente": cod_cliente,
                    "data": primeira_compra[0].get("data"),
                    "data_formatada": primeira_compra[0].get("data_"),
                    "data_ultima_compra": ultima_compra[0].get("data") if ultima_compra else None
                }
            
            return None
        
        # Se cliente_id/cod_cliente não for fornecido, busca para todos os clientes
        # Primeiro, obtém a lista de todos os clientes
        clientes = list(db.geradores.find(
            {"cod_cliente": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Para cada cliente, busca a data da primeira compra
        resultados = []
        
        # Processa todos os clientes sem limitação
        for cliente in clientes:
            cod_cliente = cliente.get("cod_cliente")
            if not cod_cliente:
                continue
            
            # Adiciona o código do cliente ao filtro
            filtro_cliente = {**filtro_base, "codigo_cliente_fornecedor": cod_cliente}
            
            # Busca as vendas do cliente, ordenadas por data (ascendente para primeira compra)
            primeira_compra = list(db.movimentacao.find(filtro_cliente).sort("data", 1).limit(1))
            
            # Busca as vendas do cliente, ordenadas por data (descendente para última compra)
            ultima_compra = list(db.movimentacao.find(filtro_cliente).sort("data", -1).limit(1))
            
            if not primeira_compra:
                continue
            
            resultados.append({
                "id": cliente.get("_id"),
                "codigo_cliente": cod_cliente,
                "nome": cliente.get("razao_social", ""),
                "data": primeira_compra[0].get("data"),
                "data_formatada": primeira_compra[0].get("data_"),
                "data_ultima_compra": ultima_compra[0].get("data") if ultima_compra else None
            })
        
        return resultados
    
    except Exception as e:
        print(f"Erro ao obter data da primeira compra: {e}")
        return None

def obter_faturamento_ultimos_12_meses(db, cliente_id=None, cod_cliente=None):
    """
    Calcula o faturamento total nos últimos 12 meses.
    
    Args:
        db: Conexão com o banco de dados
        cliente_id: ID do cliente (opcional)
        cod_cliente: Código do cliente (opcional)
        
    Returns:
        Valor total faturado nos últimos 12 meses ou lista de faturamentos se cliente_id/cod_cliente não for fornecido
    """
    try:
        # Calcula a data de 12 meses atrás
        data_atual = datetime.now()
        data_12_meses_atras = int((data_atual - timedelta(days=365)).timestamp())
        
        # Lista de eventos de venda válidos
        eventos_venda = [
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
        
        # Lista de eventos de devolução
        eventos_devolucao = [
            "E09 - PA - DEVOLUÇÃO DE VENDA",
            "E12 - PA - DEVOLUÇÃO TROCA - SISTEMA ANTIGO"
        ]
        
        # Filtro base para eventos de venda
        filtro_venda = {
            "evento": {"$in": eventos_venda},
            "tipo_operacao": "S",
            "cancelada": False,
            "data": {"$gte": data_12_meses_atras}
        }
        
        # Filtro base para eventos de devolução
        filtro_devolucao = {
            "evento": {"$in": eventos_devolucao},
            "tipo_operacao": "E",
            "cancelada": False,
            "data": {"$gte": data_12_meses_atras}
        }
        
        # Se cliente_id for fornecido, busca o código do cliente
        if cliente_id and not cod_cliente:
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente and "cod_cliente" in cliente:
                cod_cliente = cliente["cod_cliente"]
        
        # Se cod_cliente for fornecido, calcula o faturamento apenas para esse cliente
        if cod_cliente:
            # Adiciona o código do cliente aos filtros
            filtro_venda["codigo_cliente_fornecedor"] = cod_cliente
            filtro_devolucao["codigo_cliente_fornecedor"] = cod_cliente
            
            # Calcula o valor total de vendas
            vendas = db.movimentacao.find(filtro_venda)
            total_vendas = sum(venda.get("valor_final", 0) for venda in vendas)
            
            # Calcula o valor total de devoluções
            devolucoes = db.movimentacao.find(filtro_devolucao)
            total_devolucoes = sum(devolucao.get("valor_final", 0) for devolucao in devolucoes)
            
            # Calcula o faturamento líquido
            faturamento_liquido = total_vendas - total_devolucoes
            
            return {
                "codigo_cliente": cod_cliente,
                "total_vendas": total_vendas,
                "total_devolucoes": total_devolucoes,
                "faturamento_liquido": faturamento_liquido
            }
        
        # Se cliente_id/cod_cliente não for fornecido, calcula para todos os clientes
        # Primeiro, obtém a lista de todos os clientes
        clientes = list(db.geradores.find(
            {"cod_cliente": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Para cada cliente, calcula o faturamento
        resultados = []
        
        # Processa todos os clientes sem limitação
        for cliente in clientes:
            cod_cliente = cliente.get("cod_cliente")
            if not cod_cliente:
                continue
            
            # Adiciona o código do cliente aos filtros
            filtro_venda_cliente = {**filtro_venda, "codigo_cliente_fornecedor": cod_cliente}
            filtro_devolucao_cliente = {**filtro_devolucao, "codigo_cliente_fornecedor": cod_cliente}
            
            # Calcula o valor total de vendas
            vendas = db.movimentacao.find(filtro_venda_cliente)
            total_vendas = sum(venda.get("valor_final", 0) for venda in vendas)
            
            # Se não houver vendas, pula para o próximo cliente
            if total_vendas == 0:
                continue
            
            # Calcula o valor total de devoluções
            devolucoes = db.movimentacao.find(filtro_devolucao_cliente)
            total_devolucoes = sum(devolucao.get("valor_final", 0) for devolucao in devolucoes)
            
            # Calcula o faturamento líquido
            faturamento_liquido = total_vendas - total_devolucoes
            
            resultados.append({
                "id": cliente.get("_id"),
                "codigo_cliente": cod_cliente,
                "nome": cliente.get("razao_social", ""),
                "total_vendas": total_vendas,
                "total_devolucoes": total_devolucoes,
                "faturamento_liquido": faturamento_liquido
            })
        
        # Ordena os resultados pelo faturamento líquido (do maior para o menor)
        resultados.sort(key=lambda x: x.get("faturamento_liquido", 0), reverse=True)
        
        return resultados
    
    except Exception as e:
        print(f"Erro ao calcular faturamento: {e}")
        return None

def obter_ciclos_compra_ultimos_6_meses(db, cliente_id=None, cod_cliente=None):
    """
    Calcula o número de ciclos (meses) em que o cliente comprou nos últimos 6 meses.
    
    Args:
        db: Conexão com o banco de dados
        cliente_id: ID do cliente (opcional)
        cod_cliente: Código do cliente (opcional)
        
    Returns:
        Número de ciclos de compra ou lista de ciclos se cliente_id/cod_cliente não for fornecido
    """
    try:
        # Calcula os limites dos últimos 6 meses (excluindo o mês atual)
        data_atual = datetime.now()
        
        # Primeiro dia do mês atual
        primeiro_dia_mes_atual = datetime(data_atual.year, data_atual.month, 1)
        
        # Último dia do mês anterior
        ultimo_dia_mes_anterior = primeiro_dia_mes_atual - timedelta(days=1)
        
        # Primeiro dia de 6 meses atrás (contando a partir do mês anterior)
        primeiro_dia_6_meses_atras = ultimo_dia_mes_anterior - timedelta(days=1)
        for _ in range(5):  # 5 meses anteriores + o mês anterior = 6 meses
            primeiro_dia_6_meses_atras = datetime(
                primeiro_dia_6_meses_atras.year,
                primeiro_dia_6_meses_atras.month, 
                1
            ) - timedelta(days=1)
        primeiro_dia_6_meses_atras = datetime(
            primeiro_dia_6_meses_atras.year,
            primeiro_dia_6_meses_atras.month,
            1
        )
        
        # Converte para timestamp
        timestamp_inicio_6_meses = int(primeiro_dia_6_meses_atras.timestamp())
        timestamp_fim_6_meses = int(primeiro_dia_mes_atual.timestamp())
        
        # Timestamp para o ciclo atual (mês atual)
        timestamp_inicio_ciclo_atual = int(primeiro_dia_mes_atual.timestamp())
        timestamp_fim_ciclo_atual = int((datetime.now() + timedelta(days=1)).timestamp())  # Até hoje
        
        # Lista de eventos de venda válidos
        eventos_venda = [
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
        
        # Filtro base para eventos de venda dos últimos 6 meses (excluindo o mês atual)
        filtro_base_6_meses = {
            "evento": {"$in": eventos_venda},
            "tipo_operacao": "S",
            "cancelada": False,
            "data": {"$gte": timestamp_inicio_6_meses, "$lt": timestamp_fim_6_meses}
        }
        
        # Filtro base para eventos de venda do ciclo atual (mês atual)
        filtro_base_ciclo_atual = {
            "evento": {"$in": eventos_venda},
            "tipo_operacao": "S",
            "cancelada": False,
            "data": {"$gte": timestamp_inicio_ciclo_atual, "$lt": timestamp_fim_ciclo_atual}
        }
        
        # Se cliente_id for fornecido, busca o código do cliente
        if cliente_id and not cod_cliente:
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente and "cod_cliente" in cliente:
                cod_cliente = cliente["cod_cliente"]
        
        # Se cod_cliente for fornecido, calcula os ciclos apenas para esse cliente
        if cod_cliente:
            # Filtros para os últimos 6 meses
            filtro_6_meses = {**filtro_base_6_meses, "codigo_cliente_fornecedor": cod_cliente}
            
            # Busca todas as compras do cliente nos últimos 6 meses (excluindo o mês atual)
            compras_6_meses = list(db.movimentacao.find(filtro_6_meses, {"data": 1, "data_": 1}))
            
            # Extrai os meses distintos das compras dos últimos 6 meses
            meses_compra_6_meses = set()
            for compra in compras_6_meses:
                data_str = compra.get("data_", "")
                if data_str:
                    try:
                        # Formato esperado: "YYYY-MM-DD"
                        ano_mes = data_str[:7]  # Extrai "YYYY-MM"
                        meses_compra_6_meses.add(ano_mes)
                    except Exception:
                        pass
            
            # Conta o número de meses distintos nos últimos 6 meses
            num_ciclos_6_meses = len(meses_compra_6_meses)
            
            # Lista os meses para referência
            meses_lista_6_meses = sorted(list(meses_compra_6_meses))
            
            # Verifica se o cliente comprou no ciclo atual (mês atual)
            filtro_ciclo_atual = {**filtro_base_ciclo_atual, "codigo_cliente_fornecedor": cod_cliente}
            compras_ciclo_atual = list(db.movimentacao.find(filtro_ciclo_atual, {"data": 1, "data_": 1}))
            comprou_ciclo_atual = len(compras_ciclo_atual) > 0
            
            # Se comprou no ciclo atual, obtém o mês atual para referência
            mes_ciclo_atual = None
            if comprou_ciclo_atual and compras_ciclo_atual:
                data_str = compras_ciclo_atual[0].get("data_", "")
                if data_str:
                    try:
                        mes_ciclo_atual = data_str[:7]  # Extrai "YYYY-MM"
                    except Exception:
                        pass
            
            return {
                "codigo_cliente": cod_cliente,
                "num_ciclos_6_meses": num_ciclos_6_meses,
                "meses_compra_6_meses": meses_lista_6_meses,
                "comprou_ciclo_atual": comprou_ciclo_atual,
                "mes_ciclo_atual": mes_ciclo_atual
            }
        
        # Se cliente_id/cod_cliente não for fornecido, calcula para todos os clientes
        # Primeiro, obtém a lista de todos os clientes
        clientes = list(db.geradores.find(
            {"cod_cliente": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Para cada cliente, calcula o número de ciclos
        resultados = []
        
        # Processa todos os clientes sem limitação
        for cliente in clientes:
            cod_cliente = cliente.get("cod_cliente")
            if not cod_cliente:
                continue
            
            # Filtros para os últimos 6 meses
            filtro_6_meses = {**filtro_base_6_meses, "codigo_cliente_fornecedor": cod_cliente}
            
            # Busca todas as compras do cliente nos últimos 6 meses (excluindo o mês atual)
            compras_6_meses = list(db.movimentacao.find(filtro_6_meses, {"data": 1, "data_": 1}))
            
            # Se não houver compras nos últimos 6 meses, verifica se há no ciclo atual
            if not compras_6_meses:
                filtro_ciclo_atual = {**filtro_base_ciclo_atual, "codigo_cliente_fornecedor": cod_cliente}
                compras_ciclo_atual = list(db.movimentacao.find(filtro_ciclo_atual, {"data": 1, "data_": 1}))
                
                # Se não houver compras nem nos últimos 6 meses nem no ciclo atual, pula para o próximo cliente
                if not compras_ciclo_atual:
                    continue
            
            # Extrai os meses distintos das compras dos últimos 6 meses
            meses_compra_6_meses = set()
            for compra in compras_6_meses:
                data_str = compra.get("data_", "")
                if data_str:
                    try:
                        # Formato esperado: "YYYY-MM-DD"
                        ano_mes = data_str[:7]  # Extrai "YYYY-MM"
                        meses_compra_6_meses.add(ano_mes)
                    except Exception:
                        pass
            
            # Conta o número de meses distintos nos últimos 6 meses
            num_ciclos_6_meses = len(meses_compra_6_meses)
            
            # Lista os meses para referência
            meses_lista_6_meses = sorted(list(meses_compra_6_meses))
            
            # Verifica se o cliente comprou no ciclo atual (mês atual)
            filtro_ciclo_atual = {**filtro_base_ciclo_atual, "codigo_cliente_fornecedor": cod_cliente}
            compras_ciclo_atual = list(db.movimentacao.find(filtro_ciclo_atual, {"data": 1, "data_": 1}))
            comprou_ciclo_atual = len(compras_ciclo_atual) > 0
            
            # Se comprou no ciclo atual, obtém o mês atual para referência
            mes_ciclo_atual = None
            if comprou_ciclo_atual and compras_ciclo_atual:
                data_str = compras_ciclo_atual[0].get("data_", "")
                if data_str:
                    try:
                        mes_ciclo_atual = data_str[:7]  # Extrai "YYYY-MM"
                    except Exception:
                        pass
            
            resultados.append({
                "id": cliente.get("_id"),
                "codigo_cliente": cod_cliente,
                "nome": cliente.get("razao_social", ""),
                "num_ciclos_6_meses": num_ciclos_6_meses,
                "meses_compra_6_meses": meses_lista_6_meses,
                "comprou_ciclo_atual": comprou_ciclo_atual,
                "mes_ciclo_atual": mes_ciclo_atual
            })
        
        # Ordena os resultados pelo número de ciclos (do maior para o menor)
        resultados.sort(key=lambda x: x.get("num_ciclos_6_meses", 0), reverse=True)
        
        return resultados
    
    except Exception as e:
        print(f"Erro ao calcular ciclos de compra: {e}")
        import traceback
        traceback.print_exc()
        return None

def obter_total_pecas_compradas(db, cliente_id=None, cod_cliente=None):
    """
    Calcula o número total de peças compradas pelo cliente.
    
    Args:
        db: Conexão com o banco de dados
        cliente_id: ID do cliente (opcional)
        cod_cliente: Código do cliente (opcional)
        
    Returns:
        Número total de peças compradas ou lista de totais se cliente_id/cod_cliente não for fornecido
    """
    try:
        # Lista de eventos de venda válidos
        eventos_venda = [
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
        eventos_devolucao = [
            "E09 - PA - DEVOLUÇÃO DE VENDA",
            "E12 - PA - DEVOLUÇÃO TROCA - SISTEMA ANTIGO"
        ]
        
        # Filtro base para vendas
        filtro_venda_base = {
            "evento": {"$in": eventos_venda},
            "tipo_operacao": "S",
            "cancelada": False,
            "qtde": {"$exists": True}  # Garante que o campo qtde existe
        }
        
        # Filtro base para devoluções
        filtro_devolucao_base = {
            "evento": {"$in": eventos_devolucao},
            "tipo_operacao": "E",
            "cancelada": False,
            "qtde": {"$exists": True}  # Garante que o campo qtde existe
        }
        
        # Se cliente_id for fornecido, busca o código do cliente
        if cliente_id and not cod_cliente:
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente and "cod_cliente" in cliente:
                cod_cliente = cliente["cod_cliente"]
        
        # Se cod_cliente for fornecido, calcula o total apenas para esse cliente
        if cod_cliente:
            # Adiciona o código do cliente aos filtros
            filtro_venda = {**filtro_venda_base, "codigo_cliente_fornecedor": cod_cliente}
            filtro_devolucao = {**filtro_devolucao_base, "codigo_cliente_fornecedor": cod_cliente}
            
            # Busca todas as compras do cliente
            vendas = list(db.movimentacao.find(filtro_venda, {"qtde": 1}))
            
            # Calcula o total de peças compradas
            total_pecas_compradas = sum(venda.get("qtde", 0) for venda in vendas)
            
            # Busca todas as devoluções do cliente
            devolucoes = list(db.movimentacao.find(filtro_devolucao, {"qtde": 1}))
            
            # Calcula o total de peças devolvidas
            total_pecas_devolvidas = sum(devolucao.get("qtde", 0) for devolucao in devolucoes)
            
            # Calcula o total líquido
            total_pecas_liquido = total_pecas_compradas - total_pecas_devolvidas
            
            # Formata o resultado
            return {
                "codigo_cliente": cod_cliente,
                "total_bruto": total_pecas_compradas,
                "total_devolucoes": total_pecas_devolvidas,
                "total_liquido": total_pecas_liquido
            }
        
        # Se cliente_id/cod_cliente não for fornecido, calcula para todos os clientes
        # Primeiro, obtém a lista de todos os clientes
        clientes = list(db.geradores.find(
            {"cod_cliente": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Para cada cliente, calcula o total de peças
        resultados = []
        
        # Processa todos os clientes sem limitação
        for cliente in clientes:
            cod_cliente = cliente.get("cod_cliente")
            if not cod_cliente:
                continue
            
            # Adiciona o código do cliente aos filtros
            filtro_venda_cliente = {**filtro_venda_base, "codigo_cliente_fornecedor": cod_cliente}
            filtro_devolucao_cliente = {**filtro_devolucao_base, "codigo_cliente_fornecedor": cod_cliente}
            
            # Busca todas as compras do cliente
            vendas = list(db.movimentacao.find(filtro_venda_cliente, {"qtde": 1}))
            
            # Calcula o total de peças compradas
            total_pecas_compradas = sum(venda.get("qtde", 0) for venda in vendas)
            
            # Se não houver compras, pula para o próximo cliente
            if total_pecas_compradas == 0:
                continue
            
            # Busca todas as devoluções do cliente
            devolucoes = list(db.movimentacao.find(filtro_devolucao_cliente, {"qtde": 1}))
            
            # Calcula o total de peças devolvidas
            total_pecas_devolvidas = sum(devolucao.get("qtde", 0) for devolucao in devolucoes)
            
            # Calcula o total líquido
            total_pecas_liquido = total_pecas_compradas - total_pecas_devolvidas
            
            resultados.append({
                "id": cliente.get("_id"),
                "codigo_cliente": cod_cliente,
                "nome": cliente.get("razao_social", ""),
                "total_bruto": total_pecas_compradas,
                "total_devolucoes": total_pecas_devolvidas,
                "total_liquido": total_pecas_liquido
            })
        
        # Ordena os resultados pelo total líquido de peças (do maior para o menor)
        resultados.sort(key=lambda x: x.get("total_liquido", 0), reverse=True)
        
        return resultados
    
    except Exception as e:
        print(f"Erro ao calcular total de peças compradas: {e}")
        import traceback
        traceback.print_exc()
        return None

def obter_valor_por_marca(db, cliente_id=None, cod_cliente=None):
    """
    Calcula o valor total comprado por marca.
    
    Args:
        db: Conexão com o banco de dados
        cliente_id: ID do cliente (opcional)
        cod_cliente: Código do cliente (opcional)
        
    Returns:
        Dicionário com valores por marca ou lista de dicionários se cliente_id/cod_cliente não for fornecido
    """
    try:
        # Lista de eventos de venda válidos
        eventos_venda = [
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
        eventos_devolucao = [
            "E09 - PA - DEVOLUÇÃO DE VENDA",
            "E12 - PA - DEVOLUÇÃO TROCA - SISTEMA ANTIGO"
        ]
        
        # Filtro base para vendas
        filtro_venda_base = {
            "evento": {"$in": eventos_venda},
            "tipo_operacao": "S",
            "cancelada": False,
            "marca": {"$exists": True, "$ne": None}
        }
        
        # Filtro base para devoluções
        filtro_devolucao_base = {
            "evento": {"$in": eventos_devolucao},
            "tipo_operacao": "E",
            "cancelada": False,
            "marca": {"$exists": True, "$ne": None}
        }
        
        # Se cliente_id for fornecido, busca o código do cliente
        if cliente_id and not cod_cliente:
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente and "cod_cliente" in cliente:
                cod_cliente = cliente["cod_cliente"]
        
        # Se cod_cliente for fornecido, calcula o valor por marca apenas para esse cliente
        if cod_cliente:
            # Adiciona o código do cliente aos filtros
            filtro_venda = {**filtro_venda_base, "codigo_cliente_fornecedor": cod_cliente}
            filtro_devolucao = {**filtro_devolucao_base, "codigo_cliente_fornecedor": cod_cliente}
            
            # Busca as vendas do cliente
            vendas = db.movimentacao.find(filtro_venda)
            
            # Inicializa o dicionário para armazenar os valores por marca
            valor_por_marca = {}
            
            # Processa as vendas
            for venda in vendas:
                marca = venda.get("marca", "Sem Marca")
                valor = venda.get("valor_final", 0)
                
                if marca not in valor_por_marca:
                    valor_por_marca[marca] = {
                        "valor_vendas": 0,
                        "valor_devolucoes": 0,
                        "valor_liquido": 0
                    }
                
                valor_por_marca[marca]["valor_vendas"] += valor
                valor_por_marca[marca]["valor_liquido"] += valor
            
            # Busca as devoluções do cliente
            devolucoes = db.movimentacao.find(filtro_devolucao)
            
            # Processa as devoluções
            for devolucao in devolucoes:
                marca = devolucao.get("marca", "Sem Marca")
                valor = devolucao.get("valor_final", 0)
                
                if marca not in valor_por_marca:
                    valor_por_marca[marca] = {
                        "valor_vendas": 0,
                        "valor_devolucoes": 0,
                        "valor_liquido": 0
                    }
                
                valor_por_marca[marca]["valor_devolucoes"] += valor
                valor_por_marca[marca]["valor_liquido"] -= valor
            
            # Ordena as marcas pelo valor líquido (do maior para o menor)
            valor_por_marca_ordenado = {}
            for marca, valores in sorted(
                valor_por_marca.items(), 
                key=lambda item: item[1]["valor_liquido"], 
                reverse=True
            ):
                valor_por_marca_ordenado[marca] = valores
            
            return {
                "codigo_cliente": cod_cliente,
                "valor_por_marca": valor_por_marca_ordenado,
                "total_marcas": len(valor_por_marca)
            }
        
        # Se cliente_id/cod_cliente não for fornecido, calcula para todos os clientes
        # Primeiro, obtém a lista de todos os clientes
        clientes = list(db.geradores.find(
            {"cod_cliente": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Para cada cliente, calcula o valor por marca
        resultados = []
        
        # Processa todos os clientes sem limitação
        for cliente in clientes:
            cod_cliente = cliente.get("cod_cliente")
            if not cod_cliente:
                continue
            
            # Adiciona o código do cliente aos filtros
            filtro_venda_cliente = {**filtro_venda_base, "codigo_cliente_fornecedor": cod_cliente}
            filtro_devolucao_cliente = {**filtro_devolucao_base, "codigo_cliente_fornecedor": cod_cliente}
            
            # Busca as vendas do cliente
            vendas = db.movimentacao.find(filtro_venda_cliente)
            
            # Inicializa o dicionário para armazenar os valores por marca
            valor_por_marca = {}
            
            # Processa as vendas
            for venda in vendas:
                marca = venda.get("marca", "Sem Marca")
                valor = venda.get("valor_final", 0)
                
                if marca not in valor_por_marca:
                    valor_por_marca[marca] = {
                        "valor_vendas": 0,
                        "valor_devolucoes": 0,
                        "valor_liquido": 0
                    }
                
                valor_por_marca[marca]["valor_vendas"] += valor
                valor_por_marca[marca]["valor_liquido"] += valor
            
            # Se não houver vendas, pula para o próximo cliente
            if not valor_por_marca:
                continue
            
            # Busca as devoluções do cliente
            devolucoes = db.movimentacao.find(filtro_devolucao_cliente)
            
            # Processa as devoluções
            for devolucao in devolucoes:
                marca = devolucao.get("marca", "Sem Marca")
                valor = devolucao.get("valor_final", 0)
                
                if marca not in valor_por_marca:
                    valor_por_marca[marca] = {
                        "valor_vendas": 0,
                        "valor_devolucoes": 0,
                        "valor_liquido": 0
                    }
                
                valor_por_marca[marca]["valor_devolucoes"] += valor
                valor_por_marca[marca]["valor_liquido"] -= valor
            
            # Ordena as marcas pelo valor líquido (do maior para o menor)
            valor_por_marca_ordenado = {}
            for marca, valores in sorted(
                valor_por_marca.items(), 
                key=lambda item: item[1]["valor_liquido"], 
                reverse=True
            ):
                valor_por_marca_ordenado[marca] = valores
            
            resultados.append({
                "id": cliente.get("_id"),
                "codigo_cliente": cod_cliente,
                "nome": cliente.get("razao_social", ""),
                "valor_por_marca": valor_por_marca_ordenado,
                "total_marcas": len(valor_por_marca)
            })
        
        # Ordena os resultados pelo número de marcas (do maior para o menor)
        resultados.sort(key=lambda x: x.get("total_marcas", 0), reverse=True)
        
        return resultados
    
    except Exception as e:
        print(f"Erro ao calcular valor por marca: {e}")
        return None

def obter_numero_marcas_diferentes(db, cliente_id=None, cod_cliente=None):
    """
    Calcula o número de marcas diferentes compradas pelo cliente.
    
    Args:
        db: Conexão com o banco de dados
        cliente_id: ID do cliente (opcional)
        cod_cliente: Código do cliente (opcional)
        
    Returns:
        Número de marcas diferentes ou lista de números se cliente_id/cod_cliente não for fornecido
    """
    try:
        # Reutiliza a função de valor por marca, que já calcula o número de marcas diferentes
        resultado = obter_valor_por_marca(db, cliente_id, cod_cliente)
        
        if resultado:
            if isinstance(resultado, list):
                # Se for uma lista de resultados (múltiplos clientes)
                for item in resultado:
                    # A lista de marcas seguirá a mesma ordem do campo valor_por_marca,
                    # que já está ordenado pelo valor líquido
                    item["lista_marcas"] = list(item.get("valor_por_marca", {}).keys())
                return resultado
            else:
                # Se for um único cliente
                return {
                    "codigo_cliente": resultado.get("codigo_cliente"),
                    "total_marcas": resultado.get("total_marcas", 0),
                    # A lista de marcas seguirá a mesma ordem do campo valor_por_marca,
                    # que já está ordenado pelo valor líquido
                    "lista_marcas": list(resultado.get("valor_por_marca", {}).keys())
                }
        
        return None
    
    except Exception as e:
        print(f"Erro ao calcular número de marcas diferentes: {e}")
        return None

def obter_titulos_pagos_em_dia(db, cliente_id=None, cod_cliente=None):
    """
    Calcula o total de títulos pagos em dia pelo cliente.
    
    Args:
        db: Conexão com o banco de dados
        cliente_id: ID do cliente (opcional)
        cod_cliente: Código do cliente (opcional)
        
    Returns:
        Total de títulos pagos em dia ou lista de totais se cliente_id/cod_cliente não for fornecido
    """
    try:
        # Função auxiliar para ajustar datas de vencimento em fins de semana
        def ajustar_data_vencimento(timestamp):
            if timestamp is None:
                return None
                
            # Converte timestamp para datetime
            from datetime import datetime, timedelta
            data = datetime.fromtimestamp(timestamp)
            
            # Verifica se é sábado (5) ou domingo (6)
            dia_semana = data.weekday()
            
            if dia_semana == 5:  # Sábado
                # Ajusta para segunda-feira (adiciona 2 dias)
                data_ajustada = data + timedelta(days=2)
                return int(data_ajustada.timestamp())
            elif dia_semana == 6:  # Domingo
                # Ajusta para segunda-feira (adiciona 1 dia)
                data_ajustada = data + timedelta(days=1)
                return int(data_ajustada.timestamp())
            else:
                # Não é fim de semana, mantém a data original
                return timestamp
        
        # Lista de tipos de pagamento válidos
        tipos_pagamento = [
            "BOLETO",
            "BOLETO BANCO DO BRASIL TBS",
            "BOLETO BRADESCO TBS",
            "BOLETO CAIXA TBS",
            "BOLETO ITAU TBS"
        ]
        
        # Filtro base para lançamentos
        filtro_base = {
            "tipo": "R",
            "substituido": False,
            "titulo": True,
            "tipo_pgto_descricao": {"$in": tipos_pagamento}
            # Removemos a restrição de data_pagamento para verificar todos os lançamentos
        }
        
        # Se cliente_id for fornecido, busca o código do cliente
        if cliente_id and not cod_cliente:
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente and "cod_cliente" in cliente:
                cod_cliente = cliente["cod_cliente"]
        
        # Se cod_cliente for fornecido, calcula o total apenas para esse cliente
        if cod_cliente:
            # Adiciona o código do cliente ao filtro
            filtro_base["cod_gerador"] = cod_cliente
            
            # Busca todos os lançamentos do cliente
            lancamentos = list(db.lancamentos_completo.find(filtro_base))
            
            # Verifica se o cliente usa boleto
            usa_boleto = len(lancamentos) > 0
            
            # Agrupa os lançamentos por número do lançamento e pega o de maior trans_id
            lancamentos_por_numero = {}
            for lancamento in lancamentos:
                numero = lancamento.get("lancamento")
                trans_id = lancamento.get("trans_id", 0)
                
                if numero not in lancamentos_por_numero or trans_id > lancamentos_por_numero[numero].get("trans_id", 0):
                    lancamentos_por_numero[numero] = lancamento
            
            # Conta os lançamentos pagos em dia e em diferentes prazos
            total_lancamentos = len(lancamentos_por_numero)
            pagos_em_dia = 0
            pagos_em_ate_7d = 0
            pagos_em_ate_15d = 0
            pagos_em_ate_30d = 0
            pagos_com_mais_30d = 0
            total_a_vencer = 0  # Boletos que ainda não venceram
            total_vencido = 0   # Boletos que venceram e não foram pagos
            total_a_vencer_valor = 0  # Valor total dos títulos a vencer
            
            # Variáveis para calcular inadimplência
            inadimplente_dias = 0  # Dias do título mais atrasado
            inadimplente_valor = 0  # Valor total dos títulos vencidos
            
            # Data atual para comparar com datas de vencimento
            from datetime import datetime
            data_atual = datetime.now().timestamp()
            
            for lancamento in lancamentos_por_numero.values():
                data_pagamento = lancamento.get("data_pagamento")
                data_vencimento = lancamento.get("data_vencimento")
                
                # Obtém o valor do lançamento, verificando diferentes campos possíveis
                valor_lancamento = lancamento.get("valor_liquido", 0)  # Prioriza valor_liquido
                if valor_lancamento == 0:  # Se não encontrou, tenta outros campos
                    valor_lancamento = lancamento.get("valor", 0)
                if valor_lancamento == 0:  # Se ainda não encontrou, tenta outros campos
                    valor_lancamento = lancamento.get("valor_titulo", 
                                    lancamento.get("valor_total", 
                                    lancamento.get("valor_parcela", 
                                    lancamento.get("valor_lancamento", 
                                    lancamento.get("valor_original", 0)))))
                
                # Ajusta a data de vencimento se cair em fim de semana
                data_vencimento_ajustada = ajustar_data_vencimento(data_vencimento)
                
                # Verifica se o lançamento foi pago
                if data_pagamento is not None and data_vencimento_ajustada is not None:
                    # Calcula a diferença em dias entre o pagamento e o vencimento
                    data_pag = datetime.fromtimestamp(data_pagamento)
                    data_venc = datetime.fromtimestamp(data_vencimento_ajustada)
                    diferenca_dias = (data_pag - data_venc).days
                    
                    # Classifica o pagamento de acordo com a diferença de dias
                    if diferenca_dias <= 0:
                        # Pago em dia (na data ou antes do vencimento)
                        pagos_em_dia += 1
                    elif 1 <= diferenca_dias <= 7:
                        # Pago com 1 a 7 dias de atraso
                        pagos_em_ate_7d += 1
                    elif 8 <= diferenca_dias <= 15:
                        # Pago com 8 a 15 dias de atraso
                        pagos_em_ate_15d += 1
                    elif 16 <= diferenca_dias <= 30:
                        # Pago com 16 a 30 dias de atraso
                        pagos_em_ate_30d += 1
                    else:
                        # Pago com mais de 30 dias de atraso
                        pagos_com_mais_30d += 1
                else:
                    # Lançamento não foi pago, verifica se já venceu ou não
                    if data_vencimento_ajustada is not None:
                        if data_vencimento_ajustada < data_atual:
                            # Já venceu e não foi pago
                            total_vencido += 1
                            
                            # Calcula dias de atraso
                            data_venc = datetime.fromtimestamp(data_vencimento_ajustada)
                            data_atual_dt = datetime.fromtimestamp(data_atual)
                            dias_atraso = (data_atual_dt - data_venc).days
                            
                            # Atualiza o maior atraso
                            if dias_atraso > inadimplente_dias:
                                inadimplente_dias = dias_atraso
                            
                            # Soma o valor do título vencido
                            inadimplente_valor += valor_lancamento
                        else:
                            # Ainda não venceu
                            total_a_vencer += 1
                            total_a_vencer_valor += valor_lancamento
                    else:
                        # Se não tem data de vencimento, consideramos como a vencer
                        total_a_vencer += 1
                        total_a_vencer_valor += valor_lancamento
            
            # Calcula o total de pagamentos realizados
            total_pagos = pagos_em_dia + pagos_em_ate_7d + pagos_em_ate_15d + pagos_em_ate_30d + pagos_com_mais_30d
            
            # Verifica se o total de lançamentos bate com a soma das categorias
            total_calculado = total_pagos + total_a_vencer + total_vencido
            if total_calculado != total_lancamentos:
                print(f"AVISO: Total calculado ({total_calculado}) não corresponde ao total de lançamentos ({total_lancamentos})")
            
            # Calcula os percentuais em relação ao total de pagamentos (não ao total de lançamentos)
            percentual_pagos_em_dia = round((pagos_em_dia / total_pagos) * 100, 2) if total_pagos > 0 else 0
            percentual_pagos_em_ate_7d = round((pagos_em_ate_7d / total_pagos) * 100, 2) if total_pagos > 0 else 0
            percentual_pagos_em_ate_15d = round((pagos_em_ate_15d / total_pagos) * 100, 2) if total_pagos > 0 else 0
            percentual_pagos_em_ate_30d = round((pagos_em_ate_30d / total_pagos) * 100, 2) if total_pagos > 0 else 0
            percentual_pagos_com_mais_30d = round((pagos_com_mais_30d / total_pagos) * 100, 2) if total_pagos > 0 else 0
            
            # Calcula também os percentuais em relação ao total de lançamentos
            percentual_pagos_total = round((total_pagos / total_lancamentos) * 100, 2) if total_lancamentos > 0 else 0
            percentual_a_vencer = round((total_a_vencer / total_lancamentos) * 100, 2) if total_lancamentos > 0 else 0
            percentual_vencido = round((total_vencido / total_lancamentos) * 100, 2) if total_lancamentos > 0 else 0
            
            # Determina se o cliente está inadimplente (tem títulos vencidos)
            inadimplente = total_vencido > 0
            
            # Adiciona log para debug
            print(f"    Cliente {cod_cliente}: {total_lancamentos} lançamentos, {total_pagos} pagos, {total_a_vencer} a vencer, {total_vencido} vencidos, {pagos_em_dia} pagos em dia, usa boleto: {usa_boleto}")
            if inadimplente:
                print(f"    Inadimplente: {inadimplente_dias} dias, R$ {inadimplente_valor:.2f}")
            
            # Retorna os resultados
            return {
                "total_lancamentos": total_lancamentos,
                "total_pagos": total_pagos,
                "total_a_vencer": total_a_vencer,
                "total_vencido": total_vencido,
                "total_a_vencer_valor": total_a_vencer_valor,
                "percentual_pagos_total": percentual_pagos_total,
                "percentual_a_vencer": percentual_a_vencer,
                "percentual_vencido": percentual_vencido,
                "inadimplente": inadimplente,
                "inadimplente_dias": inadimplente_dias if inadimplente else 0,
                "inadimplente_valor": round(inadimplente_valor, 2) if inadimplente else 0,
                "total_a_vencer_valor": total_a_vencer_valor,
                "usa_boleto": usa_boleto,
                "pagos_em_dia": pagos_em_dia,
                "percentual_pagos_em_dia": percentual_pagos_em_dia,
                "pagos_em_ate_7d": pagos_em_ate_7d,
                "percentual_pagos_em_ate_7d": percentual_pagos_em_ate_7d,
                "pagos_em_ate_15d": pagos_em_ate_15d,
                "percentual_pagos_em_ate_15d": percentual_pagos_em_ate_15d,
                "pagos_em_ate_30d": pagos_em_ate_30d,
                "percentual_pagos_em_ate_30d": percentual_pagos_em_ate_30d,
                "pagos_com_mais_30d": pagos_com_mais_30d,
                "percentual_pagos_com_mais_30d": percentual_pagos_com_mais_30d
            }
        
        # Se cliente_id/cod_cliente não for fornecido, calcula para todos os clientes
        # Primeiro, obtém a lista de todos os clientes
        clientes = list(db.geradores.find(
            {"cod_cliente": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Para cada cliente, calcula o total de títulos pagos em dia
        resultados = []
        
        # Processa todos os clientes sem limitação
        for cliente in clientes:
            cod_cliente = cliente.get("cod_cliente")
            if not cod_cliente:
                continue
            
            # Adiciona o código do cliente ao filtro
            filtro_cliente = {**filtro_base, "cod_gerador": cod_cliente}
            
            # Busca todos os lançamentos do cliente
            lancamentos = list(db.lancamentos_completo.find(filtro_cliente))
            
            # Verifica se o cliente usa boleto
            usa_boleto = len(lancamentos) > 0
            
            # Agrupa os lançamentos por número do lançamento e pega o de maior trans_id
            lancamentos_por_numero = {}
            for lancamento in lancamentos:
                numero = lancamento.get("lancamento")
                trans_id = lancamento.get("trans_id", 0)
                
                if numero not in lancamentos_por_numero or trans_id > lancamentos_por_numero[numero].get("trans_id", 0):
                    lancamentos_por_numero[numero] = lancamento
            
            # Conta os lançamentos pagos em dia e em diferentes prazos
            total_lancamentos = len(lancamentos_por_numero)
            
            # Se não houver lançamentos, pula para o próximo cliente
            if total_lancamentos == 0:
                continue
            
            pagos_em_dia = 0
            pagos_em_ate_7d = 0
            pagos_em_ate_15d = 0
            pagos_em_ate_30d = 0
            pagos_com_mais_30d = 0
            total_a_vencer = 0  # Boletos que ainda não venceram
            total_vencido = 0   # Boletos que venceram e não foram pagos
            total_a_vencer_valor = 0  # Valor total dos títulos a vencer
            
            # Variáveis para calcular inadimplência
            inadimplente_dias = 0  # Dias do título mais atrasado
            inadimplente_valor = 0  # Valor total dos títulos vencidos
            
            # Data atual para comparar com datas de vencimento
            from datetime import datetime
            data_atual = datetime.now().timestamp()
            
            for lancamento in lancamentos_por_numero.values():
                data_pagamento = lancamento.get("data_pagamento")
                data_vencimento = lancamento.get("data_vencimento")
                
                # Obtém o valor do lançamento, verificando diferentes campos possíveis
                valor_lancamento = lancamento.get("valor_liquido", 0)  # Prioriza valor_liquido
                if valor_lancamento == 0:  # Se não encontrou, tenta outros campos
                    valor_lancamento = lancamento.get("valor", 0)
                if valor_lancamento == 0:  # Se ainda não encontrou, tenta outros campos
                    valor_lancamento = lancamento.get("valor_titulo", 
                                    lancamento.get("valor_total", 
                                    lancamento.get("valor_parcela", 
                                    lancamento.get("valor_lancamento", 
                                    lancamento.get("valor_original", 0)))))
                
                # Ajusta a data de vencimento se cair em fim de semana
                data_vencimento_ajustada = ajustar_data_vencimento(data_vencimento)
                
                # Verifica se o lançamento foi pago
                if data_pagamento is not None and data_vencimento_ajustada is not None:
                    # Calcula a diferença em dias entre o pagamento e o vencimento
                    data_pag = datetime.fromtimestamp(data_pagamento)
                    data_venc = datetime.fromtimestamp(data_vencimento_ajustada)
                    diferenca_dias = (data_pag - data_venc).days
                    
                    # Classifica o pagamento de acordo com a diferença de dias
                    if diferenca_dias <= 0:
                        # Pago em dia (na data ou antes do vencimento)
                        pagos_em_dia += 1
                    elif 1 <= diferenca_dias <= 7:
                        # Pago com 1 a 7 dias de atraso
                        pagos_em_ate_7d += 1
                    elif 8 <= diferenca_dias <= 15:
                        # Pago com 8 a 15 dias de atraso
                        pagos_em_ate_15d += 1
                    elif 16 <= diferenca_dias <= 30:
                        # Pago com 16 a 30 dias de atraso
                        pagos_em_ate_30d += 1
                    else:
                        # Pago com mais de 30 dias de atraso
                        pagos_com_mais_30d += 1
                else:
                    # Lançamento não foi pago, verifica se já venceu ou não
                    if data_vencimento_ajustada is not None:
                        if data_vencimento_ajustada < data_atual:
                            # Já venceu e não foi pago
                            total_vencido += 1
                            
                            # Calcula dias de atraso
                            data_venc = datetime.fromtimestamp(data_vencimento_ajustada)
                            data_atual_dt = datetime.fromtimestamp(data_atual)
                            dias_atraso = (data_atual_dt - data_venc).days
                            
                            # Atualiza o maior atraso
                            if dias_atraso > inadimplente_dias:
                                inadimplente_dias = dias_atraso
                            
                            # Soma o valor do título vencido
                            inadimplente_valor += valor_lancamento
                        else:
                            # Ainda não venceu
                            total_a_vencer += 1
                            total_a_vencer_valor += valor_lancamento
                    else:
                        # Se não tem data de vencimento, consideramos como a vencer
                        total_a_vencer += 1
                        total_a_vencer_valor += valor_lancamento
            
            # Calcula o total de pagamentos realizados
            total_pagos = pagos_em_dia + pagos_em_ate_7d + pagos_em_ate_15d + pagos_em_ate_30d + pagos_com_mais_30d
            
            # Verifica se o total de lançamentos bate com a soma das categorias
            total_calculado = total_pagos + total_a_vencer + total_vencido
            if total_calculado != total_lancamentos:
                print(f"AVISO: Total calculado ({total_calculado}) não corresponde ao total de lançamentos ({total_lancamentos})")
            
            # Calcula os percentuais em relação ao total de pagamentos (não ao total de lançamentos)
            percentual_pagos_em_dia = round((pagos_em_dia / total_pagos) * 100, 2) if total_pagos > 0 else 0
            percentual_pagos_em_ate_7d = round((pagos_em_ate_7d / total_pagos) * 100, 2) if total_pagos > 0 else 0
            percentual_pagos_em_ate_15d = round((pagos_em_ate_15d / total_pagos) * 100, 2) if total_pagos > 0 else 0
            percentual_pagos_em_ate_30d = round((pagos_em_ate_30d / total_pagos) * 100, 2) if total_pagos > 0 else 0
            percentual_pagos_com_mais_30d = round((pagos_com_mais_30d / total_pagos) * 100, 2) if total_pagos > 0 else 0
            
            # Calcula também os percentuais em relação ao total de lançamentos
            percentual_pagos_total = round((total_pagos / total_lancamentos) * 100, 2) if total_lancamentos > 0 else 0
            percentual_a_vencer = round((total_a_vencer / total_lancamentos) * 100, 2) if total_lancamentos > 0 else 0
            percentual_vencido = round((total_vencido / total_lancamentos) * 100, 2) if total_lancamentos > 0 else 0
            
            # Determina se o cliente está inadimplente (tem títulos vencidos)
            inadimplente = total_vencido > 0
            
            resultados.append({
                "id": cliente.get("_id"),
                "codigo_cliente": cod_cliente,
                "nome": cliente.get("razao_social", ""),
                "total_lancamentos": total_lancamentos,
                "total_pagos": total_pagos,
                "total_a_vencer": total_a_vencer,
                "total_vencido": total_vencido,
                "total_a_vencer_valor": total_a_vencer_valor,
                "percentual_pagos_total": percentual_pagos_total,
                "percentual_a_vencer": percentual_a_vencer,
                "percentual_vencido": percentual_vencido,
                "inadimplente": inadimplente,
                "inadimplente_dias": inadimplente_dias if inadimplente else 0,
                "inadimplente_valor": round(inadimplente_valor, 2) if inadimplente else 0,
                "total_a_vencer_valor": total_a_vencer_valor,
                "usa_boleto": usa_boleto,
                "pagos_em_dia": pagos_em_dia,
                "percentual_pagos_em_dia": percentual_pagos_em_dia,
                "pagos_em_ate_7d": pagos_em_ate_7d,
                "percentual_pagos_em_ate_7d": percentual_pagos_em_ate_7d,
                "pagos_em_ate_15d": pagos_em_ate_15d,
                "percentual_pagos_em_ate_15d": percentual_pagos_em_ate_15d,
                "pagos_em_ate_30d": pagos_em_ate_30d,
                "percentual_pagos_em_ate_30d": percentual_pagos_em_ate_30d,
                "pagos_com_mais_30d": pagos_com_mais_30d,
                "percentual_pagos_com_mais_30d": percentual_pagos_com_mais_30d
            })
        
        # Ordena os resultados pelo percentual de pagamentos em dia (do maior para o menor)
        resultados.sort(key=lambda x: x.get("percentual_pagos_em_dia", 0), reverse=True)
        
        return resultados
    
    except Exception as e:
        print(f"Erro ao calcular títulos pagos em dia: {e}")
        import traceback
        traceback.print_exc()
        return None

def integrar_todas_consultas(db, limite_clientes=None, tamanho_lote=20, usar_cache=True, cliente_id=None):
    """
    Integra todas as consultas em um único resultado.
    
    Args:
        db: Conexão com o banco de dados
        limite_clientes: Limite de clientes a serem processados (None para processar todos)
        tamanho_lote: Tamanho do lote para processamento em batches
        usar_cache: Se True, usa cache para consultas já realizadas
        cliente_id: ID específico de um cliente para processar apenas ele
        
    Returns:
        Lista de dicionários com todas as informações consolidadas por cliente
    """
    try:
        print("Iniciando integração de todas as consultas...")
        inicio_total = datetime.now()
        
        # Verifica se existe cache e se deve ser usado
        cache_file = 'cache_resultados.json'
        resultados_cache = {}
        
        if usar_cache and os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    resultados_cache = json.load(f)
                print(f"Cache carregado com {len(resultados_cache)} clientes.")
            except Exception as e:
                print(f"Erro ao carregar cache: {e}")
                resultados_cache = {}
        
        # Obtém a lista de clientes
        if cliente_id:
            # Se um cliente_id específico foi fornecido, busca apenas esse cliente
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente:
                clientes = [cliente]
            else:
                print(f"Cliente com ID {cliente_id} não encontrado.")
                return []
        else:
            # Caso contrário, busca todos os clientes ou um limite deles
            query = {"cod_cliente": {"$exists": True, "$ne": None}}
            projection = {"_id": 1, "cod_cliente": 1, "razao_social": 1}
            
            if limite_clientes:
                clientes = list(db.geradores.find(query, projection).limit(limite_clientes))
            else:
                clientes = list(db.geradores.find(query, projection))
        
        total_clientes = len(clientes)
        print(f"Total de {total_clientes} clientes encontrados.")
        
        # Lista para armazenar os resultados consolidados
        resultados_consolidados = []
        
        # Processa os clientes em lotes
        for i in range(0, total_clientes, tamanho_lote):
            lote_atual = clientes[i:i+tamanho_lote]
            print(f"Processando lote {i//tamanho_lote + 1}/{(total_clientes-1)//tamanho_lote + 1} ({i+1}-{min(i+tamanho_lote, total_clientes)} de {total_clientes} clientes)...")
            
            # Para cada cliente no lote, executa todas as consultas
            for j, cliente in enumerate(lote_atual, 1):
                inicio_cliente = datetime.now()
                cliente_id = cliente.get("_id")
                cod_cliente = cliente.get("cod_cliente")
                nome_cliente = cliente.get("razao_social", "")
                
                print(f"  [{i+j}/{total_clientes}] Processando cliente: {cod_cliente} - {nome_cliente}")
                
                # Verifica se o cliente já está no cache
                if usar_cache and cod_cliente in resultados_cache:
                    print(f"    Usando cache para cliente {cod_cliente}")
                    resultados_consolidados.append(resultados_cache[cod_cliente])
                    continue
                
                # Inicializa o dicionário de resultados para o cliente
                resultado_cliente = {
                    "id": cliente_id,
                    "codigo_cliente": cod_cliente,
                    "nome_completo": nome_cliente,
                    "data_primeira_compra": None,
                    "faturamento_ultimos_12_meses": {
                        "total_vendas": 0,
                        "total_devolucoes": 0,
                        "faturamento_liquido": 0
                    },
                    "ciclos_compra_ultimos_6_meses": 0,
                    "ciclo_atual": False,
                    "meses_compra": [],
                    "total_pecas": {
                        "compradas": 0,
                        "devolvidas": 0,
                        "liquido": 0
                    },
                    "titulos_pagos_em_dia": {
                        "total_lancamentos": 0,
                        "total_pagos": 0,
                        "total_a_vencer": 0,
                        "total_vencido": 0,
                        "percentual_pagos_total": 0,
                        "percentual_a_vencer": 0,
                        "percentual_vencido": 0,
                        "inadimplente": False,
                        "inadimplente_dias": 0,
                        "inadimplente_valor": 0,
                        "total_a_vencer_valor": 0,
                        "usa_boleto": False,
                        "pagos_em_dia": 0,
                        "percentual_pagos_em_dia": 0,
                        "pagos_em_ate_7d": 0,
                        "percentual_pagos_em_ate_7d": 0,
                        "pagos_em_ate_15d": 0,
                        "percentual_pagos_em_ate_15d": 0,
                        "pagos_em_ate_30d": 0,
                        "percentual_pagos_em_ate_30d": 0,
                        "pagos_com_mais_30d": 0,
                        "percentual_pagos_com_mais_30d": 0
                    },
                    "valor_por_marca": {},
                    "numero_marcas_diferentes": 0,
                    "lista_marcas": [],
                    "limite_credito": 0,
                    "limite_credito_utilizado": 0
                }
                
                # Consulta 3: Data da primeira compra
                print("    Obtendo data da primeira compra...")
                data_primeira_compra = obter_data_primeira_compra(db, cliente_id=cliente_id)
                if data_primeira_compra:
                    resultado_cliente["data_primeira_compra"] = data_primeira_compra.get("data_formatada")
                    resultado_cliente["data_primeira_compra_timestamp"] = data_primeira_compra.get("data")
                    resultado_cliente["data_ultima_compra_timestamp"] = data_primeira_compra.get("data_ultima_compra")
                
                # Consulta 4: Faturamento total nos últimos 12 meses
                print("    Calculando faturamento...")
                faturamento = obter_faturamento_ultimos_12_meses(db, cliente_id=cliente_id)
                if faturamento:
                    resultado_cliente["faturamento_ultimos_12_meses"] = {
                        "total_vendas": faturamento.get("total_vendas", 0),
                        "total_devolucoes": faturamento.get("total_devolucoes", 0),
                        "faturamento_liquido": faturamento.get("faturamento_liquido", 0)
                    }
                
                # Consulta 5: Número de ciclos em que comprou nos últimos 6 meses
                print("    Calculando ciclos de compra...")
                ciclos = obter_ciclos_compra_ultimos_6_meses(db, cliente_id=cliente_id)
                if ciclos:
                    resultado_cliente["ciclos_compra_ultimos_6_meses"] = ciclos.get("num_ciclos_6_meses", 0)
                    resultado_cliente["ciclo_atual"] = ciclos.get("comprou_ciclo_atual", False)
                    resultado_cliente["meses_compra"] = ciclos.get("meses_compra_6_meses", [])
                
                # Consulta 6: Número total de peças compradas
                print("    Calculando total de peças...")
                pecas = obter_total_pecas_compradas(db, cliente_id=cliente_id)
                if pecas:
                    resultado_cliente["total_pecas"] = {
                        "compradas": pecas.get("total_bruto", 0),
                        "devolvidas": pecas.get("total_devolucoes", 0),
                        "liquido": pecas.get("total_liquido", 0)
                    }
                
                # Consulta 7: Total de títulos pagos em dia
                print("    Calculando títulos pagos em dia...")
                pagamentos = obter_titulos_pagos_em_dia(db, cliente_id=cliente_id)
                
                # Adiciona informações sobre limite de crédito
                cliente_obj = db.geradores.find_one({"_id": cliente_id})
                limite_credito = cliente_obj.get("limite_credito", 0) if cliente_obj else 0
                
                if pagamentos:
                    # Calcula o limite de crédito utilizado (total_a_vencer_valor + inadimplente_valor)
                    limite_credito_utilizado = pagamentos.get("total_a_vencer_valor", 0) + pagamentos.get("inadimplente_valor", 0)
                    
                    # Adiciona os campos ao resultado do cliente
                    resultado_cliente["limite_credito"] = limite_credito
                    resultado_cliente["limite_credito_utilizado"] = limite_credito_utilizado
                    
                    resultado_cliente["titulos_pagos_em_dia"] = {
                        "total_lancamentos": pagamentos.get("total_lancamentos", 0),
                        "total_pagos": pagamentos.get("total_pagos", 0),
                        "total_a_vencer": pagamentos.get("total_a_vencer", 0),
                        "total_vencido": pagamentos.get("total_vencido", 0),
                        "percentual_pagos_total": pagamentos.get("percentual_pagos_total", 0),
                        "percentual_a_vencer": pagamentos.get("percentual_a_vencer", 0),
                        "percentual_vencido": pagamentos.get("percentual_vencido", 0),
                        "inadimplente": pagamentos.get("inadimplente", False),
                        "inadimplente_dias": pagamentos.get("inadimplente_dias", 0),
                        "inadimplente_valor": round(pagamentos.get("inadimplente_valor", 0), 2),
                        "total_a_vencer_valor": pagamentos.get("total_a_vencer_valor", 0),
                        "usa_boleto": pagamentos.get("usa_boleto", False),
                        "pagos_em_dia": pagamentos.get("pagos_em_dia", 0),
                        "percentual_pagos_em_dia": pagamentos.get("percentual_pagos_em_dia", 0),
                        "pagos_em_ate_7d": pagamentos.get("pagos_em_ate_7d", 0),
                        "percentual_pagos_em_ate_7d": pagamentos.get("percentual_pagos_em_ate_7d", 0),
                        "pagos_em_ate_15d": pagamentos.get("pagos_em_ate_15d", 0),
                        "percentual_pagos_em_ate_15d": pagamentos.get("percentual_pagos_em_ate_15d", 0),
                        "pagos_em_ate_30d": pagamentos.get("pagos_em_ate_30d", 0),
                        "percentual_pagos_em_ate_30d": pagamentos.get("percentual_pagos_em_ate_30d", 0),
                        "pagos_com_mais_30d": pagamentos.get("pagos_com_mais_30d", 0),
                        "percentual_pagos_com_mais_30d": pagamentos.get("percentual_pagos_com_mais_30d", 0)
                    }
                
                # Consulta 8: Valor total comprado por marca
                print("    Calculando valor por marca...")
                valor_marcas = obter_valor_por_marca(db, cliente_id=cliente_id)
                if valor_marcas:
                    resultado_cliente["valor_por_marca"] = valor_marcas.get("valor_por_marca", {})
                    # Extrai a lista de marcas diretamente do valor_por_marca
                    resultado_cliente["lista_marcas"] = list(valor_marcas.get("valor_por_marca", {}).keys())
                
                # Consulta 9: Número de marcas diferentes compradas
                print("    Calculando número de marcas diferentes...")
                marcas = obter_numero_marcas_diferentes(db, cliente_id=cliente_id)
                if marcas:
                    resultado_cliente["numero_marcas_diferentes"] = marcas.get("total_marcas", 0)
                    resultado_cliente["lista_marcas"] = marcas.get("lista_marcas", [])
                
                # Adiciona o resultado do cliente à lista de resultados consolidados
                resultados_consolidados.append(resultado_cliente)
                
                # Atualiza o cache
                if usar_cache:
                    resultados_cache[cod_cliente] = resultado_cliente
                
                # Salva o cache a cada cliente processado
                if usar_cache and len(resultados_cache) % 5 == 0:
                    try:
                        with open(cache_file, 'w', encoding='utf-8') as f:
                            json.dump(resultados_cache, f, default=json_util.default, ensure_ascii=False, indent=2)
                        print(f"    Cache atualizado com {len(resultados_cache)} clientes.")
                    except Exception as e:
                        print(f"    Erro ao salvar cache: {e}")
                
                fim_cliente = datetime.now()
                tempo_cliente = (fim_cliente - inicio_cliente).total_seconds()
                print(f"    Cliente processado em {tempo_cliente:.2f} segundos.")
            
            # Salva resultados parciais a cada lote
            with open('resultados_parciais.json', 'w', encoding='utf-8') as f:
                json.dump(resultados_consolidados, f, default=json_util.default, ensure_ascii=False, indent=2)
            print(f"Resultados parciais salvos com {len(resultados_consolidados)} clientes processados.")
        
        # Salva o cache final
        if usar_cache:
            try:
                with open(cache_file, 'w', encoding='utf-8') as f:
                    json.dump(resultados_cache, f, default=json_util.default, ensure_ascii=False, indent=2)
                print(f"Cache final salvo com {len(resultados_cache)} clientes.")
            except Exception as e:
                print(f"Erro ao salvar cache final: {e}")
        
        fim_total = datetime.now()
        tempo_total = (fim_total - inicio_total).total_seconds()
        print(f"Processamento concluído para {len(resultados_consolidados)} clientes em {tempo_total:.2f} segundos.")
        return resultados_consolidados
    
    except Exception as e:
        print(f"Erro ao integrar consultas: {e}")
        import traceback
        traceback.print_exc()
        return None

def testar_integracao_consultas():
    """Testa a integração de todas as consultas."""
    db = conectar_mongodb()
    if db is None:
        print("Falha ao conectar ao MongoDB.")
        return
    
    # Clientes específicos que sabemos que têm dados
    clientes_teste = ["0100004049", "0000000894", "0000005078", "0000000836", "0000000839"]
    
    print(f"Integrando consultas para {len(clientes_teste)} clientes específicos...")
    
    # Lista para armazenar os resultados
    resultados = []
    
    # Processa cada cliente de teste
    for cod_cliente in clientes_teste:
        print(f"Processando cliente: {cod_cliente}")
        cliente = db.geradores.find_one({"cod_cliente": cod_cliente})
        
        if not cliente:
            print(f"Cliente {cod_cliente} não encontrado.")
            continue
        
        cliente_id = cliente.get("_id")
        resultado = integrar_todas_consultas(db, None, 1, False, cliente_id=cliente_id)
        
        if resultado and len(resultado) > 0:
            resultados.append(resultado[0])
    
    if resultados:
        print(f"\nResultados consolidados para {len(resultados)} clientes.")
        
        # Exibe um resumo de todos os clientes
        print("\nResumo dos resultados consolidados:")
        for i, resultado in enumerate(resultados, 1):
            print(f"\n{i}. Cliente: {resultado.get('codigo_cliente')} - {resultado.get('nome_completo')}")
            
            # Data da primeira compra
            data_primeira_compra = resultado.get('data_primeira_compra')
            print(f"   Data da primeira compra: {data_primeira_compra if data_primeira_compra else 'N/A'}")
            
            # Faturamento
            faturamento = resultado.get('faturamento_ultimos_12_meses', {})
            print(f"   Faturamento (12 meses): R$ {faturamento.get('faturamento_liquido', 0):.2f}")
            
            # Ciclos de compra
            ciclos = resultado.get('ciclos_compra_ultimos_6_meses', 0)
            print(f"   Ciclos de compra (6 meses): {ciclos}")
            
            # Total de peças
            total_pecas = resultado.get('total_pecas', {})
            print(f"   Total de peças: {total_pecas.get('liquido', 0)}")
            
            # Títulos pagos em dia
            pagamentos = resultado.get('titulos_pagos_em_dia', {})
            pagos_em_dia = pagamentos.get('pagos_em_dia', 0)
            total_lancamentos = pagamentos.get('total_lancamentos', 0)
            percentual_pagos_em_dia = pagamentos.get('percentual_pagos_em_dia', 0)
            usa_boleto = pagamentos.get('usa_boleto', False)
            inadimplente = pagamentos.get('inadimplente', False)
            print(f"   Títulos pagos em dia: {pagos_em_dia} de {total_lancamentos} ({percentual_pagos_em_dia}%)")
            print(f"   Usa boleto: {usa_boleto}")
            print(f"   Inadimplente: {inadimplente}")
            
            # Número de marcas
            num_marcas = resultado.get('numero_marcas_diferentes', 0)
            marcas = resultado.get('lista_marcas', [])
            print(f"   Número de marcas diferentes: {num_marcas}")
            print(f"   Marcas: {', '.join(marcas) if marcas else 'N/A'}")
        
        # Salva os resultados em um arquivo JSON
        with open('resultados_teste.json', 'w', encoding='utf-8') as f:
            json.dump(resultados, f, default=json_util.default, ensure_ascii=False, indent=2)
        
        print(f"\nResultados completos salvos em 'resultados_teste.json'")
    else:
        print("Falha ao integrar consultas.")

if __name__ == "__main__":
    # Verifica se deve processar todos os clientes (baseado no arquivo .env)
    if PROCESSAR_TODOS:
        db = conectar_mongodb()
        if db:
            print("Processando todos os clientes...")
            resultados = integrar_todas_consultas(db, None, TAMANHO_LOTE, USAR_CACHE)
            if resultados:
                print(f"Processados {len(resultados)} clientes.")
                with open('resultados_completos.json', 'w', encoding='utf-8') as f:
                    json.dump(resultados, f, default=json_util.default, ensure_ascii=False, indent=2)
                print("Resultados completos salvos em 'resultados_completos.json'")
    else:
        # Executa o teste de integração com clientes específicos
        testar_integracao_consultas()
