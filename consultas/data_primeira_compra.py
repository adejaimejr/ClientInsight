"""
Consulta de data da primeira compra dos clientes.
"""
from datetime import datetime
from .base import EVENTOS_VENDA

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
        # Se cliente_id for fornecido, busca apenas esse cliente
        if cliente_id:
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente and "cod_cliente" in cliente:
                cod_cliente = cliente["cod_cliente"]
            else:
                return None
        
        # Se cod_cliente for fornecido, calcula apenas para esse cliente
        if cod_cliente:
            # Filtro para vendas do cliente
            filtro_venda_cliente = {
                "codigo_cliente_fornecedor": cod_cliente,
                "evento": {"$in": EVENTOS_VENDA}
            }
            
            # Busca a primeira compra do cliente (ordenada por data)
            primeira_compra = list(db.movimentacao.find(filtro_venda_cliente).sort("data", 1).limit(1))
            
            # Se não houver compras, retorna None
            if not primeira_compra:
                return None
            
            # Busca a última compra do cliente (ordenada por data decrescente)
            ultima_compra = list(db.movimentacao.find(filtro_venda_cliente).sort("data", -1).limit(1))
            
            # Formata a data da primeira compra
            data_timestamp = primeira_compra[0].get("data")
            data_formatada = datetime.fromtimestamp(data_timestamp).strftime("%Y-%m-%d") if data_timestamp else None
            
            # Adiciona o campo formatado ao resultado
            primeira_compra[0]["data_"] = data_formatada
            
            return {
                "codigo_cliente": cod_cliente,
                "data": data_timestamp,
                "data_formatada": data_formatada,
                "data_ultima_compra": ultima_compra[0].get("data") if ultima_compra else None
            }
        
        # Se cliente_id/cod_cliente não for fornecido, calcula para todos os clientes
        # Primeiro, obtém a lista de todos os clientes
        clientes = list(db.geradores.find(
            {"cod_cliente": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Lista para armazenar os resultados
        resultados = []
        
        # Obtém a data da primeira compra para cada cliente
        for cliente in clientes:
            cod_cliente = cliente.get("cod_cliente")
            if not cod_cliente:
                continue
            
            # Filtro para vendas do cliente
            filtro_venda_cliente = {
                "codigo_cliente_fornecedor": cod_cliente,
                "evento": {"$in": EVENTOS_VENDA}
            }
            
            # Busca a primeira compra do cliente (ordenada por data)
            primeira_compra = list(db.movimentacao.find(filtro_venda_cliente).sort("data", 1).limit(1))
            
            # Se não houver compras, pula para o próximo cliente
            if not primeira_compra:
                continue
            
            # Busca a última compra do cliente (ordenada por data decrescente)
            ultima_compra = list(db.movimentacao.find(filtro_venda_cliente).sort("data", -1).limit(1))
            
            # Formata a data da primeira compra
            data_timestamp = primeira_compra[0].get("data")
            data_formatada = datetime.fromtimestamp(data_timestamp).strftime("%Y-%m-%d") if data_timestamp else None
            
            # Adiciona o campo formatado ao resultado
            primeira_compra[0]["data_"] = data_formatada
            
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
