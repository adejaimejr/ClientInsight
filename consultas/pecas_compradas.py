"""
Consulta de total de peças compradas pelos clientes.
"""
from .base import EVENTOS_VENDA, EVENTOS_DEVOLUCAO

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
                "evento": {"$in": EVENTOS_VENDA},
                "tipo_operacao": "S",
                "cancelada": False
            }
            
            # Filtro para devoluções do cliente
            filtro_devolucao_cliente = {
                "codigo_cliente_fornecedor": cod_cliente,
                "evento": {"$in": EVENTOS_DEVOLUCAO},
                "tipo_operacao": "E",
                "cancelada": False
            }
            
            # Busca todas as compras do cliente
            vendas = db.movimentacao.find(filtro_venda_cliente)
            
            # Calcula o total de peças compradas
            total_pecas_compradas = sum(venda.get("qtde", 0) for venda in vendas)
            
            # Se não houver compras, pula para o próximo cliente
            if total_pecas_compradas == 0:
                return None
            
            # Busca todas as devoluções do cliente
            devolucoes = db.movimentacao.find(filtro_devolucao_cliente)
            
            # Calcula o total de peças devolvidas
            total_pecas_devolvidas = sum(devolucao.get("qtde", 0) for devolucao in devolucoes)
            
            # Calcula o total líquido
            total_liquido = total_pecas_compradas - total_pecas_devolvidas
            
            return {
                "codigo_cliente": cod_cliente,
                "total_bruto": total_pecas_compradas,
                "total_devolucoes": total_pecas_devolvidas,
                "total_liquido": total_liquido
            }
        
        # Se cliente_id/cod_cliente não for fornecido, calcula para todos os clientes
        # Primeiro, obtém a lista de todos os clientes
        clientes = list(db.geradores.find(
            {"cod_cliente": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Lista para armazenar os resultados
        resultados = []
        
        # Calcula o total de peças para cada cliente
        for cliente in clientes:
            cod_cliente = cliente.get("cod_cliente")
            if not cod_cliente:
                continue
            
            # Filtro para vendas do cliente
            filtro_venda_cliente = {
                "codigo_cliente_fornecedor": cod_cliente,
                "evento": {"$in": EVENTOS_VENDA},
                "tipo_operacao": "S",
                "cancelada": False
            }
            
            # Filtro para devoluções do cliente
            filtro_devolucao_cliente = {
                "codigo_cliente_fornecedor": cod_cliente,
                "evento": {"$in": EVENTOS_DEVOLUCAO},
                "tipo_operacao": "E",
                "cancelada": False
            }
            
            # Busca todas as compras do cliente
            vendas = db.movimentacao.find(filtro_venda_cliente)
            
            # Calcula o total de peças compradas
            total_pecas_compradas = sum(venda.get("qtde", 0) for venda in vendas)
            
            # Se não houver compras, pula para o próximo cliente
            if total_pecas_compradas == 0:
                continue
            
            # Busca todas as devoluções do cliente
            devolucoes = db.movimentacao.find(filtro_devolucao_cliente)
            
            # Calcula o total de peças devolvidas
            total_pecas_devolvidas = sum(devolucao.get("qtde", 0) for devolucao in devolucoes)
            
            # Calcula o total líquido
            total_liquido = total_pecas_compradas - total_pecas_devolvidas
            
            resultados.append({
                "id": cliente.get("_id"),
                "codigo_cliente": cod_cliente,
                "nome": cliente.get("razao_social", ""),
                "total_bruto": total_pecas_compradas,
                "total_devolucoes": total_pecas_devolvidas,
                "total_liquido": total_liquido
            })
        
        return resultados
    
    except Exception as e:
        print(f"Erro ao calcular total de peças compradas: {e}")
        return None
