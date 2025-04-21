"""
Consulta de faturamento dos clientes.
"""
from datetime import datetime, timedelta
from .base import EVENTOS_VENDA, EVENTOS_DEVOLUCAO

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
                "cancelada": False,
                "data": {"$gte": data_12_meses_atras}
            }
            
            # Filtro para devoluções do cliente
            filtro_devolucao_cliente = {
                "codigo_cliente_fornecedor": cod_cliente,
                "evento": {"$in": EVENTOS_DEVOLUCAO},
                "tipo_operacao": "E",
                "cancelada": False,
                "data": {"$gte": data_12_meses_atras}
            }
            
            # Calcula o valor total de vendas
            vendas = db.movimentacao.find(filtro_venda_cliente)
            total_vendas = sum(venda.get("valor_final", 0) for venda in vendas)
            
            # Se não houver vendas, pula para o próximo cliente
            if total_vendas == 0:
                return None
            
            # Calcula o valor total de devoluções
            devolucoes = db.movimentacao.find(filtro_devolucao_cliente)
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
        
        # Lista para armazenar os resultados
        resultados = []
        
        # Calcula o faturamento para cada cliente
        for cliente in clientes:
            cod_cliente = cliente.get("cod_cliente")
            if not cod_cliente:
                continue
            
            # Filtro para vendas do cliente
            filtro_venda_cliente = {
                "codigo_cliente_fornecedor": cod_cliente,
                "evento": {"$in": EVENTOS_VENDA},
                "tipo_operacao": "S",
                "cancelada": False,
                "data": {"$gte": data_12_meses_atras}
            }
            
            # Filtro para devoluções do cliente
            filtro_devolucao_cliente = {
                "codigo_cliente_fornecedor": cod_cliente,
                "evento": {"$in": EVENTOS_DEVOLUCAO},
                "tipo_operacao": "E",
                "cancelada": False,
                "data": {"$gte": data_12_meses_atras}
            }
            
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
        
        return resultados
    
    except Exception as e:
        print(f"Erro ao calcular faturamento: {e}")
        return None
