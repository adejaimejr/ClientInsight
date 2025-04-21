"""
Consulta de ciclos de compra dos clientes.
"""
from datetime import datetime, timedelta
from .base import EVENTOS_VENDA

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
        
        # Primeiro dia de 6 meses atrás
        primeiro_dia_6_meses_atras = primeiro_dia_mes_atual - timedelta(days=180)
        
        # Converte para timestamp
        timestamp_6_meses_atras = int(primeiro_dia_6_meses_atras.timestamp())
        timestamp_mes_atual = int(primeiro_dia_mes_atual.timestamp())
        
        # Filtro base para os últimos 6 meses (excluindo o mês atual)
        filtro_base_6_meses = {
            "evento": {"$in": EVENTOS_VENDA},
            "tipo_operacao": "S",
            "cancelada": False,
            "data": {"$gte": timestamp_6_meses_atras, "$lt": timestamp_mes_atual}
        }
        
        # Filtro base para o mês atual
        filtro_base_ciclo_atual = {
            "evento": {"$in": EVENTOS_VENDA},
            "tipo_operacao": "S",
            "cancelada": False,
            "data": {"$gte": timestamp_mes_atual}
        }
        
        # Se cliente_id for fornecido, busca apenas esse cliente
        if cliente_id:
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente and "cod_cliente" in cliente:
                cod_cliente = cliente["cod_cliente"]
            else:
                return None
        
        # Se cod_cliente for fornecido, calcula apenas para esse cliente
        if cod_cliente:
            # Filtro para vendas do cliente nos últimos 6 meses
            filtro_6_meses = {**filtro_base_6_meses, "codigo_cliente_fornecedor": cod_cliente}
            
            # Busca todas as vendas do cliente nos últimos 6 meses
            vendas_6_meses = db.movimentacao.find(filtro_6_meses)
            
            # Conjunto para armazenar os meses distintos
            meses_compra_6_meses = set()
            
            # Para cada venda, extrai o mês e adiciona ao conjunto
            for venda in vendas_6_meses:
                try:
                    # Converte o timestamp para datetime
                    data_venda = datetime.fromtimestamp(venda.get("data"))
                    
                    # Extrai o ano e mês
                    ano_mes = f"{data_venda.year}-{data_venda.month:02d}"
                    
                    # Adiciona ao conjunto
                    meses_compra_6_meses.add(ano_mes)
                except Exception:
                    pass
            
            # Conta o número de meses distintos nos últimos 6 meses
            num_ciclos_6_meses = len(meses_compra_6_meses)
            
            # Lista os meses para referência
            meses_lista_6_meses = sorted(list(meses_compra_6_meses))
            
            # Verifica se o cliente comprou no ciclo atual (mês atual)
            filtro_ciclo_atual = {**filtro_base_ciclo_atual, "codigo_cliente_fornecedor": cod_cliente}
            comprou_ciclo_atual = db.movimentacao.count_documents(filtro_ciclo_atual) > 0
            
            return {
                "codigo_cliente": cod_cliente,
                "num_ciclos_6_meses": num_ciclos_6_meses,
                "meses_compra_6_meses": meses_lista_6_meses,
                "comprou_ciclo_atual": comprou_ciclo_atual
            }
        
        # Se cliente_id/cod_cliente não for fornecido, calcula para todos os clientes
        # Primeiro, obtém a lista de todos os clientes
        clientes = list(db.geradores.find(
            {"cod_cliente": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Lista para armazenar os resultados
        resultados = []
        
        # Calcula os ciclos de compra para cada cliente
        for cliente in clientes:
            cod_cliente = cliente.get("cod_cliente")
            if not cod_cliente:
                continue
            
            # Filtro para vendas do cliente nos últimos 6 meses
            filtro_6_meses = {**filtro_base_6_meses, "codigo_cliente_fornecedor": cod_cliente}
            
            # Busca todas as vendas do cliente nos últimos 6 meses
            vendas_6_meses = db.movimentacao.find(filtro_6_meses)
            
            # Conjunto para armazenar os meses distintos
            meses_compra_6_meses = set()
            
            # Para cada venda, extrai o mês e adiciona ao conjunto
            for venda in vendas_6_meses:
                try:
                    # Converte o timestamp para datetime
                    data_venda = datetime.fromtimestamp(venda.get("data"))
                    
                    # Extrai o ano e mês
                    ano_mes = f"{data_venda.year}-{data_venda.month:02d}"
                    
                    # Adiciona ao conjunto
                    meses_compra_6_meses.add(ano_mes)
                except Exception:
                    pass
            
            # Se não houver compras nos últimos 6 meses, pula para o próximo cliente
            if len(meses_compra_6_meses) == 0:
                continue
            
            # Conta o número de meses distintos nos últimos 6 meses
            num_ciclos_6_meses = len(meses_compra_6_meses)
            
            # Lista os meses para referência
            meses_lista_6_meses = sorted(list(meses_compra_6_meses))
            
            # Verifica se o cliente comprou no ciclo atual (mês atual)
            filtro_ciclo_atual = {**filtro_base_ciclo_atual, "codigo_cliente_fornecedor": cod_cliente}
            comprou_ciclo_atual = db.movimentacao.count_documents(filtro_ciclo_atual) > 0
            
            resultados.append({
                "id": cliente.get("_id"),
                "codigo_cliente": cod_cliente,
                "nome": cliente.get("razao_social", ""),
                "num_ciclos_6_meses": num_ciclos_6_meses,
                "meses_compra_6_meses": meses_lista_6_meses,
                "comprou_ciclo_atual": comprou_ciclo_atual
            })
        
        return resultados
    
    except Exception as e:
        print(f"Erro ao calcular ciclos de compra: {e}")
        return None
