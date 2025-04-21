"""
Consulta de valor por marca para os clientes.
"""
from .base import EVENTOS_VENDA, EVENTOS_DEVOLUCAO

def obter_valor_por_marca(db, cliente_id=None, cod_cliente=None):
    """
    Calcula o valor total de compras por marca para o cliente.
    
    Args:
        db: Conexão com o banco de dados
        cliente_id: ID do cliente (opcional)
        cod_cliente: Código do cliente (opcional)
        
    Returns:
        Dicionário com valor por marca ou lista de dicionários se cliente_id/cod_cliente não for fornecido
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
            
            # Filtro para devoluções do cliente
            filtro_devolucao_cliente = {
                "codigo_cliente_fornecedor": cod_cliente,
                "evento": {"$in": EVENTOS_DEVOLUCAO}
            }
            
            # Busca todas as compras do cliente
            vendas = list(db.movimentacao.find(filtro_venda_cliente))
            
            # Se não houver compras, retorna None
            if not vendas:
                return {
                    "codigo_cliente": cod_cliente,
                    "valor_por_marca": {}
                }
            
            # Busca todas as devoluções do cliente
            devolucoes = list(db.movimentacao.find(filtro_devolucao_cliente))
            
            # Dicionário para armazenar o valor por marca
            valor_por_marca = {}
            
            # Processa cada venda
            for venda in vendas:
                # Garante que marca nunca seja None - substitui por "Sem marca" ou "INDEFINIDO"
                marca = venda.get("marca")
                if marca is None or marca == "null" or marca == "":
                    marca = "INDEFINIDO"
                
                # Verifica os diferentes campos que podem conter o valor
                valor = 0
                # Prioridade 1: usar valor_final (preço após descontos)
                if "valor_final" in venda and venda["valor_final"]:
                    try:
                        valor = float(venda["valor_final"])
                    except (ValueError, TypeError):
                        pass
                # Prioridade 2: usar preco_bruto
                elif "preco_bruto" in venda and venda["preco_bruto"]:
                    try:
                        valor = float(venda["preco_bruto"])
                    except (ValueError, TypeError):
                        pass
                # Tentativas adicionais para outros campos possíveis de valor
                elif "valor_total" in venda and venda["valor_total"]:
                    try:
                        valor = float(venda["valor_total"])
                    except (ValueError, TypeError):
                        pass
                elif "valor" in venda and venda["valor"]:
                    try:
                        valor = float(venda["valor"])
                    except (ValueError, TypeError):
                        pass
                
                # Adiciona o valor ao dicionário
                if marca in valor_por_marca:
                    valor_por_marca[marca] += valor
                else:
                    valor_por_marca[marca] = valor
            
            # Processa cada devolução
            for devolucao in devolucoes:
                # Garante que marca nunca seja None - substitui por "Sem marca" ou "INDEFINIDO"
                marca = devolucao.get("marca")
                if marca is None or marca == "null" or marca == "":
                    marca = "INDEFINIDO"
                
                # Verifica os diferentes campos que podem conter o valor
                valor = 0
                # Prioridade 1: usar valor_final (preço após descontos)
                if "valor_final" in devolucao and devolucao["valor_final"]:
                    try:
                        valor = float(devolucao["valor_final"])
                    except (ValueError, TypeError):
                        pass
                # Prioridade 2: usar preco_bruto
                elif "preco_bruto" in devolucao and devolucao["preco_bruto"]:
                    try:
                        valor = float(devolucao["preco_bruto"])
                    except (ValueError, TypeError):
                        pass
                # Tentativas adicionais para outros campos possíveis de valor
                elif "valor_total" in devolucao and devolucao["valor_total"]:
                    try:
                        valor = float(devolucao["valor_total"])
                    except (ValueError, TypeError):
                        pass
                elif "valor" in devolucao and devolucao["valor"]:
                    try:
                        valor = float(devolucao["valor"])
                    except (ValueError, TypeError):
                        pass
                
                # Subtrai o valor do dicionário
                if marca in valor_por_marca:
                    valor_por_marca[marca] -= valor
            
            # Arredonda todos os valores para 2 casas decimais
            valor_por_marca = {marca: round(valor, 2) for marca, valor in valor_por_marca.items()}
            
            # Ordena o dicionário por valor em ordem decrescente
            valor_por_marca_ordenado = dict(sorted(valor_por_marca.items(), key=lambda x: x[1], reverse=True))
            
            return {
                "codigo_cliente": cod_cliente,
                "valor_por_marca": valor_por_marca_ordenado
            }
        
        # Se cliente_id/cod_cliente não for fornecido, calcula para todos os clientes
        # Primeiro, obtém a lista de todos os clientes
        clientes = list(db.geradores.find(
            {"cod_cliente": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Lista para armazenar os resultados
        resultados = []
        
        # Calcula o valor por marca para cada cliente
        for cliente in clientes:
            cod_cliente = cliente.get("cod_cliente")
            if not cod_cliente:
                continue
            
            # Filtro para vendas do cliente
            filtro_venda_cliente = {
                "codigo_cliente_fornecedor": cod_cliente,
                "evento": {"$in": EVENTOS_VENDA}
            }
            
            # Filtro para devoluções do cliente
            filtro_devolucao_cliente = {
                "codigo_cliente_fornecedor": cod_cliente,
                "evento": {"$in": EVENTOS_DEVOLUCAO}
            }
            
            # Busca todas as compras do cliente
            vendas = list(db.movimentacao.find(filtro_venda_cliente))
            
            # Se não houver compras, pula para o próximo cliente
            if not vendas:
                continue
            
            # Busca todas as devoluções do cliente
            devolucoes = list(db.movimentacao.find(filtro_devolucao_cliente))
            
            # Dicionário para armazenar o valor por marca
            valor_por_marca = {}
            
            # Processa cada venda
            for venda in vendas:
                # Garante que marca nunca seja None - substitui por "Sem marca" ou "INDEFINIDO"
                marca = venda.get("marca")
                if marca is None or marca == "null" or marca == "":
                    marca = "INDEFINIDO"
                
                # Verifica os diferentes campos que podem conter o valor
                valor = 0
                # Prioridade 1: usar valor_final (preço após descontos)
                if "valor_final" in venda and venda["valor_final"]:
                    try:
                        valor = float(venda["valor_final"])
                    except (ValueError, TypeError):
                        pass
                # Prioridade 2: usar preco_bruto
                elif "preco_bruto" in venda and venda["preco_bruto"]:
                    try:
                        valor = float(venda["preco_bruto"])
                    except (ValueError, TypeError):
                        pass
                # Tentativas adicionais para outros campos possíveis de valor
                elif "valor_total" in venda and venda["valor_total"]:
                    try:
                        valor = float(venda["valor_total"])
                    except (ValueError, TypeError):
                        pass
                elif "valor" in venda and venda["valor"]:
                    try:
                        valor = float(venda["valor"])
                    except (ValueError, TypeError):
                        pass
                
                # Adiciona o valor ao dicionário
                if marca in valor_por_marca:
                    valor_por_marca[marca] += valor
                else:
                    valor_por_marca[marca] = valor
            
            # Processa cada devolução
            for devolucao in devolucoes:
                # Garante que marca nunca seja None - substitui por "Sem marca" ou "INDEFINIDO"
                marca = devolucao.get("marca")
                if marca is None or marca == "null" or marca == "":
                    marca = "INDEFINIDO"
                
                # Verifica os diferentes campos que podem conter o valor
                valor = 0
                # Prioridade 1: usar valor_final (preço após descontos)
                if "valor_final" in devolucao and devolucao["valor_final"]:
                    try:
                        valor = float(devolucao["valor_final"])
                    except (ValueError, TypeError):
                        pass
                # Prioridade 2: usar preco_bruto
                elif "preco_bruto" in devolucao and devolucao["preco_bruto"]:
                    try:
                        valor = float(devolucao["preco_bruto"])
                    except (ValueError, TypeError):
                        pass
                # Tentativas adicionais para outros campos possíveis de valor
                elif "valor_total" in devolucao and devolucao["valor_total"]:
                    try:
                        valor = float(devolucao["valor_total"])
                    except (ValueError, TypeError):
                        pass
                elif "valor" in devolucao and devolucao["valor"]:
                    try:
                        valor = float(devolucao["valor"])
                    except (ValueError, TypeError):
                        pass
                
                # Subtrai o valor do dicionário
                if marca in valor_por_marca:
                    valor_por_marca[marca] -= valor
            
            # Arredonda todos os valores para 2 casas decimais
            valor_por_marca = {marca: round(valor, 2) for marca, valor in valor_por_marca.items()}
            
            # Ordena o dicionário por valor em ordem decrescente
            valor_por_marca_ordenado = dict(sorted(valor_por_marca.items(), key=lambda x: x[1], reverse=True))
            
            resultados.append({
                "id": cliente.get("_id"),
                "codigo_cliente": cod_cliente,
                "nome": cliente.get("razao_social", ""),
                "valor_por_marca": valor_por_marca_ordenado
            })
        
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
        # Se cliente_id for fornecido, busca apenas esse cliente
        if cliente_id:
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente and "cod_cliente" in cliente:
                cod_cliente = cliente["cod_cliente"]
            else:
                return None
        
        # Se cod_cliente for fornecido, calcula apenas para esse cliente
        if cod_cliente:
            # Primeiro, obtém o valor por marca ordenado
            resultado_valor_por_marca = obter_valor_por_marca(db, cliente_id=cliente_id, cod_cliente=cod_cliente)
            
            if not resultado_valor_por_marca or not resultado_valor_por_marca.get("valor_por_marca"):
                # Se não houver valores por marca, retorna 0
                return {
                    "codigo_cliente": cod_cliente,
                    "total_marcas": 0,
                    "lista_marcas": []
                }
            
            # Usa as marcas já ordenadas pelo valor
            valor_por_marca = resultado_valor_por_marca.get("valor_por_marca", {})
            marcas_ordenadas = list(valor_por_marca.keys())
            
            return {
                "codigo_cliente": cod_cliente,
                "total_marcas": len(marcas_ordenadas),
                "lista_marcas": marcas_ordenadas
            }
        
        # Se cliente_id/cod_cliente não for fornecido, calcula para todos os clientes
        # Primeiro, obtém a lista de todos os clientes
        clientes = list(db.geradores.find(
            {"cod_cliente": {"$exists": True, "$ne": None}},
            {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        ))
        
        # Lista para armazenar os resultados
        resultados = []
        
        # Calcula o número de marcas diferentes para cada cliente
        for cliente in clientes:
            cod_cliente = cliente.get("cod_cliente")
            if not cod_cliente:
                continue
            
            # Obtém o valor por marca ordenado para este cliente
            resultado_valor_por_marca = obter_valor_por_marca(db, cod_cliente=cod_cliente)
            
            if not resultado_valor_por_marca or not resultado_valor_por_marca.get("valor_por_marca"):
                continue
            
            # Usa as marcas já ordenadas pelo valor
            valor_por_marca = resultado_valor_por_marca.get("valor_por_marca", {})
            marcas_ordenadas = list(valor_por_marca.keys())
            
            resultados.append({
                "id": cliente.get("_id"),
                "codigo_cliente": cod_cliente,
                "nome": cliente.get("razao_social", ""),
                "total_marcas": len(marcas_ordenadas),
                "lista_marcas": marcas_ordenadas
            })
        
        return resultados
    
    except Exception as e:
        print(f"Erro ao calcular número de marcas diferentes: {e}")
        return None
