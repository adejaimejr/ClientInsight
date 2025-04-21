"""
Consultas básicas de informações do cliente.
"""

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
        
        # Se cliente_id não for fornecido, busca todos os clientes
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

def obter_nome_completo(db, cliente_id=None, cod_cliente=None):
    """
    Obtém o nome completo do cliente.
    
    Args:
        db: Conexão com o banco de dados
        cliente_id: ID do cliente (opcional)
        cod_cliente: Código do cliente (opcional)
        
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
        
        # Se cod_cliente for fornecido, busca apenas esse cliente
        if cod_cliente:
            cliente = db.geradores.find_one({"cod_cliente": cod_cliente})
            if cliente and "razao_social" in cliente:
                return cliente["razao_social"]
            return None
        
        # Se cliente_id/cod_cliente não for fornecido, busca todos os clientes
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
