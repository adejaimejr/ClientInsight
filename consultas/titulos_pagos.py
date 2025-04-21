"""
Consulta de títulos pagos em dia pelos clientes.
"""
from datetime import datetime, timedelta

def calcular_feriados_moveis(ano):
    """
    Calcula as datas dos feriados móveis para um determinado ano.
    
    Args:
        ano: Ano para calcular os feriados
        
    Returns:
        Lista de datas em formato timestamp para os feriados móveis do ano
    """
    # Cálculo da Páscoa usando o algoritmo de Butcher/Meeus
    a = ano % 19
    b = ano // 100
    c = ano % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mes_pascoa = (h + l - 7 * m + 114) // 31
    dia_pascoa = ((h + l - 7 * m + 114) % 31) + 1
    
    # Data da Páscoa
    pascoa = datetime(ano, mes_pascoa, dia_pascoa)
    
    # Feriados relacionados à Páscoa
    carnaval = pascoa - timedelta(days=47)  # 47 dias antes da Páscoa
    sexta_santa = pascoa - timedelta(days=2)  # Sexta-feira antes da Páscoa
    corpus_christi = pascoa + timedelta(days=60)  # 60 dias após a Páscoa
    
    # Converte para timestamp e retorna
    return [
        int(carnaval.timestamp()),
        int(sexta_santa.timestamp()),
        int(pascoa.timestamp()),
        int(corpus_christi.timestamp())
    ]

def obter_feriados_nacionais(ano=None):
    """
    Retorna os feriados nacionais para um determinado ano.
    
    Args:
        ano: Ano para obter os feriados. Se não for fornecido, usa o ano atual.
        
    Returns:
        Lista de timestamps representando os feriados nacionais
    """
    if ano is None:
        ano = datetime.now().year
    
    # Feriados fixos
    feriados = [
        # Confraternização Universal - 1 de Janeiro
        int(datetime(ano, 1, 1).timestamp()),
        # Tiradentes - 21 de Abril
        int(datetime(ano, 4, 21).timestamp()),
        # Dia do Trabalho - 1 de Maio
        int(datetime(ano, 5, 1).timestamp()),
        # Independência do Brasil - 7 de Setembro
        int(datetime(ano, 9, 7).timestamp()),
        # Nossa Senhora Aparecida - 12 de Outubro
        int(datetime(ano, 10, 12).timestamp()),
        # Finados - 2 de Novembro
        int(datetime(ano, 11, 2).timestamp()),
        # Proclamação da República - 15 de Novembro
        int(datetime(ano, 11, 15).timestamp()),
        # Natal - 25 de Dezembro
        int(datetime(ano, 12, 25).timestamp())
    ]
    
    # Adiciona os feriados móveis
    feriados.extend(calcular_feriados_moveis(ano))
    
    return feriados

def ajustar_data_vencimento(timestamp):
    """
    Ajusta a data de vencimento se cair em fim de semana ou feriado.
    
    Args:
        timestamp: Data de vencimento em formato timestamp
        
    Returns:
        Timestamp ajustado (se a data cair em fim de semana ou feriado, é ajustada para o próximo dia útil)
    """
    if timestamp is None:
        return None
        
    # Converte timestamp para datetime
    data = datetime.fromtimestamp(timestamp)
    
    # Obtém os feriados nacionais para o ano da data
    feriados = obter_feriados_nacionais(data.year)
    
    # Verifica se é feriado, sábado ou domingo e ajusta para o próximo dia útil
    # Pode ter que avançar mais de um dia, por isso usamos um loop
    while True:
        # Verifica se é sábado (5) ou domingo (6)
        dia_semana = data.weekday()
        
        # Converte a data atual para timestamp para verificar se é feriado
        timestamp_atual = int(data.timestamp())
        
        # Normaliza timestamps para comparar apenas data, ignorando hora
        data_normalizada = datetime(data.year, data.month, data.day).timestamp()
        feriados_normalizados = [
            datetime.fromtimestamp(feriado).replace(hour=0, minute=0, second=0).timestamp()
            for feriado in feriados
        ]
        
        # Verifica se não é final de semana nem feriado
        if dia_semana < 5 and data_normalizada not in feriados_normalizados:
            break
            
        # Adiciona um dia e continua verificando
        data = data + timedelta(days=1)
    
    # Retorna o timestamp ajustado
    return int(data.timestamp())

def obter_titulos_pagos_em_dia(db, cliente_id=None, cod_cliente=None):
    """
    Calcula o percentual de títulos pagos em dia pelo cliente.
    
    Args:
        db: Conexão com o banco de dados
        cliente_id: ID do cliente (opcional)
        cod_cliente: Código do cliente (opcional)
        
    Returns:
        Percentual de títulos pagos em dia ou lista de percentuais se cliente_id/cod_cliente não for fornecido
    """
    try:
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
        
        # Se cliente_id for fornecido, buscamos o código do cliente
        if cliente_id:
            # Busca o código do cliente
            cliente = db.geradores.find_one({"_id": cliente_id})
            if cliente and "cod_cliente" in cliente:
                cod_cliente = cliente["cod_cliente"]
            else:
                return {
                    "codigo_cliente": None,
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
                }
        # Se nem cliente_id nem cod_cliente for fornecido, calcula para todos os clientes
        elif not cod_cliente:
            # Primeiro, obtém a lista de todos os clientes
            clientes = list(db.geradores.find(
                {"cod_cliente": {"$exists": True, "$ne": None}},
                {"_id": 1, "cod_cliente": 1, "razao_social": 1}
            ))
            
            # Lista para armazenar os resultados
            resultados = []
            
            # Calcula o percentual de títulos pagos em dia para cada cliente
            for cliente in clientes:
                cod_cliente = cliente.get("cod_cliente")
                if not cod_cliente:
                    continue
                
                # Como cod_gerador, codigo_cliente_fornecedor e cod_cliente são a mesma informação,
                # buscamos por qualquer um desses campos e aplicamos o filtro base
                filtro_cliente = {
                    "$and": [
                        filtro_base,
                        {"$or": [
                            {"cod_gerador": cod_cliente},
                            {"codigo_cliente_fornecedor": cod_cliente},
                            {"cod_cliente": cod_cliente},
                            {"codigo_cliente": cod_cliente},
                            {"cliente_codigo": cod_cliente}
                        ]}
                    ]
                }
                lancamentos = list(db.lancamentos_completo.find(filtro_cliente))
                
                # Se não houver lançamentos, pula para o próximo cliente
                if not lancamentos:
                    continue
                
                # Inicializa contadores
                total_lancamentos = len(lancamentos)
                total_pagos = 0
                total_a_vencer = 0
                total_vencido = 0
                inadimplente = False
                inadimplente_dias = 0
                inadimplente_valor = 0
                total_a_vencer_valor = 0
                usa_boleto = False
                
                # Contadores para títulos pagos em dia
                pagos_em_dia = 0
                pagos_em_ate_7d = 0
                pagos_em_ate_15d = 0
                pagos_em_ate_30d = 0
                pagos_com_mais_30d = 0
                
                # Data atual para comparação
                data_atual = datetime.now().timestamp()
                
                # Processa cada lançamento
                for lancamento in lancamentos:
                    # Usamos os campos disponíveis conforme identificados no diagnóstico
                    valor_pago_recebido = lancamento.get("valor_pago_recebido")
                    valor_liquido = lancamento.get("valor_liquido")
                    valor_inicial = lancamento.get("valor_inicial")
                    data_vencimento = lancamento.get("data_vencimento")
                    data_pagamento = lancamento.get("data_pagamento")
                    
                    # Ajusta a data de vencimento se cair em fim de semana ou feriado
                    data_vencimento_ajustada = ajustar_data_vencimento(data_vencimento)
                    
                    # Tratamos os campos de tipo_pgto com segurança
                    tipo_pgto = lancamento.get("tipo_pgto", "")
                    if tipo_pgto is not None and not isinstance(tipo_pgto, str):
                        tipo_pgto = str(tipo_pgto)
                    else:
                        tipo_pgto = tipo_pgto or ""
                    
                    tipo_pgto_descricao = lancamento.get("tipo_pgto_descricao", "")
                    if tipo_pgto_descricao is not None and not isinstance(tipo_pgto_descricao, str):
                        tipo_pgto_descricao = str(tipo_pgto_descricao)
                    else:
                        tipo_pgto_descricao = tipo_pgto_descricao or ""
                    
                    efetuado = lancamento.get("efetuado")
                    
                    # Verifica se o cliente usa boleto com base na descrição ou tipo de pagamento
                    if "boleto" in tipo_pgto.lower() or "boleto" in tipo_pgto_descricao.lower():
                        usa_boleto = True
                    
                    # Determina o status com base nos campos disponíveis
                    # Um título foi pago se tiver valor_pago_recebido ou data_pagamento ou efetuado for True
                    foi_pago = valor_pago_recebido is not None or data_pagamento is not None or efetuado is True
                    
                    if foi_pago:
                        total_pagos += 1
                        
                        # Verifica se o lançamento foi pago em dia
                        if data_vencimento_ajustada and data_pagamento:
                            try:
                                # Converte para float se for string
                                if isinstance(data_pagamento, str):
                                    data_pagamento = float(data_pagamento)
                                    
                                # Calcula a diferença em dias
                                diferenca_dias = (data_pagamento - data_vencimento_ajustada) / (24 * 60 * 60)
                                
                                # Verifica se foi pago em dia
                                if diferenca_dias <= 0:
                                    pagos_em_dia += 1
                                elif diferenca_dias <= 7:
                                    pagos_em_ate_7d += 1
                                elif diferenca_dias <= 15:
                                    pagos_em_ate_15d += 1
                                elif diferenca_dias <= 30:
                                    pagos_em_ate_30d += 1
                                else:
                                    pagos_com_mais_30d += 1
                            except (ValueError, TypeError):
                                # Se não conseguir converter, considera como não pago em dia
                                pass
                    
                    # Verifica se o lançamento está a vencer
                    elif data_vencimento_ajustada and data_atual < data_vencimento_ajustada:
                        total_a_vencer += 1
                        
                        # Soma o valor a vencer - tenta diferentes campos disponíveis
                        valor = valor_liquido or valor_inicial or 0
                        if valor:
                            try:
                                if isinstance(valor, str):
                                    valor = float(valor)
                                total_a_vencer_valor += valor
                            except (ValueError, TypeError):
                                pass
                    
                    # Verifica se o lançamento está vencido
                    elif data_vencimento_ajustada and data_atual > data_vencimento_ajustada:
                        total_vencido += 1
                        
                        # Verifica se o cliente está inadimplente
                        try:
                            # Calcula a diferença em dias
                            diferenca_dias = (data_atual - data_vencimento_ajustada) / (24 * 60 * 60)
                            
                            # Atualiza o maior número de dias de inadimplência
                            if diferenca_dias > inadimplente_dias:
                                inadimplente_dias = diferenca_dias
                                inadimplente = True
                            
                            # Soma o valor dos lançamentos vencidos
                            valor = valor_liquido or valor_inicial or 0
                            if valor:
                                try:
                                    if isinstance(valor, str):
                                        valor = float(valor)
                                    inadimplente_valor += valor
                                except (ValueError, TypeError):
                                    pass
                        except (ValueError, TypeError):
                            # Se não conseguir converter, não considera como inadimplente
                            pass
                
                # Calcula os percentuais
                percentual_pagos_total = (total_pagos / total_lancamentos) * 100 if total_lancamentos > 0 else 0
                percentual_a_vencer = (total_a_vencer / total_lancamentos) * 100 if total_lancamentos > 0 else 0
                percentual_vencido = (total_vencido / total_lancamentos) * 100 if total_lancamentos > 0 else 0
                
                # Calcula os percentuais de títulos pagos em dia
                percentual_pagos_em_dia = (pagos_em_dia / total_pagos) * 100 if total_pagos > 0 else 0
                percentual_pagos_em_ate_7d = (pagos_em_ate_7d / total_pagos) * 100 if total_pagos > 0 else 0
                percentual_pagos_em_ate_15d = (pagos_em_ate_15d / total_pagos) * 100 if total_pagos > 0 else 0
                percentual_pagos_em_ate_30d = (pagos_em_ate_30d / total_pagos) * 100 if total_pagos > 0 else 0
                percentual_pagos_com_mais_30d = (pagos_com_mais_30d / total_pagos) * 100 if total_pagos > 0 else 0
                
                resultados.append({
                    "id": cliente.get("_id"),
                    "codigo_cliente": cod_cliente,
                    "nome": cliente.get("razao_social", ""),
                    "total_lancamentos": total_lancamentos,
                    "total_pagos": total_pagos,
                    "total_a_vencer": total_a_vencer,
                    "total_vencido": total_vencido,
                    "percentual_pagos_total": round(percentual_pagos_total, 2),
                    "percentual_a_vencer": round(percentual_a_vencer, 2),
                    "percentual_vencido": round(percentual_vencido, 2),
                    "inadimplente": inadimplente,
                    "inadimplente_dias": int(inadimplente_dias),
                    "inadimplente_valor": round(inadimplente_valor, 2),
                    "total_a_vencer_valor": round(total_a_vencer_valor, 2),
                    "usa_boleto": usa_boleto,
                    "pagos_em_dia": pagos_em_dia,
                    "percentual_pagos_em_dia": round(percentual_pagos_em_dia, 2),
                    "pagos_em_ate_7d": pagos_em_ate_7d,
                    "percentual_pagos_em_ate_7d": round(percentual_pagos_em_ate_7d, 2),
                    "pagos_em_ate_15d": pagos_em_ate_15d,
                    "percentual_pagos_em_ate_15d": round(percentual_pagos_em_ate_15d, 2),
                    "pagos_em_ate_30d": pagos_em_ate_30d,
                    "percentual_pagos_em_ate_30d": round(percentual_pagos_em_ate_30d, 2),
                    "pagos_com_mais_30d": pagos_com_mais_30d,
                    "percentual_pagos_com_mais_30d": round(percentual_pagos_com_mais_30d, 2)
                })
            
            return resultados
        
        # Como cod_gerador, codigo_cliente_fornecedor e cod_cliente são a mesma informação,
        # buscamos por qualquer um desses campos e aplicamos o filtro base
        filtro_cliente = {
            "$and": [
                filtro_base,
                {"$or": [
                    {"cod_gerador": cod_cliente},
                    {"codigo_cliente_fornecedor": cod_cliente},
                    {"cod_cliente": cod_cliente},
                    {"codigo_cliente": cod_cliente},
                    {"cliente_codigo": cod_cliente}
                ]}
            ]
        }
        
        lancamentos = list(db.lancamentos_completo.find(filtro_cliente))
        
        # Se não houver lançamentos, retorna zeros
        if not lancamentos:
            return {
                "codigo_cliente": cod_cliente,
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
            }
        
        # Inicializa contadores
        total_lancamentos = len(lancamentos)
        total_pagos = 0
        total_a_vencer = 0
        total_vencido = 0
        inadimplente = False
        inadimplente_dias = 0
        inadimplente_valor = 0
        total_a_vencer_valor = 0
        usa_boleto = False
        
        # Contadores para títulos pagos em dia
        pagos_em_dia = 0
        pagos_em_ate_7d = 0
        pagos_em_ate_15d = 0
        pagos_em_ate_30d = 0
        pagos_com_mais_30d = 0
        
        # Data atual para comparação
        data_atual = datetime.now().timestamp()
        
        # Processa cada lançamento
        for lancamento in lancamentos:
            # Usamos os campos disponíveis conforme identificados no diagnóstico
            valor_pago_recebido = lancamento.get("valor_pago_recebido")
            valor_liquido = lancamento.get("valor_liquido")
            valor_inicial = lancamento.get("valor_inicial")
            data_vencimento = lancamento.get("data_vencimento")
            data_pagamento = lancamento.get("data_pagamento")
            
            # Ajusta a data de vencimento se cair em fim de semana ou feriado
            data_vencimento_ajustada = ajustar_data_vencimento(data_vencimento)
            
            # Tratamos os campos de tipo_pgto com segurança
            tipo_pgto = lancamento.get("tipo_pgto", "")
            if tipo_pgto is not None and not isinstance(tipo_pgto, str):
                tipo_pgto = str(tipo_pgto)
            else:
                tipo_pgto = tipo_pgto or ""
            
            tipo_pgto_descricao = lancamento.get("tipo_pgto_descricao", "")
            if tipo_pgto_descricao is not None and not isinstance(tipo_pgto_descricao, str):
                tipo_pgto_descricao = str(tipo_pgto_descricao)
            else:
                tipo_pgto_descricao = tipo_pgto_descricao or ""
            
            efetuado = lancamento.get("efetuado")
            
            # Verifica se o cliente usa boleto com base na descrição ou tipo de pagamento
            if "boleto" in tipo_pgto.lower() or "boleto" in tipo_pgto_descricao.lower():
                usa_boleto = True
            
            # Determina o status com base nos campos disponíveis
            # Um título foi pago se tiver valor_pago_recebido ou data_pagamento ou efetuado for True
            foi_pago = valor_pago_recebido is not None or data_pagamento is not None or efetuado is True
            
            if foi_pago:
                total_pagos += 1
                
                # Verifica se o lançamento foi pago em dia
                if data_vencimento_ajustada and data_pagamento:
                    try:
                        # Converte para float se for string
                        if isinstance(data_pagamento, str):
                            data_pagamento = float(data_pagamento)
                            
                        # Calcula a diferença em dias
                        diferenca_dias = (data_pagamento - data_vencimento_ajustada) / (24 * 60 * 60)
                        
                        # Verifica se foi pago em dia
                        if diferenca_dias <= 0:
                            pagos_em_dia += 1
                        elif diferenca_dias <= 7:
                            pagos_em_ate_7d += 1
                        elif diferenca_dias <= 15:
                            pagos_em_ate_15d += 1
                        elif diferenca_dias <= 30:
                            pagos_em_ate_30d += 1
                        else:
                            pagos_com_mais_30d += 1
                    except (ValueError, TypeError):
                        # Se não conseguir converter, considera como não pago em dia
                        pass
            
            # Verifica se o lançamento está a vencer
            elif data_vencimento_ajustada and data_atual < data_vencimento_ajustada:
                total_a_vencer += 1
                
                # Soma o valor a vencer - tenta diferentes campos disponíveis
                valor = valor_liquido or valor_inicial or 0
                if valor:
                    try:
                        if isinstance(valor, str):
                            valor = float(valor)
                        total_a_vencer_valor += valor
                    except (ValueError, TypeError):
                        pass
            
            # Verifica se o lançamento está vencido
            elif data_vencimento_ajustada and data_atual > data_vencimento_ajustada:
                total_vencido += 1
                
                # Verifica se o cliente está inadimplente
                try:
                    # Calcula a diferença em dias
                    diferenca_dias = (data_atual - data_vencimento_ajustada) / (24 * 60 * 60)
                    
                    # Atualiza o maior número de dias de inadimplência
                    if diferenca_dias > inadimplente_dias:
                        inadimplente_dias = diferenca_dias
                        inadimplente = True
                    
                    # Soma o valor dos lançamentos vencidos
                    valor = valor_liquido or valor_inicial or 0
                    if valor:
                        try:
                            if isinstance(valor, str):
                                valor = float(valor)
                            inadimplente_valor += valor
                        except (ValueError, TypeError):
                            pass
                except (ValueError, TypeError):
                    # Se não conseguir converter, não considera como inadimplente
                    pass
        
        # Calcula os percentuais
        percentual_pagos_total = (total_pagos / total_lancamentos) * 100 if total_lancamentos > 0 else 0
        percentual_a_vencer = (total_a_vencer / total_lancamentos) * 100 if total_lancamentos > 0 else 0
        percentual_vencido = (total_vencido / total_lancamentos) * 100 if total_lancamentos > 0 else 0
        
        # Calcula os percentuais de títulos pagos em dia
        percentual_pagos_em_dia = (pagos_em_dia / total_pagos) * 100 if total_pagos > 0 else 0
        percentual_pagos_em_ate_7d = (pagos_em_ate_7d / total_pagos) * 100 if total_pagos > 0 else 0
        percentual_pagos_em_ate_15d = (pagos_em_ate_15d / total_pagos) * 100 if total_pagos > 0 else 0
        percentual_pagos_em_ate_30d = (pagos_em_ate_30d / total_pagos) * 100 if total_pagos > 0 else 0
        percentual_pagos_com_mais_30d = (pagos_com_mais_30d / total_pagos) * 100 if total_pagos > 0 else 0
        
        return {
            "codigo_cliente": cod_cliente,
            "total_lancamentos": total_lancamentos,
            "total_pagos": total_pagos,
            "total_a_vencer": total_a_vencer,
            "total_vencido": total_vencido,
            "percentual_pagos_total": round(percentual_pagos_total, 2),
            "percentual_a_vencer": round(percentual_a_vencer, 2),
            "percentual_vencido": round(percentual_vencido, 2),
            "inadimplente": inadimplente,
            "inadimplente_dias": int(inadimplente_dias),
            "inadimplente_valor": round(inadimplente_valor, 2),
            "total_a_vencer_valor": round(total_a_vencer_valor, 2),
            "usa_boleto": usa_boleto,
            "pagos_em_dia": pagos_em_dia,
            "percentual_pagos_em_dia": round(percentual_pagos_em_dia, 2),
            "pagos_em_ate_7d": pagos_em_ate_7d,
            "percentual_pagos_em_ate_7d": round(percentual_pagos_em_ate_7d, 2),
            "pagos_em_ate_15d": pagos_em_ate_15d,
            "percentual_pagos_em_ate_15d": round(percentual_pagos_em_ate_15d, 2),
            "pagos_em_ate_30d": pagos_em_ate_30d,
            "percentual_pagos_em_ate_30d": round(percentual_pagos_em_ate_30d, 2),
            "pagos_com_mais_30d": pagos_com_mais_30d,
            "percentual_pagos_com_mais_30d": round(percentual_pagos_com_mais_30d, 2)
        }
    
    except Exception as e:
        print(f"Erro ao calcular títulos pagos em dia: {e}")
        return {
            "codigo_cliente": cod_cliente,
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
        }
