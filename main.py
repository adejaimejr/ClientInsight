"""
Sistema de Extração de Dados do ERP - Implementação Principal
Este módulo contém as implementações principais para extração e processamento de dados do MongoDB.
"""
import os
import json
import time
import traceback
from datetime import datetime
import glob
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import json_util

# Importa as consultas do pacote consultas
from consultas.base import verificar_cliente_tem_movimentacao, obter_clientes_com_movimentacao
from consultas.faturamento import obter_faturamento_ultimos_12_meses
from consultas.ciclos_compra import obter_ciclos_compra_ultimos_6_meses
from consultas.pecas_compradas import obter_total_pecas_compradas
from consultas.titulos_pagos import obter_titulos_pagos_em_dia
from consultas.valor_por_marca import obter_valor_por_marca, obter_numero_marcas_diferentes
from consultas.data_primeira_compra import obter_data_primeira_compra
from consultas.cliente import obter_codigo_cliente, obter_nome_completo

# Carrega as variáveis de ambiente
load_dotenv()

# Configuração da conexão com o MongoDB
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")

# Configurações de processamento
CLIENTE_TESTE = os.getenv("CLIENTE_TESTE")
PROCESSAR_TODOS = os.getenv("PROCESSAR_TODOS", "false").lower() == "true"
TAMANHO_LOTE = int(os.getenv("TAMANHO_LOTE", "20"))
USAR_CACHE = os.getenv("USAR_CACHE", "false").lower() == "true"
USAR_PARALELO = os.getenv("USAR_PARALELO", "false").lower() == "true"
NUM_THREADS = int(os.getenv("NUM_THREADS", "2"))

# Configuração de logs
MOSTRAR_LOGS = os.getenv("MOSTRAR_LOGS", "true").lower() == "true"

def log(mensagem, nivel=0, sempre_mostrar=False):
    """
    Função para exibir logs com base na configuração.
    
    Args:
        mensagem: Mensagem a ser exibida
        nivel: Nível de indentação (0=sem indentação, 1=4 espaços, etc.)
        sempre_mostrar: Se True, mostra a mensagem mesmo com MOSTRAR_LOGS=false
    """
    if MOSTRAR_LOGS or sempre_mostrar:
        indentacao = "    " * nivel
        print(f"{indentacao}{mensagem}")

def conectar_mongodb():
    """Estabelece conexão com o MongoDB."""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DATABASE]
        log(f"Conexão estabelecida com o banco de dados: {MONGODB_DATABASE}", sempre_mostrar=True)
        return db
    except Exception as e:
        log(f"Erro ao conectar ao MongoDB: {e}", sempre_mostrar=True)
        return None

def processar_cliente_individual(db, cliente_id, usar_cache=True):
    """
    Processa um cliente individual, executando todas as consultas necessárias.
    
    Args:
        db: Conexão com o banco de dados MongoDB
        cliente_id: ID do cliente a ser processado
        usar_cache: Se True, usa cache para consultas já realizadas
        
    Returns:
        Dicionário com todas as informações consolidadas do cliente
    """
    if db is None:
        return None
        
    # Obtém informações básicas do cliente
    cliente = db.geradores.find_one({"_id": cliente_id})
    if not cliente:
        log(f"Cliente com ID {cliente_id} não encontrado.")
        return None
        
    cod_cliente = cliente.get("cod_cliente")
    nome_cliente = cliente.get("razao_social", "")
    data_cadastro_timestamp = cliente.get("data_cadastro")
    
    # Formata a data de cadastro se disponível
    data_cadastro_formatada = None
    if data_cadastro_timestamp:
        try:
            data_cadastro_formatada = datetime.fromtimestamp(data_cadastro_timestamp).strftime("%Y-%m-%d")
        except:
            pass
    
    # VALIDAÇÃO CRÍTICA: Verifica se o cliente realmente tem movimentações
    # Verifica se existem movimentações para este cliente
    if not verificar_cliente_tem_movimentacao(db, cod_cliente):
        log(f"  [AVISO] Cliente {cod_cliente} - {nome_cliente} não possui movimentações. Ignorando.", nivel=1)
        return None
    
    # Inicializa o dicionário de resultados para o cliente
    resultado_cliente = {
        "id": cliente_id,
        "codigo_cliente": cod_cliente,
        "nome_completo": nome_cliente,
        "data_cadastro": data_cadastro_formatada,
        "data_cadastro_timestamp": data_cadastro_timestamp,
        "data_primeira_compra": None,
        "data_primeira_compra_timestamp": None,
        "data_ultima_compra": None,
        "data_ultima_compra_timestamp": None,
        "limite_credito": cliente.get("limite_credito", 0),
        "limite_credito_utilizado": cliente.get("limite_utilizado", 0),
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
    }
    
    # Consulta 3: Data da primeira compra
    log("Obtendo data da primeira compra...", nivel=2)
    data_primeira_compra = obter_data_primeira_compra(db, cliente_id=cliente_id)
    if data_primeira_compra:
        resultado_cliente["data_primeira_compra"] = data_primeira_compra.get("data_formatada")
        resultado_cliente["data_primeira_compra_timestamp"] = data_primeira_compra.get("data")
        
        # Adiciona a data da última compra formatada
        data_ultima_compra_timestamp = data_primeira_compra.get("data_ultima_compra")
        resultado_cliente["data_ultima_compra_timestamp"] = data_ultima_compra_timestamp
        
        # Formata a data da última compra se disponível
        if data_ultima_compra_timestamp:
            try:
                resultado_cliente["data_ultima_compra"] = datetime.fromtimestamp(data_ultima_compra_timestamp).strftime("%Y-%m-%d")
            except:
                pass
    
    # Consulta 4: Faturamento total nos últimos 12 meses
    log("Calculando faturamento...", nivel=2)
    faturamento = obter_faturamento_ultimos_12_meses(db, cliente_id=cliente_id)
    if faturamento:
        resultado_cliente["faturamento_ultimos_12_meses"] = {
            "total_vendas": faturamento.get("total_vendas", 0),
            "total_devolucoes": faturamento.get("total_devolucoes", 0),
            "faturamento_liquido": faturamento.get("faturamento_liquido", 0)
        }
    
    # Consulta 5: Número de ciclos em que comprou nos últimos 6 meses
    log("Calculando ciclos de compra...", nivel=2)
    ciclos = obter_ciclos_compra_ultimos_6_meses(db, cliente_id=cliente_id)
    if ciclos:
        resultado_cliente["ciclos_compra_ultimos_6_meses"] = ciclos.get("num_ciclos_6_meses", 0)
        resultado_cliente["ciclo_atual"] = ciclos.get("comprou_ciclo_atual", False)
        resultado_cliente["meses_compra"] = ciclos.get("meses_compra_6_meses", [])
    
    # Consulta 6: Número total de peças compradas
    log("Calculando total de peças...", nivel=2)
    pecas = obter_total_pecas_compradas(db, cliente_id=cliente_id)
    if pecas:
        resultado_cliente["total_pecas"] = {
            "compradas": pecas.get("total_bruto", 0),
            "devolvidas": pecas.get("total_devolucoes", 0),
            "liquido": pecas.get("total_liquido", 0)
        }
    
    # Consulta 7: Total de títulos pagos em dia
    log("Calculando títulos pagos em dia...", nivel=2)
    pagamentos = obter_titulos_pagos_em_dia(db, cliente_id=cliente_id)
    
    # Adiciona informações sobre limite de crédito
    cliente_obj = db.geradores.find_one({"_id": cliente_id})
    limite_credito = cliente_obj.get("limite_credito", 0) if cliente_obj else 0
    
    if pagamentos:
        # Calcula o limite de crédito utilizado (total_a_vencer_valor + inadimplente_valor)
        limite_credito_utilizado = pagamentos.get("total_a_vencer_valor", 0) + pagamentos.get("inadimplente_valor", 0)
        
        resultado_cliente["titulos_pagos_em_dia"] = pagamentos
        resultado_cliente["limite_credito"] = limite_credito
        resultado_cliente["limite_credito_utilizado"] = limite_credito_utilizado
    
    # Consulta 8: Valor total por marca
    log("Calculando valor por marca...", nivel=2)
    valor_por_marca = obter_valor_por_marca(db, cliente_id=cliente_id)
    if valor_por_marca:
        resultado_cliente["valor_por_marca"] = valor_por_marca
    
    # Consulta 9: Número de marcas diferentes
    log("Calculando número de marcas diferentes...", nivel=2)
    marcas = obter_numero_marcas_diferentes(db, cliente_id=cliente_id)
    if marcas:
        resultado_cliente["numero_marcas_diferentes"] = marcas.get("numero_marcas", 0)
        resultado_cliente["lista_marcas"] = marcas.get("lista_marcas", [])
    
    return resultado_cliente

def main():
    """Função principal para processar clientes."""
    try:
        # Conecta ao MongoDB
        db = conectar_mongodb()
        if db is None:
            log("Falha ao conectar ao banco de dados MongoDB.", sempre_mostrar=True)
            return
        
        # Cria o diretório de resultados se não existir
        if not os.path.exists("resultados"):
            os.makedirs("resultados")
        
        if PROCESSAR_TODOS:
            log("Processando todos os clientes...", sempre_mostrar=True)
            
            # Obtém lista de clientes com movimentações
            clientes_com_movimentacao = obter_clientes_com_movimentacao(db)
            
            if clientes_com_movimentacao:
                log(f"Total de clientes com movimentações: {len(clientes_com_movimentacao)}", sempre_mostrar=True)
                
                # Converte o conjunto em lista para permitir fatiamento
                codigos_clientes = list(clientes_com_movimentacao)
                
                # Divide em lotes para processamento mais eficiente
                total_clientes = len(codigos_clientes)
                num_lotes = (total_clientes + TAMANHO_LOTE - 1) // TAMANHO_LOTE
                
                resultados_totais = []
                total_clientes_processados = 0
                
                for i in range(num_lotes):
                    inicio = i * TAMANHO_LOTE
                    fim = min((i + 1) * TAMANHO_LOTE, total_clientes)
                    
                    log(f"Processando lote {i+1}/{num_lotes} ({inicio+1}-{fim} de {total_clientes})...", sempre_mostrar=True)
                    
                    lote_atual = codigos_clientes[inicio:fim]
                    resultados_lote = []
                    
                    for j, cod_cliente in enumerate(lote_atual):
                        # Busca informações completas do cliente pelo código
                        cliente = db.geradores.find_one({"cod_cliente": cod_cliente})
                        if not cliente:
                            log(f"Cliente com código {cod_cliente} não encontrado no banco de dados.")
                            continue
                            
                        cliente_id = cliente["_id"]
                        nome_cliente = cliente.get("razao_social", "")
                        
                        # Incrementa o contador total de clientes processados
                        total_clientes_processados += 1
                        
                        # Log detalhado (apenas se MOSTRAR_LOGS=true)
                        log(f"Processando cliente {j+1}/{len(lote_atual)}: {cod_cliente} - {nome_cliente}")
                        
                        # Log mínimo (sempre mostra independente de MOSTRAR_LOGS)
                        if total_clientes_processados % 50 == 0 or j == 0:  # A cada 50 clientes ou no início do lote
                            log(f"Processando cliente {total_clientes_processados}/{total_clientes}", sempre_mostrar=True)
                        
                        resultado = processar_cliente_individual(db, cliente_id)
                        if resultado:
                            resultados_lote.append(resultado)
                    
                    # Salva resultados parciais do lote
                    nome_arquivo_lote = f"resultados/resultados_parciais_lote_{i+1}.json"
                    with open(nome_arquivo_lote, "w", encoding="utf-8") as f:
                        json.dump(resultados_lote, f, default=json_util.default, ensure_ascii=False, indent=2)
                    
                    log(f"Resultados parciais do lote {i+1} salvos em '{nome_arquivo_lote}'")
                    
                    # Adiciona aos resultados totais
                    resultados_totais.extend(resultados_lote)
                
                # Salva resultados completos
                nome_arquivo_completo = "resultados/resultados_completos.json"
                with open(nome_arquivo_completo, "w", encoding="utf-8") as f:
                    json.dump(resultados_totais, f, default=json_util.default, ensure_ascii=False, indent=2)
                
                log(f"Resultados completos salvos em '{nome_arquivo_completo}'", sempre_mostrar=True)
                log(f"Total de clientes processados: {len(resultados_totais)}", sempre_mostrar=True)
            
            else:
                log("Nenhum cliente com movimentações encontrado.", sempre_mostrar=True)
        
        else:
            # Processa apenas o cliente de teste
            log(f"Processando cliente de teste: {CLIENTE_TESTE}", sempre_mostrar=True)
            
            # Localiza o cliente pelo código
            cliente = db.geradores.find_one({"cod_cliente": CLIENTE_TESTE})
            
            if cliente:
                cliente_id = cliente["_id"]
                nome_cliente = cliente.get("razao_social", "")
                
                log(f"Processando cliente: {CLIENTE_TESTE} - {nome_cliente}", sempre_mostrar=True)
                
                resultado = processar_cliente_individual(db, cliente_id)
                
                if resultado:
                    # Gera um timestamp para o nome do arquivo
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    nome_arquivo = f"resultados/resultado_{CLIENTE_TESTE}_{timestamp}.json"
                    
                    with open(nome_arquivo, "w", encoding="utf-8") as f:
                        json.dump(resultado, f, default=json_util.default, ensure_ascii=False, indent=2)
                    
                    log(f"Resultado salvo em '{nome_arquivo}'", sempre_mostrar=True)
                    
                    # Exibe algumas informações para verificação
                    if MOSTRAR_LOGS and resultado:
                        faturamento = resultado.get("faturamento_ultimos_12_meses", {}).get("faturamento_liquido", 0)
                        ciclos = resultado.get("ciclos_compra_ultimos_6_meses", 0)
                        total_pecas = resultado.get("total_pecas", {}).get("liquido", 0)
                        marcas_diferentes = resultado.get("numero_marcas_diferentes", 0)
                        lista_marcas = resultado.get("lista_marcas", [])
                        
                        log(f"  Faturamento últimos 12 meses: {faturamento:.2f}")
                        log(f"  Ciclos de compra últimos 6 meses: {ciclos}")
                        log(f"  Total de peças compradas: {total_pecas}")
                        log(f"   Número de marcas diferentes: {marcas_diferentes}")
                        log(f"   Marcas: {', '.join(lista_marcas)}")
                
                else:
                    log(f"Não foi possível processar o cliente {CLIENTE_TESTE}.", sempre_mostrar=True)
            
            else:
                log(f"Cliente com código {CLIENTE_TESTE} não encontrado.", sempre_mostrar=True)
    
    except Exception as e:
        log(f"Erro durante o processamento: {e}", sempre_mostrar=True)
        log(traceback.format_exc(), sempre_mostrar=True)

if __name__ == "__main__":
    # Sempre mostra a hora de início, independente da configuração de log
    start_time = time.time()
    start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[INÍCIO] Processamento iniciado em: {start_datetime}")
    
    # Executa o processamento principal
    main()
    
    # Sempre mostra a hora de término e o tempo total, independente da configuração de log
    end_time = time.time()
    end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total_time = end_time - start_time
    
    print(f"\n[TÉRMINO] Processamento concluído em: {end_datetime}")
    print(f"Tempo total de processamento: {total_time:.2f} segundos")
    if total_time > 60:
        print(f"                             {total_time/60:.2f} minutos")
    if total_time > 3600:
        print(f"                             {total_time/3600:.2f} horas")
