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

# Importa a funcionalidade de classificação
from classificacao.classificar import classificar_cliente

# Importa o módulo de envio para MongoDB
import enviar_para_mongodb

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
        resultado_cliente["numero_marcas_diferentes"] = marcas.get("total_marcas", 0)
        resultado_cliente["lista_marcas"] = marcas.get("lista_marcas", [])
    
    # NOVA FUNCIONALIDADE: Realiza a classificação do cliente
    log(f"  Classificando cliente {cod_cliente} - {nome_cliente}...", nivel=2)
    try:
        resultado_classificacao = classificar_cliente(resultado_cliente)
        
        # Adiciona a categoria diretamente ao nível principal do resultado
        resultado_cliente["categoria"] = resultado_classificacao["categoria"]
        
        # Adiciona os detalhes completos da classificação
        resultado_cliente["classificacao"] = resultado_classificacao
        
        log(f"  Classificação concluída: {resultado_classificacao['categoria']} ({resultado_classificacao['pontuacao_final']})", nivel=2)
    except Exception as e:
        log(f"  [ERRO] Falha ao classificar cliente: {str(e)}", nivel=2)
        resultado_cliente["categoria"] = "Bronze"
        resultado_cliente["classificacao"] = {
            "erro": str(e),
            "categoria": "Bronze",
            "pontuacao_final": 0
        }
    
    return resultado_cliente

def main():
    """Função principal para processar clientes."""
    try:
        # Conecta ao MongoDB
        db = conectar_mongodb()
        
        if db is None:
            log("Não foi possível estabelecer conexão com o MongoDB.", sempre_mostrar=True)
            return
        
        # Se processar todos, faz a consulta para todos os clientes
        if PROCESSAR_TODOS:
            log("Processando todos os clientes com movimentações...", sempre_mostrar=True)
            
            # Obtém a lista completa de clientes com movimentações
            log("Obtendo lista de clientes com movimentações...")
            clientes_com_movimentacao = obter_clientes_com_movimentacao(db)
            
            if clientes_com_movimentacao:
                # Converte o conjunto em lista para permitir fatiamento
                clientes_com_movimentacao = list(clientes_com_movimentacao)
                total_clientes = len(clientes_com_movimentacao)
                log(f"Total de clientes com movimentações: {total_clientes}", sempre_mostrar=True)
                
                # Cria o diretório de resultados se não existir
                os.makedirs("resultados", exist_ok=True)
                os.makedirs("resultados/lotes", exist_ok=True)
                
                # Processa os clientes em lotes
                resultados_totais = []
                lote_atual = 1
                primeiro_lote = True
                
                for i in range(0, total_clientes, TAMANHO_LOTE):
                    # Define o lote atual
                    codigos_clientes_lote = clientes_com_movimentacao[i:i+TAMANHO_LOTE]
                    total_no_lote = len(codigos_clientes_lote)
                    
                    log(f"Processando lote {lote_atual} ({total_no_lote} clientes)...", sempre_mostrar=True)
                    
                    # Lista para armazenar os resultados do lote atual
                    resultados_lote = []
                    
                    # Contador para acompanhar o progresso no lote
                    contador = 0
                    
                    # Processa cada cliente no lote atual
                    for cod_cliente in codigos_clientes_lote:
                        # Busca informações completas do cliente pelo código
                        cliente = db.geradores.find_one({"cod_cliente": cod_cliente})
                        if not cliente:
                            log(f"Cliente com código {cod_cliente} não encontrado no banco.")
                            contador += 1
                            continue
                            
                        cliente_id = cliente["_id"]
                        nome_cliente = cliente.get("razao_social", "")
                        
                        log(f"Processando cliente {contador+1}/{total_no_lote} do lote {lote_atual}: {cod_cliente} - {nome_cliente}")
                        
                        try:
                            resultado = processar_cliente_individual(db, cliente_id, usar_cache=USAR_CACHE)
                            if resultado:
                                resultados_lote.append(resultado)
                                resultados_totais.append(resultado)
                        except Exception as e:
                            log(f"Erro ao processar cliente {cod_cliente}: {e}")
                            log(traceback.format_exc())
                        
                        contador += 1
                        
                        # Exibe progresso a cada 10 clientes
                        if contador % 10 == 0 or contador == total_no_lote:
                            percentual_lote = (contador / total_no_lote) * 100
                            percentual_total = ((i + contador) / total_clientes) * 100
                            log(f"Progresso: {contador}/{total_no_lote} clientes no lote ({percentual_lote:.1f}%) | " + 
                                f"Total: {i+contador}/{total_clientes} ({percentual_total:.1f}%)", sempre_mostrar=True)
                    
                    # Salva os resultados do lote atual em um arquivo JSON
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    nome_arquivo_lote = f"resultados/lotes/resultado_lote_{lote_atual}_{timestamp}.json"
                    
                    with open(nome_arquivo_lote, "w", encoding="utf-8") as f:
                        json.dump(resultados_lote, f, default=json_util.default, ensure_ascii=False, indent=2)
                    
                    log(f"Resultados do lote {lote_atual} salvos em '{nome_arquivo_lote}'", sempre_mostrar=True)
                    log(f"Total de clientes processados no lote {lote_atual}: {len(resultados_lote)}", sempre_mostrar=True)
                    
                    # Envia os resultados do lote para o MongoDB
                    log(f"Enviando resultados do lote {lote_atual} para o MongoDB...", sempre_mostrar=True)
                    diretorio_lote = os.path.dirname(nome_arquivo_lote)
                    limpar_collection = primeiro_lote  # Limpa apenas no primeiro lote
                    
                    # Chama a função de envio para MongoDB e aguarda a conclusão
                    envio_sucesso = enviar_para_mongodb.main(
                        limpar_collection_antes=limpar_collection, 
                        diretorio_resultados=diretorio_lote
                    )
                    
                    if envio_sucesso:
                        log(f"Lote {lote_atual} enviado com sucesso para o MongoDB.", sempre_mostrar=True)
                    else:
                        log(f"Erro ao enviar lote {lote_atual} para o MongoDB. Interrompendo processamento.", sempre_mostrar=True)
                        # Se o envio falhar, interrompe o processamento
                        break
                    
                    # Incrementa o contador de lotes e marca que não é mais o primeiro lote
                    lote_atual += 1
                    primeiro_lote = False
                    
                    # Aguarda um momento antes de iniciar o próximo lote para não sobrecarregar o sistema
                    log(f"Aguardando 2 segundos antes de iniciar o próximo lote...", sempre_mostrar=True)
                    time.sleep(2)
                
                # Após processar todos os lotes, salva o resultado completo em um único arquivo
                timestamp_final = datetime.now().strftime("%Y%m%d_%H%M%S")
                nome_arquivo_completo = f"resultados/resultado_completo_{timestamp_final}.json"
                
                with open(nome_arquivo_completo, "w", encoding="utf-8") as f:
                    json.dump(resultados_totais, f, default=json_util.default, ensure_ascii=False, indent=2)
                
                log(f"Resultados completos salvos em '{nome_arquivo_completo}'", sempre_mostrar=True)
                log(f"Total de clientes processados: {len(resultados_totais)}", sempre_mostrar=True)
                
                # Apaga os arquivos de lotes temporários
                try:
                    log("Removendo arquivos de lotes temporários...", sempre_mostrar=True)
                    arquivos_lote = glob.glob(os.path.join("resultados/lotes", "resultado_lote_*.json"))
                    contador_removidos = 0
                    
                    for arquivo in arquivos_lote:
                        try:
                            os.remove(arquivo)
                            contador_removidos += 1
                        except Exception as e:
                            log(f"Erro ao remover arquivo {arquivo}: {e}")
                    
                    log(f"Foram removidos {contador_removidos} arquivos de lotes temporários", sempre_mostrar=True)
                except Exception as e:
                    log(f"Erro ao remover arquivos de lotes: {e}", sempre_mostrar=True)
            
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
                    
                    # Cria o diretório de resultados se não existir
                    os.makedirs("resultados", exist_ok=True)
                    
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
                    
                    # Envia o resultado para o MongoDB
                    log(f"Enviando resultado para o MongoDB...", sempre_mostrar=True)
                    
                    # Chama a função de envio para MongoDB
                    envio_sucesso = enviar_para_mongodb.main(
                        limpar_collection_antes=True, 
                        diretorio_resultados="resultados"
                    )
                    
                    if envio_sucesso:
                        log(f"Resultado enviado com sucesso para o MongoDB.", sempre_mostrar=True)
                    else:
                        log(f"Erro ao enviar resultado para o MongoDB.", sempre_mostrar=True)
                
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
