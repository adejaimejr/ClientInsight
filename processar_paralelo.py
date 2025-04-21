"""
Sistema de Extração de Dados do ERP - Processamento Paralelo
Este módulo implementa o processamento paralelo de clientes para otimizar o tempo de execução.
"""
import os
import json
import time
import traceback
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import json_util
import concurrent.futures
import threading
import glob

# Importa as funções do módulo principal
from main import processar_cliente_individual, conectar_mongodb
from consultas.base import obter_clientes_com_movimentacao

# Carrega as variáveis de ambiente
load_dotenv()

# Configuração da conexão com o MongoDB
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")

# Configurações de processamento
TAMANHO_LOTE = int(os.getenv("TAMANHO_LOTE", "10"))
NUM_THREADS = int(os.getenv("NUM_THREADS", "4"))
USAR_CACHE = os.getenv("USAR_CACHE", "false").lower() == "true"

# Lock para escrita em arquivos
file_lock = threading.Lock()

def processar_grupo_clientes(db, grupo_clientes, grupo_id):
    """
    Processa um grupo de clientes em paralelo.
    
    Args:
        db: Conexão com o banco de dados
        grupo_clientes: Lista de clientes a serem processados
        grupo_id: ID do grupo para identificação
        
    Returns:
        Lista de resultados dos clientes processados
    """
    print(f"Iniciando processamento do grupo {grupo_id} com {len(grupo_clientes)} clientes...")
    inicio_grupo = time.time()
    
    # Cria a pasta de resultados se não existir
    resultados_dir = os.path.join(os.getcwd(), "resultados")
    if not os.path.exists(resultados_dir):
        os.makedirs(resultados_dir)
    
    # Cria a pasta temporária se não existir
    temp_dir = os.path.join(resultados_dir, "temp")
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)
    
    # Lista para armazenar os resultados
    resultados = []
    
    # Contador para salvamento parcial
    contador = 0
    
    # Processa cada cliente do grupo
    for cliente in grupo_clientes:
        inicio_cliente = time.time()
        cliente_id = cliente.get("_id")
        cod_cliente = cliente.get("cod_cliente")
        nome_cliente = cliente.get("razao_social", "")
        
        print(f"  [Grupo {grupo_id}] Processando cliente: {cod_cliente} - {nome_cliente}")
        
        # Processa o cliente individualmente
        resultado = processar_cliente_individual(db, cliente_id, USAR_CACHE)
        if resultado:
            resultados.append(resultado)
            contador += 1
            
            # Salva resultados parciais a cada 10 clientes
            if contador % 10 == 0:
                with file_lock:
                    arquivo_parcial = os.path.join(temp_dir, f'resultados_parciais_grupo_{grupo_id}.json')
                    with open(arquivo_parcial, 'w', encoding='utf-8') as f:
                        json.dump(resultados, f, default=json_util.default, ensure_ascii=False, indent=2)
                    print(f"  [Grupo {grupo_id}] Resultados parciais salvos com {contador} clientes processados.")
        
        fim_cliente = time.time()
        print(f"  [Grupo {grupo_id}] Cliente processado em {fim_cliente - inicio_cliente:.2f} segundos.")
    
    # Salva os resultados finais do grupo
    with file_lock:
        arquivo_grupo = os.path.join(temp_dir, f'resultados_grupo_{grupo_id}.json')
        with open(arquivo_grupo, 'w', encoding='utf-8') as f:
            json.dump(resultados, f, default=json_util.default, ensure_ascii=False, indent=2)
    
    fim_grupo = time.time()
    print(f"Grupo {grupo_id} concluído em {fim_grupo - inicio_grupo:.2f} segundos. Total de {len(resultados)} clientes processados.")
    
    return resultados

def processar_clientes_paralelo(num_threads=NUM_THREADS, tamanho_lote=TAMANHO_LOTE, limite_clientes=None):
    """
    Processa os clientes em paralelo usando múltiplas threads.
    
    Args:
        num_threads: Número de threads para processamento paralelo
        tamanho_lote: Tamanho do lote para processamento em lotes
        limite_clientes: Limite de clientes a serem processados (opcional)
        
    Returns:
        Lista de resultados dos clientes processados
    """
    try:
        print(f"Iniciando processamento paralelo com {num_threads} threads...")
        inicio_total = datetime.now()
        
        # Cria a pasta de resultados se não existir
        resultados_dir = os.path.join(os.getcwd(), "resultados")
        if not os.path.exists(resultados_dir):
            os.makedirs(resultados_dir)
            print(f"Pasta de resultados criada: {resultados_dir}")
        
        # Cria a pasta temporária se não existir
        temp_dir = os.path.join(resultados_dir, "temp")
        if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
            print(f"Pasta temporária criada: {temp_dir}")
        
        # Conecta ao MongoDB
        db = conectar_mongodb()
        if db is None:
            print("Falha ao conectar ao MongoDB.")
            return []
        
        # Obtém apenas os clientes com movimentações
        codigos_clientes_com_movimentacao = obter_clientes_com_movimentacao(db)
        print(f"Total de {len(codigos_clientes_com_movimentacao)} clientes com movimentações encontrados.")
        
        # Obtém os clientes correspondentes
        query = {
            "cod_cliente": {"$in": list(codigos_clientes_com_movimentacao)},
            "cod_cliente": {"$exists": True, "$ne": None}
        }
        projection = {"_id": 1, "cod_cliente": 1, "razao_social": 1}
        
        if limite_clientes:
            clientes = list(db.geradores.find(query, projection).limit(limite_clientes))
        else:
            clientes = list(db.geradores.find(query, projection))
        
        total_clientes = len(clientes)
        print(f"Total de {total_clientes} clientes ativos a processar.")
        
        # Divide os clientes em grupos para processamento paralelo
        grupos_clientes = []
        tamanho_grupo = (total_clientes + num_threads - 1) // num_threads
        
        for i in range(0, total_clientes, tamanho_grupo):
            grupos_clientes.append(clientes[i:i+tamanho_grupo])
        
        print(f"Clientes divididos em {len(grupos_clientes)} grupos para processamento paralelo.")
        
        # Processa os grupos em paralelo
        resultados_completos = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submete as tarefas para o executor
            futures = []
            for i, grupo in enumerate(grupos_clientes):
                future = executor.submit(processar_grupo_clientes, db, grupo, i+1)
                futures.append(future)
            
            # Processa os resultados à medida que são concluídos
            for future in concurrent.futures.as_completed(futures):
                try:
                    resultados_grupo = future.result()
                    resultados_completos.extend(resultados_grupo)
                except Exception as e:
                    print(f"Erro ao processar grupo: {e}")
                    traceback.print_exc()
        
        # Combina todos os resultados em um único arquivo
        data_hora = datetime.now().strftime("%Y%m%d_%H%M%S")
        arquivo_final = os.path.join(resultados_dir, f'resultados_completos_paralelo_{data_hora}.json')
        
        with open(arquivo_final, 'w', encoding='utf-8') as f:
            json.dump(resultados_completos, f, default=json_util.default, ensure_ascii=False, indent=2)
        
        # Remove arquivos temporários
        try:
            for arquivo in glob.glob(os.path.join(temp_dir, "resultados_parciais_*.json")):
                os.remove(arquivo)
            for arquivo in glob.glob(os.path.join(temp_dir, "resultados_grupo_*.json")):
                os.remove(arquivo)
            print("Arquivos temporários removidos com sucesso.")
        except Exception as e:
            print(f"Erro ao remover arquivos temporários: {e}")
        
        fim_total = datetime.now()
        tempo_total = (fim_total - inicio_total).total_seconds()
        print(f"Processamento paralelo concluído para {len(resultados_completos)} clientes em {tempo_total:.2f} segundos.")
        print(f"Resultados completos salvos em '{arquivo_final}'")
        
        return resultados_completos
    
    except Exception as e:
        print(f"Erro no processamento paralelo: {e}")
        traceback.print_exc()
        return []

if __name__ == "__main__":
    # Processa os clientes em paralelo
    processar_clientes_paralelo(num_threads=NUM_THREADS, tamanho_lote=TAMANHO_LOTE)
