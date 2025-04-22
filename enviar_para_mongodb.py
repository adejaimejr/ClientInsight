"""
Script para enviar os resultados de processamento para o MongoDB.
Este script limpa a collection ClientInsight antes do primeiro envio e depois
carrega todos os arquivos JSON de resultados para essa collection.
"""
import os
import json
import glob
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import json_util
import argparse

# Carrega as variáveis de ambiente
load_dotenv()

# Configuração da conexão com o MongoDB
MONGODB_URI = os.getenv("MONGODB_URI")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE")

def log(mensagem, nivel=0, sempre_mostrar=True):
    """Função para exibir logs."""
    indentacao = "    " * nivel
    print(f"{indentacao}{mensagem}")

def conectar_mongodb():
    """Estabelece conexão com o MongoDB."""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DATABASE]
        log(f"Conexão estabelecida com o banco de dados: {MONGODB_DATABASE}")
        return db
    except Exception as e:
        log(f"Erro ao conectar ao MongoDB: {e}")
        return None

def limpar_collection(db, nome_collection):
    """Limpa uma collection do MongoDB."""
    try:
        collection = db[nome_collection]
        resultado = collection.delete_many({})
        log(f"Collection {nome_collection} limpa. {resultado.deleted_count} documentos removidos.")
    except Exception as e:
        log(f"Erro ao limpar a collection {nome_collection}: {e}")

def enviar_arquivos_para_mongodb(db, diretorio_resultados, nome_collection):
    """
    Envia todos os arquivos JSON de um diretório para uma collection do MongoDB.
    
    Args:
        db: Conexão com o banco de dados MongoDB
        diretorio_resultados: Diretório que contém os arquivos JSON
        nome_collection: Nome da collection para onde enviar os dados
    """
    try:
        # Obtém a collection
        collection = db[nome_collection]
        
        # Obtém todos os arquivos JSON no diretório de resultados
        padrao_arquivos = os.path.join(diretorio_resultados, "*.json")
        arquivos_json = glob.glob(padrao_arquivos)
        
        if not arquivos_json:
            log(f"Nenhum arquivo JSON encontrado em {diretorio_resultados}")
            return
        
        log(f"Encontrados {len(arquivos_json)} arquivos JSON para enviar")
        
        # Contador para acompanhar o progresso
        contador = 0
        documentos_inseridos = 0
        documentos_atualizados = 0
        
        # Processa cada arquivo
        for arquivo in arquivos_json:
            nome_arquivo = os.path.basename(arquivo)
            log(f"Processando arquivo: {nome_arquivo}", nivel=1)
            
            try:
                # Carrega o conteúdo do arquivo JSON
                with open(arquivo, "r", encoding="utf-8") as f:
                    dados = json.load(f)
                
                # Verifica se os dados são uma lista ou um objeto individual
                if isinstance(dados, list):
                    # Trata dados como lista de objetos cliente
                    for cliente in dados:
                        if "codigo_cliente" not in cliente:
                            log(f"Ignorando cliente sem código no arquivo {nome_arquivo}", nivel=2)
                            continue
                            
                        # Adiciona metadados sobre o arquivo
                        cliente["_arquivo_origem"] = nome_arquivo
                        cliente["_data_importacao"] = datetime.now()
                        
                        # Usa o código do cliente como chave para upsert
                        codigo_cliente = cliente["codigo_cliente"]
                        filtro = {"codigo_cliente": codigo_cliente}
                        
                        # Insere ou atualiza o documento na collection
                        resultado = collection.update_one(
                            filtro, 
                            {"$set": cliente}, 
                            upsert=True
                        )
                        
                        if resultado.upserted_id:
                            documentos_inseridos += 1
                        elif resultado.modified_count > 0:
                            documentos_atualizados += 1
                else:
                    # Trata como um único objeto cliente
                    if "codigo_cliente" not in dados:
                        log(f"Ignorando cliente sem código no arquivo {nome_arquivo}", nivel=2)
                        continue
                        
                    # Adiciona metadados sobre o arquivo
                    dados["_arquivo_origem"] = nome_arquivo
                    dados["_data_importacao"] = datetime.now()
                    
                    # Usa o código do cliente como chave para upsert
                    codigo_cliente = dados["codigo_cliente"]
                    filtro = {"codigo_cliente": codigo_cliente}
                    
                    # Insere ou atualiza o documento na collection
                    resultado = collection.update_one(
                        filtro, 
                        {"$set": dados}, 
                        upsert=True
                    )
                    
                    if resultado.upserted_id:
                        documentos_inseridos += 1
                        log(f"Documento inserido para cliente {codigo_cliente}", nivel=2)
                    elif resultado.modified_count > 0:
                        documentos_atualizados += 1
                        log(f"Documento atualizado para cliente {codigo_cliente}", nivel=2)
                    else:
                        log(f"Nenhuma alteração para cliente {codigo_cliente}", nivel=2)
                
            except Exception as e:
                log(f"Erro ao processar arquivo {nome_arquivo}: {e}", nivel=2)
            
            contador += 1
            if contador % 10 == 0:
                log(f"Progresso: {contador}/{len(arquivos_json)} arquivos processados")
        
        log(f"Envio concluído. {documentos_inseridos} documentos inseridos e {documentos_atualizados} documentos atualizados na collection {nome_collection}")
    
    except Exception as e:
        log(f"Erro durante o envio dos arquivos: {e}")

def main(limpar_collection_antes=True, diretorio_resultados=None):
    """
    Função principal do script.
    
    Args:
        limpar_collection_antes: Se True, limpa a collection antes do envio
        diretorio_resultados: Diretório que contém os arquivos de resultados
    """
    start_time = datetime.now()
    log(f"[INÍCIO] Processo iniciado em: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Conecta ao MongoDB
    db = conectar_mongodb()
    if db is None:
        log("Não foi possível estabelecer conexão com o MongoDB. Encerrando.")
        return
    
    # Nome da collection para onde enviar os resultados
    nome_collection = "ClientInsight"
    
    # Limpa a collection antes do primeiro envio se solicitado
    if limpar_collection_antes:
        log(f"Limpando a collection {nome_collection}...")
        limpar_collection(db, nome_collection)
    
    # Define o diretório que contém os arquivos de resultados se não especificado
    if diretorio_resultados is None:
        diretorio_resultados = os.path.join(os.getcwd(), "resultados")
    
    # Envia os arquivos para o MongoDB
    log(f"Enviando arquivos do diretório {diretorio_resultados} para a collection {nome_collection}...")
    enviar_arquivos_para_mongodb(db, diretorio_resultados, nome_collection)
    
    # Calcula e exibe o tempo total de execução
    end_time = datetime.now()
    duracao = end_time - start_time
    log(f"[TÉRMINO] Processo concluído em: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Tempo total de execução: {duracao.total_seconds():.2f} segundos")
    
    return True

if __name__ == "__main__":
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(description="Enviar arquivos JSON para o MongoDB")
    parser.add_argument("--limpar", action="store_true", help="Limpar a collection antes do envio")
    parser.add_argument("--diretorio", help="Diretório que contém os arquivos de resultados")
    
    # Faz o parsing dos argumentos
    args = parser.parse_args()
    
    # Executa o script com os argumentos fornecidos
    main(limpar_collection_antes=args.limpar, diretorio_resultados=args.diretorio)
