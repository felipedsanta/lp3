import socket
import os
import logging
import time
import hashlib
import sqlite3

ENDERECO_IP_SERVIDOR = '127.0.0.1'
PORTA_SERVIDOR = 65432
TAMANHO_BUFFER = 4096
ARQUIVO_LOG_CLIENTE_TEXTO = "cliente.log"
ARQUIVO_BD_CLIENTE = "client_log.db"
ALGORITMO_CHECKSUM_PADRAO = 'md5'

def configurar_logger_texto(nome_logger, arquivo_log, nivel=logging.INFO):
    formatador = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    manuseador_arquivo = logging.FileHandler(arquivo_log)
    manuseador_arquivo.setFormatter(formatador)
    manuseador_console = logging.StreamHandler()
    manuseador_console.setFormatter(formatador)
    logger_obj = logging.getLogger(nome_logger)
    logger_obj.setLevel(nivel)
    if not logger_obj.handlers:
        logger_obj.addHandler(manuseador_arquivo)
        logger_obj.addHandler(manuseador_console)
    return logger_obj

logger_texto = configurar_logger_texto('LoggerClienteTexto_Fase6', ARQUIVO_LOG_CLIENTE_TEXTO)

conexao_bd_cliente_global = None

def inicializar_banco_dados_cliente(arquivo_bd):
    conexao = sqlite3.connect(arquivo_bd)
    cursor_bd = conexao.cursor()
    cursor_bd.execute('''
    CREATE TABLE IF NOT EXISTS client_event_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        event_type TEXT NOT NULL,
        details TEXT
    )''')
    cursor_bd.execute('''
    CREATE TABLE IF NOT EXISTS client_file_send_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        server_address TEXT NOT NULL,
        source_local_file_path TEXT NOT NULL,
        relative_file_path_sent TEXT NOT NULL,
        file_size_bytes INTEGER NOT NULL,
        checksum_algorithm TEXT,
        calculated_checksum_hex TEXT,
        send_duration_seconds REAL,
        send_speed_KBps REAL,
        server_final_response TEXT,
        client_send_status TEXT NOT NULL,
        error_details TEXT
    )''')
    conexao.commit()
    return conexao

def registrar_evento_geral_cliente_bd(conexao_bd, tipo_evento, details=None):
    try:
        cursor_bd = conexao_bd.cursor()
        cursor_bd.execute("INSERT INTO client_event_log (event_type, details) VALUES (?, ?)", (tipo_evento, details))
        conexao_bd.commit()
    except Exception as e:
        logger_texto.error(f"BD Cliente Erro (registrar_evento_geral): {e}", exc_info=True)

def registrar_envio_arquivo_cliente_bd(conexao_bd, end_servidor, caminho_origem, caminho_rel, tamanho, algo, checksum, duracao, velocidade, resposta_servidor, status_cliente, detalhes_erro=None, tempo_evento=None):
    try:
        cursor_bd = conexao_bd.cursor()
        timestamp_log = tempo_evento if tempo_evento else sqlite3.CURRENT_TIMESTAMP
        cursor_bd.execute('''
        INSERT INTO client_file_send_log (event_timestamp, server_address, source_local_file_path, relative_file_path_sent, file_size_bytes, checksum_algorithm, calculated_checksum_hex, send_duration_seconds, send_speed_KBps, server_final_response, client_send_status, error_details)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (timestamp_log, end_servidor, caminho_origem, caminho_rel, tamanho, algo, checksum, duracao, velocidade, resposta_servidor, status_cliente, detalhes_erro))
        conexao_bd.commit()
    except Exception as e:
        logger_texto.error(f"BD Cliente Erro (registrar_envio_arquivo): {e}", exc_info=True)

def calcular_checksum(caminho_arquivo, algoritmo='md5'):
    hasheador = hashlib.new(algoritmo)
    try:
        with open(caminho_arquivo, 'rb') as f:
            while bloco := f.read(TAMANHO_BUFFER):
                hasheador.update(bloco)
        return hasheador.hexdigest()
    except Exception as e:
        logger_texto.error(f"Erro ao calcular checksum para {caminho_arquivo}: {e}", exc_info=True)
        return None

def enviar_dados_arquivo(socket_cliente, caminho_arquivo_local, offset_inicial, tamanho_total):
    bytes_enviados = 0
    tamanho_a_enviar = tamanho_total - offset_inicial
    logger_texto.info(f"Enviando dados de '{os.path.basename(caminho_arquivo_local)}' a partir do byte {offset_inicial}. Total a enviar: {tamanho_a_enviar} bytes.")
    
    try:
        with open(caminho_arquivo_local, 'rb') as arquivo_local:
            arquivo_local.seek(offset_inicial) # Pula para o ponto de retomada
            while bytes_enviados < tamanho_a_enviar:
                bloco_dados = arquivo_local.read(TAMANHO_BUFFER)
                if not bloco_dados:
                    logger_texto.warning("Leitura do arquivo local terminou inesperadamente antes do esperado.")
                    break # Fim inesperado do arquivo
                socket_cliente.sendall(bloco_dados)
                bytes_enviados += len(bloco_dados)
        
        logger_texto.info(f"Envio de dados para '{os.path.basename(caminho_arquivo_local)}' concluído. Total enviado nesta sessão: {bytes_enviados} bytes.")
        return True 
    except Exception as e:
        logger_texto.error(f"Exceção durante o envio de dados do arquivo '{os.path.basename(caminho_arquivo_local)}': {e}", exc_info=True)
        return False

def enviar_pasta(socket_cliente, caminho_pasta_origem, endereco_servidor_str):
    global conexao_bd_cliente_global
    if not os.path.isdir(caminho_pasta_origem):
        logger_texto.error(f"'{caminho_pasta_origem}' não é um diretório válido.")
        registrar_evento_geral_cliente_bd(conexao_bd_cliente_global, "ERROR_FOLDER_NOT_FOUND", details=f"Caminho: {caminho_pasta_origem}")
        return

    nome_pasta_base = os.path.basename(caminho_pasta_origem)
    logger_texto.info(f"Iniciando transferência da pasta: {nome_pasta_base}")
    registrar_evento_geral_cliente_bd(conexao_bd_cliente_global, "SEND_FOLDER_START", details=f"Pasta: {nome_pasta_base}, Destino: {endereco_servidor_str}")

    socket_cliente.sendall(f"START_FOLDER_TRANSFER:{nome_pasta_base}".encode('utf-8'))
    ack_inicio_pasta = socket_cliente.recv(TAMANHO_BUFFER)
    if ack_inicio_pasta != b"ACK_START_FOLDER":
        logger_texto.error(f"Servidor não confirmou início da transferência da pasta: {ack_inicio_pasta.decode('utf-8', 'ignore')}")
        return

    for diretorio_atual, subdiretorios, nomes_arquivos in os.walk(caminho_pasta_origem):
        caminho_relativo_diretorio = os.path.relpath(diretorio_atual, caminho_pasta_origem)
        if caminho_relativo_diretorio == ".": caminho_relativo_diretorio = ""

        for nome_subdiretorio in subdiretorios:
            caminho_rel_sub = os.path.join(caminho_relativo_diretorio, nome_subdiretorio).replace(os.sep, '/')
            logger_texto.info(f"  Enviando comando para criar subpasta: {caminho_rel_sub}")
            socket_cliente.sendall(f"NEW_FOLDER:{caminho_rel_sub}".encode('utf-8'))
            socket_cliente.recv(TAMANHO_BUFFER)

        for nome_arquivo in nomes_arquivos:
            caminho_completo_arquivo = os.path.join(diretorio_atual, nome_arquivo)
            caminho_relativo_arquivo = os.path.join(caminho_relativo_diretorio, nome_arquivo).replace(os.sep, '/')
            tamanho_arquivo_bytes = os.path.getsize(caminho_completo_arquivo)
            timestamp_inicio_envio_arquivo = time.strftime('%Y-%m-%d %H:%M:%S')

            checksum_hex_calculado = calcular_checksum(caminho_completo_arquivo, ALGORITMO_CHECKSUM_PADRAO)
            if not checksum_hex_calculado:
                logger_texto.error(f"Não foi possível calcular checksum para '{caminho_completo_arquivo}'. Arquivo ignorado.")
                continue

            logger_texto.info(f"Preparando para enviar '{caminho_relativo_arquivo}'. Tamanho: {tamanho_arquivo_bytes}, Checksum: {checksum_hex_calculado[:10]}...")
            cabecalho_preparacao = f"PREPARE_FILE_TRANSFER:{caminho_relativo_arquivo}:{tamanho_arquivo_bytes}:{ALGORITMO_CHECKSUM_PADRAO}:{checksum_hex_calculado}"
            socket_cliente.sendall(cabecalho_preparacao.encode('utf-8'))
            resposta_servidor_str = socket_cliente.recv(TAMANHO_BUFFER).decode('utf-8', 'ignore')
            
            offset_para_enviar = -1

            if resposta_servidor_str == "FILE_ALREADY_EXISTS_CHECKSUM_OK":
                logger_texto.info(f"Servidor informou que '{caminho_relativo_arquivo}' já existe e está OK. Pulando.")
                registrar_envio_arquivo_cliente_bd(conexao_bd_cliente_global, endereco_servidor_str, caminho_completo_arquivo, caminho_relativo_arquivo, tamanho_arquivo_bytes, ALGORITMO_CHECKSUM_PADRAO, checksum_hex_calculado, 0, 0, resposta_servidor_str, "SKIPPED_ALREADY_EXISTS", tempo_evento=timestamp_inicio_envio_arquivo)
                continue 
            elif resposta_servidor_str.startswith("RESUME_FROM_OFFSET:"):
                offset_para_enviar = int(resposta_servidor_str.split(":", 1)[1])
                logger_texto.info(f"Servidor instruiu a retomar '{caminho_relativo_arquivo}' do byte {offset_para_enviar}.")
            elif resposta_servidor_str == "SEND_FROM_OFFSET:0":
                offset_para_enviar = 0
                logger_texto.info(f"Servidor instruiu a enviar '{caminho_relativo_arquivo}' desde o início.")
            else:
                logger_texto.error(f"Resposta inesperada do servidor ao preparar arquivo '{caminho_relativo_arquivo}': {resposta_servidor_str}")
                registrar_envio_arquivo_cliente_bd(conexao_bd_cliente_global, endereco_servidor_str, caminho_completo_arquivo, caminho_relativo_arquivo, tamanho_arquivo_bytes, ALGORITMO_CHECKSUM_PADRAO, checksum_hex_calculado, 0, 0, resposta_servidor_str, "FAIL_UNEXPECTED_PREPARE_RESPONSE", detalhes_erro=resposta_servidor_str, tempo_evento=timestamp_inicio_envio_arquivo)
                continue

            if offset_para_enviar >= 0:
                socket_cliente.sendall(f"START_FILE_DATA:{caminho_relativo_arquivo}".encode('utf-8'))
                ack_inicio_dados = socket_cliente.recv(TAMANHO_BUFFER)
                if ack_inicio_dados != b"ACK_START_FILE_DATA":
                    logger_texto.error(f"Servidor não confirmou início do envio de dados para '{caminho_relativo_arquivo}'.")
                    continue
                
                inicio_envio_dados_ts = time.time()
                sucesso_envio_dados = enviar_dados_arquivo(socket_cliente, caminho_completo_arquivo, offset_para_enviar, tamanho_arquivo_bytes)
                fim_envio_dados_ts = time.time()
                duracao_envio = fim_envio_dados_ts - inicio_envio_dados_ts
                
                velocidade_envio_kbps = 0
                bytes_enviados_sessao = tamanho_arquivo_bytes - offset_para_enviar
                if duracao_envio > 0:
                    velocidade_envio_kbps = (bytes_enviados_sessao / 1024) / duracao_envio

                if sucesso_envio_dados:
                    status_final_servidor = socket_cliente.recv(TAMANHO_BUFFER).decode('utf-8', 'ignore')
                    logger_texto.info(f"Status final do servidor para '{caminho_relativo_arquivo}': {status_final_servidor}")
                    
                    status_cliente_bd = "UNKNOWN"
                    if status_final_servidor == "FILE_CHECKSUM_OK":
                        status_cliente_bd = "SUCCESS_SENT_SERVER_OK"
                    elif status_final_servidor == "FILE_CHECKSUM_MISMATCH":
                        status_cliente_bd = "SUCCESS_SENT_SERVER_CHECKSUM_FAIL"
                    else:
                        status_cliente_bd = f"FAIL_SERVER_REPORTED_ERROR_{status_final_servidor}"
                    
                    registrar_envio_arquivo_cliente_bd(conexao_bd_cliente_global, endereco_servidor_str, caminho_completo_arquivo, caminho_relativo_arquivo, tamanho_arquivo_bytes, ALGORITMO_CHECKSUM_PADRAO, checksum_hex_calculado, duracao_envio, velocidade_envio_kbps, status_final_servidor, status_cliente_bd, tempo_evento=timestamp_inicio_envio_arquivo)
                else:
                    logger_texto.error(f"Falha local ao enviar dados do arquivo '{caminho_relativo_arquivo}'.")
                    registrar_envio_arquivo_cliente_bd(conexao_bd_cliente_global, endereco_servidor_str, caminho_completo_arquivo, caminho_relativo_arquivo, tamanho_arquivo_bytes, ALGORITMO_CHECKSUM_PADRAO, checksum_hex_calculado, duracao_envio, 0, "N/A", "FAIL_LOCAL_SEND_DATA", "Falha na função enviar_dados_arquivo", tempo_evento=timestamp_inicio_envio_arquivo)


    socket_cliente.sendall(b"END_FOLDER_TRANSFER")
    ack_fim_pasta = socket_cliente.recv(TAMANHO_BUFFER)
    if ack_fim_pasta == b"ACK_END_FOLDER":
        logger_texto.info(f"Servidor confirmou fim do processamento da pasta '{nome_pasta_base}'.")
    else:
        logger_texto.warning(f"Servidor não confirmou fim do processamento da pasta: {ack_fim_pasta.decode('utf-8', 'ignore')}")
    
    registrar_evento_geral_cliente_bd(conexao_bd_cliente_global, "SEND_FOLDER_END", details=f"Pasta: {nome_pasta_base}")


def main():
    global conexao_bd_cliente_global
    conexao_bd_cliente_global = inicializar_banco_dados_cliente(ARQUIVO_BD_CLIENTE)
    registrar_evento_geral_cliente_bd(conexao_bd_cliente_global, "CLIENT_START", details=f"Cliente iniciado. Tentará conectar em {ENDERECO_IP_SERVIDOR}:{PORTA_SERVIDOR}")
    logger_texto.info(f"Cliente iniciado. Tentará conectar em {ENDERECO_IP_SERVIDOR}:{PORTA_SERVIDOR}")

    endereco_servidor_para_log = f"{ENDERECO_IP_SERVIDOR}:{PORTA_SERVIDOR}"

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_principal:
        try:
            logger_texto.info(f"Tentando conectar ao servidor em {ENDERECO_IP_SERVIDOR}:{PORTA_SERVIDOR}...")
            registrar_evento_geral_cliente_bd(conexao_bd_cliente_global, "CONNECTING_TO_SERVER", details=f"Servidor: {endereco_servidor_para_log}")
            socket_principal.connect((ENDERECO_IP_SERVIDOR, PORTA_SERVIDOR))
            logger_texto.info(f"Conectado ao servidor em {ENDERECO_IP_SERVIDOR}:{PORTA_SERVIDOR}")
            registrar_evento_geral_cliente_bd(conexao_bd_cliente_global, "CONNECTED_TO_SERVER", details=f"Servidor: {endereco_servidor_para_log}")

            caminho_pasta_para_enviar = input("Digite o caminho completo da PASTA que deseja enviar: ")
            
            if os.path.isdir(caminho_pasta_para_enviar):
                enviar_pasta(socket_principal, caminho_pasta_para_enviar, endereco_servidor_para_log)
            else:
                logger_texto.error(f"O caminho '{caminho_pasta_para_enviar}' não é uma pasta válida ou não existe.")
                registrar_evento_geral_cliente_bd(conexao_bd_cliente_global, "ERROR_INPUT_PATH_INVALID", details=f"Caminho fornecido: {caminho_pasta_para_enviar}")

        except ConnectionRefusedError:
            logger_texto.error("Erro: A conexao foi recusada. Verifique se o servidor esta rodando.", exc_info=True)
            registrar_evento_geral_cliente_bd(conexao_bd_cliente_global, "ERROR_CONNECTION_REFUSED", details=f"Servidor: {endereco_servidor_para_log}")
        except ConnectionAbortedError:
            logger_texto.error("Erro: A conexão foi abortada.", exc_info=True)
            registrar_evento_geral_cliente_bd(conexao_bd_cliente_global, "ERROR_CONNECTION_ABORTED", details=f"Servidor: {endereco_servidor_para_log}")
        except Exception as e:
            logger_texto.critical(f"Ocorreu um erro crítico no cliente: {e}", exc_info=True)
            registrar_evento_geral_cliente_bd(conexao_bd_cliente_global, "ERROR_CRITICAL_EXCEPTION", details=str(e))
    
    registrar_evento_geral_cliente_bd(conexao_bd_cliente_global, "CLIENT_STOP")
    logger_texto.info("Cliente encerrado.")
    if conexao_bd_cliente_global:
        conexao_bd_cliente_global.close()

if __name__ == "__main__":
    main()