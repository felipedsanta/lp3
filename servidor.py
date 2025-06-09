import socket
import os
import logging
import time
import hashlib
import sqlite3

ENDERECO_IP_SERVIDOR = '0.0.0.0'
PORTA_SERVIDOR = 65432
TAMANHO_BUFFER = 4096
PASTA_UPLOADS = "uploads_servidor"
ARQUIVO_LOG_SERVIDOR_TEXTO = "servidor.log"
ARQUIVO_BD_SERVIDOR = "servidor_log.db" 

#peguei da internet, é um padrão comum
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

logger_texto = configurar_logger_texto('LoggerServidorTexto_Fase6', ARQUIVO_LOG_SERVIDOR_TEXTO)

conexao_bd_global = None

def inicializar_banco_dados(arquivo_bd):
    conexao = sqlite3.connect(arquivo_bd)
    cursor_bd = conexao.cursor()
    cursor_bd.execute('''
    CREATE TABLE IF NOT EXISTS event_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        client_address TEXT,
        event_type TEXT NOT NULL,
        details TEXT
    )''')
    
    cursor_bd.execute('''
    CREATE TABLE IF NOT EXISTS file_transfer_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_address TEXT NOT NULL,
        relative_file_path TEXT NOT NULL,
        total_file_size_bytes INTEGER NOT NULL,
        checksum_algorithm TEXT,
        expected_checksum_hex TEXT,
        current_bytes_transferred INTEGER DEFAULT 0,
        status TEXT NOT NULL, /* e.g., AWAITING_PREPARE, AWAITING_DATA, RECEIVING, PAUSED_DISCONNECT, COMPLETED_DATA, SUCCESS_CHECKSUM_OK, FAIL_CHECKSUM_MISMATCH */
        first_seen_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_update_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        transfer_start_data_timestamp DATETIME,
        transfer_end_data_timestamp DATETIME,
        final_calculated_checksum_hex TEXT,
        final_duration_seconds REAL,
        final_speed_KBps REAL,
        error_details TEXT,
        UNIQUE(client_address, relative_file_path)
    )''')
    conexao.commit()
    return conexao

def registrar_evento_geral_bd(conexao_bd_local, tipo_evento, endereco_cliente=None, detalhes_evento=None):
    try:
        cursor_bd = conexao_bd_local.cursor()
        cursor_bd.execute("INSERT INTO event_log (client_address, event_type, details) VALUES (?, ?, ?)", 
                       (endereco_cliente, tipo_evento, detalhes_evento))
        conexao_bd_local.commit()
    except Exception as e:
        logger_texto.error(f"BD Erro (registrar_evento_geral_bd): {e}", exc_info=True)


def registrar_ou_atualizar_metadados_arquivo_bd(conexao_bd_local, end_cliente, caminho_rel, tamanho_total, algo_checksum, checksum_esp, status_inicial='AWAITING_PREPARE'):
    agora_str = time.strftime('%Y-%m-%d %H:%M:%S')
    try:
        cursor_bd = conexao_bd_local.cursor()
        cursor_bd.execute('''
        INSERT INTO file_transfer_log (client_address, relative_file_path, total_file_size_bytes, checksum_algorithm, expected_checksum_hex, status, first_seen_timestamp, last_update_timestamp, current_bytes_transferred)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
        ON CONFLICT(client_address, relative_file_path) DO UPDATE SET
            total_file_size_bytes=excluded.total_file_size_bytes,
            checksum_algorithm=excluded.checksum_algorithm,
            expected_checksum_hex=excluded.expected_checksum_hex,
            status=CASE
                WHEN file_transfer_log.status = 'SUCCESS_CHECKSUM_OK' THEN 'SUCCESS_CHECKSUM_OK'
                ELSE excluded.status
            END,
            last_update_timestamp=excluded.last_update_timestamp,
            current_bytes_transferred=CASE
                 WHEN file_transfer_log.status = 'SUCCESS_CHECKSUM_OK' THEN file_transfer_log.current_bytes_transferred
                 WHEN excluded.total_file_size_bytes != file_transfer_log.total_file_size_bytes OR excluded.expected_checksum_hex != file_transfer_log.expected_checksum_hex THEN 0
                 ELSE file_transfer_log.current_bytes_transferred
            END,
            final_calculated_checksum_hex=CASE WHEN excluded.total_file_size_bytes != file_transfer_log.total_file_size_bytes OR excluded.expected_checksum_hex != file_transfer_log.expected_checksum_hex THEN NULL ELSE file_transfer_log.final_calculated_checksum_hex END,
            final_duration_seconds=CASE WHEN excluded.total_file_size_bytes != file_transfer_log.total_file_size_bytes OR excluded.expected_checksum_hex != file_transfer_log.expected_checksum_hex THEN NULL ELSE file_transfer_log.final_duration_seconds END,
            final_speed_KBps=CASE WHEN excluded.total_file_size_bytes != file_transfer_log.total_file_size_bytes OR excluded.expected_checksum_hex != file_transfer_log.expected_checksum_hex THEN NULL ELSE file_transfer_log.final_speed_KBps END,
            error_details=CASE WHEN excluded.total_file_size_bytes != file_transfer_log.total_file_size_bytes OR excluded.expected_checksum_hex != file_transfer_log.expected_checksum_hex THEN NULL ELSE file_transfer_log.error_details END
        RETURNING id
        ''', (end_cliente, caminho_rel, tamanho_total, algo_checksum, checksum_esp, status_inicial, agora_str, agora_str))
        
        id_linha = cursor_bd.fetchone()
        conexao_bd_local.commit()
        return id_linha[0] if id_linha else None
    except Exception as e:
        logger_texto.error(f"BD Erro (registrar_ou_atualizar_metadados_arquivo_bd) para {caminho_rel}: {e}", exc_info=True)
        return None

def atualizar_status_transferencia_arquivo_bd(conexao_bd_local, id_log_arquivo, novo_status, incremento_bytes=0, total_bytes_atuais=None, detalhes_erro=None, tempo_inicio_dados=None, tempo_fim_dados=None, checksum_final=None, duracao_final=None, velocidade_final=None):
    agora_str = time.strftime('%Y-%m-%d %H:%M:%S')
    try:
        cursor_bd = conexao_bd_local.cursor()
        campos_para_atualizar = {"status": novo_status, "last_update_timestamp": agora_str}
        if detalhes_erro is not None: campos_para_atualizar["error_details"] = detalhes_erro
        if tempo_inicio_dados: campos_para_atualizar["transfer_start_data_timestamp"] = tempo_inicio_dados
        if tempo_fim_dados: campos_para_atualizar["transfer_end_data_timestamp"] = tempo_fim_dados
        if checksum_final: campos_para_atualizar["final_calculated_checksum_hex"] = checksum_final
        if duracao_final is not None: campos_para_atualizar["final_duration_seconds"] = duracao_final
        if velocidade_final is not None: campos_para_atualizar["final_speed_KBps"] = velocidade_final
        
        if total_bytes_atuais is not None:
             campos_para_atualizar["current_bytes_transferred"] = total_bytes_atuais
        elif incremento_bytes > 0:
            cursor_bd.execute("UPDATE file_transfer_log SET current_bytes_transferred = current_bytes_transferred + ? WHERE id = ?", (incremento_bytes, id_log_arquivo))

        clausula_set = ", ".join([f"{chave} = ?" for chave in campos_para_atualizar.keys()])
        valores = list(campos_para_atualizar.values())
        valores.append(id_log_arquivo)
        
        cursor_bd.execute(f"UPDATE file_transfer_log SET {clausula_set} WHERE id = ?", valores)
        conexao_bd_local.commit()
    except Exception as e:
        logger_texto.error(f"BD Erro (atualizar_status_transferencia_arquivo_bd) para ID {id_log_arquivo}: {e}", exc_info=True)

def obter_estado_transferencia_arquivo_bd(conexao_bd_local, end_cliente, caminho_rel):
    try:
        cursor_bd = conexao_bd_local.cursor()
        cursor_bd.execute('''
        SELECT id, total_file_size_bytes, expected_checksum_hex, current_bytes_transferred, status 
        FROM file_transfer_log 
        WHERE client_address = ? AND relative_file_path = ?
        ''', (end_cliente, caminho_rel))
        return cursor_bd.fetchone()
    except Exception as e:
        logger_texto.error(f"BD Erro (obter_estado_transferencia_arquivo_bd) para {caminho_rel}: {e}", exc_info=True)
        return None

def calcular_checksum(caminho_arquivo, algoritmo='md5'):
    hasheador = hashlib.new(algoritmo)
    try:
        with open(caminho_arquivo, 'rb') as f:
            while bloco_dados := f.read(TAMANHO_BUFFER):
                hasheador.update(bloco_dados)
        return hasheador.hexdigest()
    except Exception: 
        return None

def receber_dados_arquivo(socket_cliente, caminho_fisico_arq_servidor, tamanho_total_arq, end_cliente_str, 
                          id_log_arq_bd, offset_inicial_transferencia, algo_checksum_esperado, checksum_hex_esperado_cliente):
    global conexao_bd_global
    logger_texto.info(f"Recebendo dados para '{os.path.basename(caminho_fisico_arq_servidor)}'. Total: {tamanho_total_arq} bytes. Offset inicial: {offset_inicial_transferencia}.")
    
    bytes_escritos_nesta_sessao = 0
    status_final_para_cliente = "FILE_DATA_ERROR"
    
    modo_abertura_arquivo = 'wb' if offset_inicial_transferencia == 0 else 'r+b'
    
    try:
        with open(caminho_fisico_arq_servidor, modo_abertura_arquivo) as arquivo_servidor:
            if offset_inicial_transferencia > 0:
                arquivo_servidor.seek(offset_inicial_transferencia)
                logger_texto.info(f"Posicionado em {offset_inicial_transferencia} para retomar escrita de '{os.path.basename(caminho_fisico_arq_servidor)}'.")
            
            bytes_esperados_nesta_sessao = tamanho_total_arq - offset_inicial_transferencia
            
            status_recebimento = "RECEIVING"
            if offset_inicial_transferencia == 0:
                 atualizar_status_transferencia_arquivo_bd(conexao_bd_global, id_log_arq_bd, status_recebimento, tempo_inicio_dados=time.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                 atualizar_status_transferencia_arquivo_bd(conexao_bd_global, id_log_arq_bd, status_recebimento)

            while bytes_escritos_nesta_sessao < bytes_esperados_nesta_sessao:
                bytes_para_ler_agora = min(TAMANHO_BUFFER, bytes_esperados_nesta_sessao - bytes_escritos_nesta_sessao)
                bloco_recebido = socket_cliente.recv(bytes_para_ler_agora)
                if not bloco_recebido:
                    logger_texto.warning(f"Conexão perdida por {end_cliente_str} durante transferência de '{os.path.basename(caminho_fisico_arq_servidor)}'.")
                    atualizar_status_transferencia_arquivo_bd(conexao_bd_global, id_log_arq_bd, "PAUSED_DISCONNECT", 
                                                total_bytes_atuais=(offset_inicial_transferencia + bytes_escritos_nesta_sessao),
                                                detalhes_erro="Conexão perdida durante transferência de dados.")
                    return "FILE_DATA_ERROR"
                arquivo_servidor.write(bloco_recebido)
                bytes_escritos_nesta_sessao += len(bloco_recebido)

        total_bytes_atuais_no_disco = offset_inicial_transferencia + bytes_escritos_nesta_sessao
        atualizar_status_transferencia_arquivo_bd(conexao_bd_global, id_log_arq_bd, "COMPLETED_DATA_RECEIVED", 
                                    total_bytes_atuais=total_bytes_atuais_no_disco,
                                    tempo_fim_dados=time.strftime('%Y-%m-%d %H:%M:%S'))
        logger_texto.info(f"Todos os dados esperados para '{os.path.basename(caminho_fisico_arq_servidor)}' recebidos. Total no disco: {total_bytes_atuais_no_disco} bytes.")

        if total_bytes_atuais_no_disco == tamanho_total_arq:
            logger_texto.info(f"Arquivo '{os.path.basename(caminho_fisico_arq_servidor)}' completo. Calculando checksum...")
            checksum_calculado_servidor = calcular_checksum(caminho_fisico_arq_servidor, algo_checksum_esperado)
            logger_texto.info(f"Checksum calculado no servidor ({algo_checksum_esperado}): {checksum_calculado_servidor}")

            cursor_bd = conexao_bd_global.cursor()
            cursor_bd.execute("SELECT transfer_start_data_timestamp, transfer_end_data_timestamp FROM file_transfer_log WHERE id = ?", (id_log_arq_bd,))
            marcas_tempo = cursor_bd.fetchone()
            duracao_transferencia = 0
            velocidade_transferencia = 0
            if marcas_tempo and marcas_tempo[0] and marcas_tempo[1]:
                try:
                    inicio_dt = time.mktime(time.strptime(marcas_tempo[0], '%Y-%m-%d %H:%M:%S'))
                    fim_dt = time.mktime(time.strptime(marcas_tempo[1], '%Y-%m-%d %H:%M:%S'))
                    duracao_transferencia = fim_dt - inicio_dt
                    if duracao_transferencia > 0:
                        velocidade_transferencia = (tamanho_total_arq / 1024) / duracao_transferencia
                except ValueError:
                     logger_texto.warning("Não foi possível parsear timestamps para cálculo de duração/velocidade.")

            if checksum_calculado_servidor == checksum_hex_esperado_cliente:
                logger_texto.info(f"CHECKSUM OK para '{os.path.basename(caminho_fisico_arq_servidor)}'.")
                atualizar_status_transferencia_arquivo_bd(conexao_bd_global, id_log_arq_bd, "SUCCESS_CHECKSUM_OK", 
                                            checksum_final=checksum_calculado_servidor, duracao_final=duracao_transferencia, velocidade_final=velocidade_transferencia)
                status_final_para_cliente = "FILE_CHECKSUM_OK"
            else:
                logger_texto.error(f"CHECKSUM MISMATCH para '{os.path.basename(caminho_fisico_arq_servidor)}'. Esperado: {checksum_hex_esperado_cliente}, Calculado: {checksum_calculado_servidor}")
                detalhes = f"Esperado: {checksum_hex_esperado_cliente}, Calculado: {checksum_calculado_servidor}"
                atualizar_status_transferencia_arquivo_bd(conexao_bd_global, id_log_arq_bd, "FAIL_CHECKSUM_MISMATCH", 
                                            checksum_final=checksum_calculado_servidor, duracao_final=duracao_transferencia, velocidade_final=velocidade_transferencia, detalhes_erro=detalhes)
                try:
                    os.remove(caminho_fisico_arq_servidor)
                    logger_texto.info(f"Arquivo corrompido '{caminho_fisico_arq_servidor}' removido.")
                except Exception as e_rem:
                    logger_texto.error(f"Erro ao remover arquivo corrompido '{caminho_fisico_arq_servidor}': {e_rem}")
                status_final_para_cliente = "FILE_CHECKSUM_MISMATCH"
        else:
             status_final_para_cliente = "FILE_DATA_ERROR" 
             atualizar_status_transferencia_arquivo_bd(conexao_bd_global, id_log_arq_bd, "FAIL_DATA_INCOMPLETE", detalhes_erro=f"Cliente enviou dados, mas arquivo finalizou com {total_bytes_atuais_no_disco}/{tamanho_total_arq} bytes.")

    except FileNotFoundError:
        logger_texto.error(f"FNF Erro ao tentar abrir/escrever em '{caminho_fisico_arq_servidor}' (offset {offset_inicial_transferencia}).", exc_info=True)
        atualizar_status_transferencia_arquivo_bd(conexao_bd_global, id_log_arq_bd, "FAIL_SERVER_ERROR", detalhes_erro=f"FileNotFound ao tentar escrever: {caminho_fisico_arq_servidor}")
        status_final_para_cliente = "FILE_DATA_ERROR" 
    except Exception as e:
        logger_texto.error(f"Exceção em receber_dados_arquivo para '{os.path.basename(caminho_fisico_arq_servidor)}': {e}", exc_info=True)
        atualizar_status_transferencia_arquivo_bd(conexao_bd_global, id_log_arq_bd, "FAIL_SERVER_ERROR", 
                                    total_bytes_atuais=(offset_inicial_transferencia + bytes_escritos_nesta_sessao),
                                    detalhes_erro=str(e))
        status_final_para_cliente = "FILE_DATA_ERROR" 
        
    return status_final_para_cliente


def main():
    global conexao_bd_global
    conexao_bd_global = inicializar_banco_dados(ARQUIVO_BD_SERVIDOR)
    registrar_evento_geral_bd(conexao_bd_global, "SERVER_START", detalhes_evento=f"Servidor escutando em {ENDERECO_IP_SERVIDOR}:{PORTA_SERVIDOR}")
    if not os.path.exists(PASTA_UPLOADS): os.makedirs(PASTA_UPLOADS)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket_servidor:
        socket_servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        socket_servidor.bind((ENDERECO_IP_SERVIDOR, PORTA_SERVIDOR))
        socket_servidor.listen()
        logger_texto.info(f"Servidor iniciado e escutando em {ENDERECO_IP_SERVIDOR}:{PORTA_SERVIDOR}")

        while True:
            try:
                socket_cliente, endereco_cliente_tupla = socket_servidor.accept()
            except KeyboardInterrupt:
                logger_texto.info("Servidor interrompido pelo usuário (Ctrl+C).")
                break
            except Exception as e_accept:
                logger_texto.error(f"Erro ao aceitar conexão: {e_accept}", exc_info=True)
                continue

            endereco_cliente_str = f"{endereco_cliente_tupla[0]}:{endereco_cliente_tupla[1]}"
            caminho_base_upload_atual = "" 
            id_log_arquivo_em_progresso_bd = None

            with socket_cliente:
                logger_texto.info(f"Conexão estabelecida com {endereco_cliente_str}")
                registrar_evento_geral_bd(conexao_bd_global, "CLIENT_CONNECT", endereco_cliente=endereco_cliente_str)
                
                conexao_com_cliente_ativa = True
                while conexao_com_cliente_ativa:
                    try:
                        bytes_cabecalho_recebidos = socket_cliente.recv(TAMANHO_BUFFER)
                        if not bytes_cabecalho_recebidos:
                            logger_texto.info(f"Cliente {endereco_cliente_str} encerrou a conexão (sem cabeçalho).")
                            registrar_evento_geral_bd(conexao_bd_global, "CLIENT_DISCONNECT", endereco_cliente=endereco_cliente_str, detalhes_evento="Cabeçalho vazio.")
                            conexao_com_cliente_ativa = False
                            break
                        
                        cabecalho_str = bytes_cabecalho_recebidos.decode('utf-8')
                        logger_texto.debug(f"Comando de {endereco_cliente_str}: {cabecalho_str[:200]}")

                        # Strings de protocolo mantidas em INGLÊS
                        if cabecalho_str.startswith("PREPARE_FILE_TRANSFER:"):
                            partes_cabecalho = cabecalho_str.split(":", 4)
                            caminho_relativo_arq = partes_cabecalho[1]
                            tamanho_total_arq_cliente = int(partes_cabecalho[2])
                            algo_checksum_arq = partes_cabecalho[3]
                            checksum_esperado_arq = partes_cabecalho[4]

                            id_log_arquivo_em_progresso_bd = registrar_ou_atualizar_metadados_arquivo_bd(conexao_bd_global, endereco_cliente_str, caminho_relativo_arq, tamanho_total_arq_cliente, algo_checksum_arq, checksum_esperado_arq, 'AWAITING_PREPARE')
                            
                            if not id_log_arquivo_em_progresso_bd:
                                socket_cliente.sendall(b"ERROR_SERVER_DB_ISSUE") # Protocolo
                                conexao_com_cliente_ativa = False; break

                            estado_arquivo_bd = obter_estado_transferencia_arquivo_bd(conexao_bd_global, endereco_cliente_str, caminho_relativo_arq)

                            # (id, total_size, expected_checksum, current_bytes, status)
                            if estado_arquivo_bd and estado_arquivo_bd[4] == 'SUCCESS_CHECKSUM_OK' and estado_arquivo_bd[1] == tamanho_total_arq_cliente and estado_arquivo_bd[2] == checksum_esperado_arq:
                                logger_texto.info(f"Arquivo '{caminho_relativo_arq}' já existe e checksum OK. Informando cliente.")
                                socket_cliente.sendall(b"FILE_ALREADY_EXISTS_CHECKSUM_OK") # Protocolo
                            elif estado_arquivo_bd and estado_arquivo_bd[1] == tamanho_total_arq_cliente and estado_arquivo_bd[2] == checksum_esperado_arq:
                                offset_para_retomar = estado_arquivo_bd[3]
                                logger_texto.info(f"Retomando '{caminho_relativo_arq}' do offset {offset_para_retomar}. Total {tamanho_total_arq_cliente}.")
                                atualizar_status_transferencia_arquivo_bd(conexao_bd_global, id_log_arquivo_em_progresso_bd, "AWAITING_DATA")
                                socket_cliente.sendall(f"RESUME_FROM_OFFSET:{offset_para_retomar}".encode('utf-8')) # Protocolo
                            else: 
                                logger_texto.info(f"Iniciando nova transferência para '{caminho_relativo_arq}'. Offset 0. Total {tamanho_total_arq_cliente}.")
                                if estado_arquivo_bd and (estado_arquivo_bd[1] != tamanho_total_arq_cliente or estado_arquivo_bd[2] != checksum_esperado_arq):
                                     atualizar_status_transferencia_arquivo_bd(conexao_bd_global, id_log_arquivo_em_progresso_bd, "AWAITING_PREPARE", total_bytes_atuais=0, detalhes_erro="Metadados do arquivo mudaram, reiniciando.")
                                
                                atualizar_status_transferencia_arquivo_bd(conexao_bd_global, id_log_arquivo_em_progresso_bd, "AWAITING_DATA")
                                socket_cliente.sendall(b"SEND_FROM_OFFSET:0")

                        elif cabecalho_str.startswith("START_FILE_DATA:"):
                            if not id_log_arquivo_em_progresso_bd:
                                logger_texto.error("Recebido START_FILE_DATA sem um PREPARE_FILE_TRANSFER anterior.")
                                socket_cliente.sendall(b"ERROR_NO_PREPARE_CALL")
                                continue

                            cursor_bd = conexao_bd_global.cursor()
                            cursor_bd.execute("SELECT relative_file_path, total_file_size_bytes, current_bytes_transferred, checksum_algorithm, expected_checksum_hex FROM file_transfer_log WHERE id = ?", (id_log_arquivo_em_progresso_bd,))
                            info_arquivo_bd = cursor_bd.fetchone()

                            if not info_arquivo_bd:
                                logger_texto.error(f"Não foi possível encontrar info no BD para file_log_id {id_log_arquivo_em_progresso_bd}")
                                socket_cliente.sendall(b"ERROR_SERVER_DB_LOOKUP_FAIL")
                                continue
                            
                            cam_rel_bd, tam_total_bd, offset_bd, cs_algo_bd, cs_hex_bd = info_arquivo_bd
                            caminho_fisico_completo_arq = os.path.join(caminho_base_upload_atual, cam_rel_bd)
                            
                            diretorio_do_arquivo = os.path.dirname(caminho_fisico_completo_arq)
                            if not os.path.exists(diretorio_do_arquivo): os.makedirs(diretorio_do_arquivo, exist_ok=True)

                            socket_cliente.sendall(b"ACK_START_FILE_DATA")
                            
                            status_final_do_recebimento = receber_dados_arquivo(socket_cliente, caminho_fisico_completo_arq, tam_total_bd, endereco_cliente_str, 
                                                                    id_log_arquivo_em_progresso_bd, offset_bd, cs_algo_bd, cs_hex_bd)
                            socket_cliente.sendall(status_final_do_recebimento.encode('utf-8'))
                            id_log_arquivo_em_progresso_bd = None 

                        elif cabecalho_str.startswith("START_FOLDER_TRANSFER:"):
                            nome_da_pasta = cabecalho_str.split(":", 1)[1]
                            caminho_base_upload_atual = os.path.join(PASTA_UPLOADS, os.path.basename(nome_da_pasta))
                            if not os.path.exists(caminho_base_upload_atual): os.makedirs(caminho_base_upload_atual)
                            registrar_evento_geral_bd(conexao_bd_global, "START_FOLDER_TRANSFER", endereco_cliente_str, f"Pasta: {nome_da_pasta}, Destino: {caminho_base_upload_atual}")
                            socket_cliente.sendall(b"ACK_START_FOLDER")
                        
                        elif cabecalho_str.startswith("NEW_FOLDER:"):
                            caminho_rel_pasta = cabecalho_str.split(":",1)[1]
                            caminho_completo_pasta = os.path.join(caminho_base_upload_atual, caminho_rel_pasta)
                            os.makedirs(caminho_completo_pasta, exist_ok=True)
                            registrar_evento_geral_bd(conexao_bd_global, "NEW_FOLDER_CREATED", endereco_cliente_str, f"Pasta: {caminho_completo_pasta}")
                            socket_cliente.sendall(b"ACK_NEW_FOLDER") 

                        elif cabecalho_str == "END_FOLDER_TRANSFER":
                            registrar_evento_geral_bd(conexao_bd_global, "END_FOLDER_TRANSFER", endereco_cliente_str, f"Pasta: {os.path.basename(caminho_base_upload_atual if caminho_base_upload_atual else 'N/A')}")
                            socket_cliente.sendall(b"ACK_END_FOLDER")
                            caminho_base_upload_atual = "" 
                        
                        else:
                            logger_texto.warning(f"Comando desconhecido de {endereco_cliente_str}: {cabecalho_str}")
                            registrar_evento_geral_bd(conexao_bd_global, "UNKNOWN_COMMAND", endereco_cliente_str, cabecalho_str)

                    except ConnectionResetError:
                        logger_texto.error(f"Conexão resetada por {endereco_cliente_str}.", exc_info=False)
                        registrar_evento_geral_bd(conexao_bd_global, "ERROR_CONNECTION_RESET", endereco_cliente=endereco_cliente_str)
                        conexao_com_cliente_ativa = False
                    except UnicodeDecodeError:
                        logger_texto.error(f"Erro ao decodificar cabeçalho de {endereco_cliente_str}.", exc_info=True)
                        registrar_evento_geral_bd(conexao_bd_global, "ERROR_UNICODE_DECODE", endereco_cliente=endereco_cliente_str)
                        conexao_com_cliente_ativa = False
                    except socket.timeout:
                        logger_texto.warning(f"Socket timeout para {endereco_cliente_str}.")
                        registrar_evento_geral_bd(conexao_bd_global, "ERROR_SOCKET_TIMEOUT", endereco_cliente=endereco_cliente_str)
                        conexao_com_cliente_ativa = False
                    except Exception as e_loop_cliente:
                        logger_texto.error(f"Erro no loop de tratamento do cliente {endereco_cliente_str}: {e_loop_cliente}", exc_info=True)
                        registrar_evento_geral_bd(conexao_bd_global, "ERROR_UNEXPECTED_CLIENT_LOOP", endereco_cliente=endereco_cliente_str, detalhes_evento=str(e_loop_cliente))
                        conexao_com_cliente_ativa = False
                
                logger_texto.info(f"Sessão com cliente {endereco_cliente_str} encerrada.")
    
    registrar_evento_geral_bd(conexao_bd_global, "SERVER_STOP")
    logger_texto.info("Servidor encerrado.")
    if conexao_bd_global:
        conexao_bd_global.close()

if __name__ == "__main__":
    main()