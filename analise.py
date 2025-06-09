# analise.py
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# --- Configurações ---
ARQUIVO_BD_SERVIDOR = "servidor_log.db"

def carregar_dados_do_servidor(caminho_bd):
    """
    Conecta ao banco de dados SQLite do servidor e carrega os logs de transferência
    de arquivos em um DataFrame do pandas.
    """
    if not os.path.exists(caminho_bd):
        print(f"Erro: O arquivo de banco de dados '{caminho_bd}' não foi encontrado.")
        print("Certifique-se de que o servidor já foi executado e transferiu alguns arquivos.")
        return None

    try:
        conexao_bd = sqlite3.connect(caminho_bd)
        # Query para selecionar todos os dados da tabela de log de arquivos
        query_sql = "SELECT * FROM file_transfer_log"
        
        # Carrega os dados para um DataFrame do pandas
        df = pd.read_sql_query(query_sql, conexao_bd)
        
        print(f"Dados carregados com sucesso de '{caminho_bd}'. {len(df)} registros encontrados.")
        
        conexao_bd.close()
        return df
    except Exception as e:
        print(f"Ocorreu um erro ao carregar os dados do banco de dados: {e}")
        return None

def analisar_e_plotar(df):
    """
    Realiza análises e gera gráficos a partir do DataFrame de logs.
    """
    if df is None or df.empty:
        print("DataFrame vazio. Nenhuma análise a ser feita.")
        return

    # --- Preparação dos Dados ---
    
    # Converte colunas de tempo para o formato datetime do pandas
    df['first_seen_timestamp'] = pd.to_datetime(df['first_seen_timestamp'])
    
    # Converte bytes para Kilobytes (KB) para melhor visualização nos gráficos
    df['tamanho_total_kb'] = df['total_file_size_bytes'] / 1024
    
    # Filtra apenas as transferências que foram completas e com checksum OK para análises de performance
    # O .copy() evita avisos de "SettingWithCopyWarning" do pandas
    df_sucesso = df[df['status'] == 'SUCCESS_CHECKSUM_OK'].copy()
    
    print("\n--- Análise Descritiva dos Arquivos Transferidos com Sucesso ---")
    if not df_sucesso.empty:
        # Mostra estatísticas básicas como média, desvio padrão, etc.
        # para as colunas numéricas mais importantes.
        estatisticas = df_sucesso[['tamanho_total_kb', 'final_duration_seconds', 'final_speed_KBps']].describe()
        print(estatisticas)
    else:
        print("Nenhum arquivo transferido com sucesso encontrado para análise de performance.")

    # --- Geração de Gráficos ---

    # Define um estilo visual mais agradável para os gráficos
    sns.set_theme(style="whitegrid")

    # 1. Gráfico de Status das Transferências (Contagem de Sucesso vs. Falhas)
    plt.figure(figsize=(12, 7))
    sns.countplot(data=df, y='status', order=df['status'].value_counts().index, palette="viridis")
    plt.title('Contagem de Status de Todas as Transferências de Arquivos', fontsize=16)
    plt.xlabel('Contagem (Nº de Tentativas)', fontsize=12)
    plt.ylabel('Status Final', fontsize=12)
    plt.tight_layout() # Ajusta o layout para não cortar os rótulos
    plt.savefig('grafico_status_transferencias.png')
    print("\nGráfico 'grafico_status_transferencias.png' salvo.")
    plt.show()

    # Continua apenas se houver dados de sucesso para plotar
    if df_sucesso.empty:
        return

    # 2. Gráfico de Dispersão: Velocidade de Transferência vs. Tamanho do Arquivo
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df_sucesso, x='tamanho_total_kb', y='final_speed_KBps', alpha=0.7)
    plt.title('Velocidade de Transferência vs. Tamanho do Arquivo', fontsize=16)
    plt.xlabel('Tamanho do Arquivo (KB)', fontsize=12)
    plt.ylabel('Velocidade Média (KB/s)', fontsize=12)
    # Usar escala logarítmica no eixo X pode ajudar a visualizar melhor
    # se os tamanhos dos arquivos variarem muito.
    plt.xscale('log')
    plt.savefig('grafico_velocidade_vs_tamanho.png')
    print("Gráfico 'grafico_velocidade_vs_tamanho.png' salvo.")
    plt.show()

    # 3. Histograma: Distribuição das Velocidades de Transferência
    plt.figure(figsize=(10, 6))
    sns.histplot(data=df_sucesso, x='final_speed_KBps', bins=20, kde=True) # kde adiciona uma linha de densidade
    plt.title('Distribuição das Velocidades de Transferência (Arquivos com Sucesso)', fontsize=16)
    plt.xlabel('Velocidade (KB/s)', fontsize=12)
    plt.ylabel('Frequência (Nº de Arquivos)', fontsize=12)
    plt.savefig('grafico_distribuicao_velocidades.png')
    print("Gráfico 'grafico_distribuicao_velocidades.png' salvo.")
    plt.show()

def main():
    """
    Função principal que orquestra o processo de análise.
    """
    print("Iniciando script de análise de logs de transferência...")
    dataframe_logs = carregar_dados_do_servidor(ARQUIVO_BD_SERVIDOR)
    analisar_e_plotar(dataframe_logs)
    print("\nAnálise concluída.")

if __name__ == "__main__":
    main()