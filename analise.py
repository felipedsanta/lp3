import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

ARQUIVO_BD_SERVIDOR = "servidor_log.db"

def carregar_dados_do_servidor(caminho_bd):

    if not os.path.exists(caminho_bd):
        print(f"Erro: O arquivo de banco de dados '{caminho_bd}' não foi encontrado.")
        print("Certifique-se de que o servidor já foi executado e transferiu alguns arquivos.")
        return None

    try:
        conexao_bd = sqlite3.connect(caminho_bd)
        query_sql = "SELECT * FROM file_transfer_log"
        
        df = pd.read_sql_query(query_sql, conexao_bd)
        
        print(f"Dados carregados com sucesso de '{caminho_bd}'. {len(df)} registros encontrados.")
        
        conexao_bd.close()
        return df
    except Exception as e:
        print(f"Ocorreu um erro ao carregar os dados do banco de dados: {e}")
        return None

def analisar_e_plotar(df):
    if df is None or df.empty:
        print("DataFrame vazio. Nenhuma análise a ser feita.")
        return

    df['first_seen_timestamp'] = pd.to_datetime(df['first_seen_timestamp'])
    
    df['tamanho_total_kb'] = df['total_file_size_bytes'] / 1024
    

    df_sucesso = df[df['status'] == 'SUCCESS_CHECKSUM_OK'].copy()
    
    print("\n--- Análise Descritiva dos Arquivos Transferidos com Sucesso ---")
    if not df_sucesso.empty:
        estatisticas = df_sucesso[['tamanho_total_kb', 'final_duration_seconds', 'final_speed_KBps']].describe()
        print(estatisticas)
    else:
        print("Nenhum arquivo transferido com sucesso encontrado para análise de performance.")

    sns.set_theme(style="whitegrid")

    plt.figure(figsize=(12, 7))
    sns.countplot(data=df, y='status', order=df['status'].value_counts().index, palette="viridis")
    plt.title('Contagem de Status de Todas as Transferências de Arquivos', fontsize=16)
    plt.xlabel('Contagem (Nº de Tentativas)', fontsize=12)
    plt.ylabel('Status Final', fontsize=12)
    plt.tight_layout()
    plt.savefig('grafico_status_transferencias.png')
    print("\nGráfico 'grafico_status_transferencias.png' salvo.")
    plt.show()

    if df_sucesso.empty:
        return
    
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df_sucesso, x='tamanho_total_kb', y='final_speed_KBps', alpha=0.7)
    plt.title('Velocidade de Transferência vs. Tamanho do Arquivo', fontsize=16)
    plt.xlabel('Tamanho do Arquivo (KB)', fontsize=12)
    plt.ylabel('Velocidade Média (KB/s)', fontsize=12)
    plt.xscale('log')
    plt.savefig('grafico_velocidade_vs_tamanho.png')
    print("Gráfico 'grafico_velocidade_vs_tamanho.png' salvo.")
    plt.show()

    plt.figure(figsize=(10, 6))
    sns.histplot(data=df_sucesso, x='final_speed_KBps', bins=20, kde=True)
    plt.title('Distribuição das Velocidades de Transferência (Arquivos com Sucesso)', fontsize=16)
    plt.xlabel('Velocidade (KB/s)', fontsize=12)
    plt.ylabel('Frequência (Nº de Arquivos)', fontsize=12)
    plt.savefig('grafico_distribuicao_velocidades.png')
    print("Gráfico 'grafico_distribuicao_velocidades.png' salvo.")
    plt.show()

def main():
    print("Iniciando script de análise de logs de transferência...")
    dataframe_logs = carregar_dados_do_servidor(ARQUIVO_BD_SERVIDOR)
    analisar_e_plotar(dataframe_logs)
    print("\nAnálise concluída.")

if __name__ == "__main__":
    main()
    










    
"""
if 'transfer_start_data_timestamp' not in df_sucesso.columns or df_sucesso['transfer_start_data_timestamp'].isnull().all():
        print("Não há dados de 'transfer_start_data_timestamp' para analisar a performance por hora.")
        return
        
    df_sucesso['transfer_start_data_timestamp'] = pd.to_datetime(df_sucesso['transfer_start_data_timestamp'])

    df_sucesso['hora_do_dia'] = df_sucesso['transfer_start_data_timestamp'].dt.hour

    performance_por_hora = df_sucesso.groupby('hora_do_dia').agg(
        velocidade_media_KBps=('final_speed_KBps', 'mean'),
        numero_de_arquivos=('id', 'count')
    )
    
    performance_por_hora = performance_por_hora.reindex(range(24), fill_value=0)

    if performance_por_hora.empty:
        print("Nenhum dado de sucesso para analisar a performance por hora.")
        return
        
    print("Velocidade média e contagem de arquivos por hora do dia:")
    print(performance_por_hora)

    plt.figure(figsize=(15, 7))
    sns.barplot(x=performance_por_hora.index, y=performance_por_hora['velocidade_media_KBps'], palette="plasma")
    plt.title('Performance Média de Transferência por Hora do Dia', fontsize=16)
    plt.xlabel('Hora do Dia (0-23)', fontsize=12)
    plt.ylabel('Velocidade Média (KB/s)', fontsize=12)
    plt.xticks(range(24)) # Garante que todos os ticks de hora sejam mostrados
    plt.tight_layout()
    plt.savefig('grafico_velocidade_por_hora.png')
    print("\n Gráfico 'grafico_velocidade_por_hora.png' salvo.")
    plt.show()

"""