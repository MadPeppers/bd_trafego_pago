########## Parte 1: Importação e Configuração Inicial ##########

import pandas as pd
from sqlalchemy import create_engine, text
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
from datetime import datetime, timedelta

nome_cliente = 'muse'

def ler_credenciais(arquivo, cliente=None):
    with open(arquivo, 'r') as file:
        credenciais = {}
        for linha in file:
            chave, valor = linha.strip().split(' = ')
            if '{cliente}' in valor and cliente:
                valor = valor.replace('{cliente}', cliente)
            credenciais[chave] = valor.strip("'").strip('{}')
    return credenciais

credenciais_padrao = ler_credenciais("/home/matheus/public_html/myapp/creds/credenciais_padrao.txt")
credenciais_cliente = ler_credenciais(f"/home/matheus/public_html/myapp/creds/{nome_cliente}_credenciais_variaveis.txt", nome_cliente)

logging.basicConfig(filename=credenciais_cliente['log_path'], level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

########## Parte 2: Conexão com o Banco de Dados ##########

def connect_to_database():
    connection_string = (
        f"mssql+pyodbc://{credenciais_padrao['username']}:{credenciais_padrao['password']}@"
        f"{credenciais_padrao['server']}/{credenciais_padrao['database']}"
        "?driver=ODBC Driver 18 for SQL Server"
        "&connect timeout=60"
    )
    engine = create_engine(connection_string)
    logging.info("Conexao com o banco de dados estabelecida.")
    return engine

########## Parte 3: Função para Obter Dados do Google Sheets ##########

def get_data_from_sheets(sheet_name):
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credenciais_padrao['credentials_google_path'], scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(credenciais_cliente['sheet_id']).worksheet(sheet_name)
    data = sheet.get_all_records()
    logging.info(f"Dados obtidos do Google Sheets para {sheet_name} com sucesso.")
    return pd.DataFrame(data)

########## Parte 4: Função para Inserir Dados no Banco ##########

def insert_data_into_db(engine, df, nome_tabela, start_date, end_date):
    with engine.begin() as conn:
        delete_query = text(f"DELETE FROM {nome_tabela} WHERE Dia BETWEEN '{start_date.strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'")
        conn.execute(delete_query)
        df.to_sql(nome_tabela, con=conn, if_exists='append', index=False, chunksize=500)
        logging.info(f"Dados entre {start_date.strftime('%Y-%m-%d')} e {end_date.strftime('%Y-%m-%d')} excluídos com sucesso.")
        logging.info(f"Dados inseridos na {nome_tabela} com sucesso.")

########## Parte 5: Funções Principais e Execução ##########

def process_meta_ads(db_engine):
    try:
        logging.info("INICIADO PROCESSO DE ETL DO META ADS")
        df_meta_ads_data = get_data_from_sheets(credenciais_padrao['tabela_metaads'])
        if not df_meta_ads_data.empty:
            end_date = datetime.now() - timedelta(days=1)
            start_date = end_date - timedelta(days=6)
            insert_data_into_db(db_engine, df_meta_ads_data, credenciais_cliente['tabela_bd_meta'], start_date, end_date)
        else:
            logging.info("Nenhum dado para inserir.")
        logging.info("FINALIZADO PROCESSO DE ETL DO META ADS")
    except Exception as e:
        logging.error(f"Erro ao executar o ETL do Meta Ads: {e}")

def process_google_ads(db_engine):
    try:
        logging.info("INICIADO PROCESSO DE ETL DO GOOGLE ADS")
        df_google_ads_data = get_data_from_sheets(credenciais_padrao['tabela_googleads'])
        if not df_google_ads_data.empty:
            end_date = datetime.now() - timedelta(days=1)
            start_date = end_date - timedelta(days=6)
            insert_data_into_db(db_engine, df_google_ads_data, credenciais_cliente['tabela_bd_google'], start_date, end_date)
        else:
            logging.info("Nenhum dado para inserir.")
        logging.info("FINALIZADO PROCESSO DE ETL DO GOOGLE ADS")
    except Exception as e:
        logging.error(f"Erro ao executar o ETL do Google Ads: {e}")

def main():
    db_engine = connect_to_database()
    process_meta_ads(db_engine)
    process_google_ads(db_engine)

if __name__ == "__main__":
    main()