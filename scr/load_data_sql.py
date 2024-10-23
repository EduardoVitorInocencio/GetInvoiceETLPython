import os
import re
import pandas as pd

from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# OBTER AS VARIÁVEIS DO ARQUIVO (.env)
DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

# CRIAR UMA URL DE CONEXÃO DO BANCO DE DADOS
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

def tratar_dados(path:str):
    folderPath = path
    df_final = pd.DataFrame()

    for FileName in os.listdir(folderPath):
        fullFilePath = folderPath + FileName

        padrao = r"(\d{2}\.\d{2}\.\d{4})"

        # PROCURAR POR UMA CORRESPONDÊNCIA NA STRING USANDO A EXPRESSÃO REGULAR
        correspondencia = re.search(padrao, fullFilePath)

        if correspondencia:
            texto_extraido = correspondencia.group(1)
        
        if os.path.isfile(fullFilePath):
        # Abrindo o Data Frame
            df = pd.read_excel(fullFilePath)
            df['extractDate'] = texto_extraido
            df_final = pd.concat([df_final,df])

            print(texto_extraido)
        
    return df_final
    
def salvar_no_postgres(df, schema='public'):
    df.to_sql('getinvoice', engine, if_exists='replace', index=True, index_label='Date', schema=schema)
    print(f"Dados salvos no schema '{schema}' do banco de dados PostgreSQL")

if __name__ == '__main__':
    df = tratar_dados(path='C:\\Users\\edinocencio\\getinvoiceETLPython\\data_sources\\')
    salvar_no_postgres(df, schema='public')