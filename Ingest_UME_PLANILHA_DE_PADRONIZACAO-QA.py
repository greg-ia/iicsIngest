import pandas as pd
import sys
import json
import os
import logging
import pymysql
from dotenv import load_dotenv
# Carregar variáveis de ambiente
load_dotenv()

# Configuração de logging
logging.basicConfig(level=logging.INFO)

# Verificar se foram fornecidos argumentos de linha de comando
if len(sys.argv) != 2:
    logging.error("Por favor: Passe os parâmetros nesta ordem:")
    logging.error("     1) Código da Onda: ")
    logging.error(r"Ex.: python DoublIngest_UME_PLANILHA_DE_PADRONIZACAO-QA.py 653")
    sys.exit(1)

CodOnda = sys.argv[1]

# Mapeamento de códigos de onda para nomes
onda_mapping = {
    "653": "UME_RECEITA",
    "673": "UME_FATURAMENTO",
    "674": "UME_RENTABILIDADE",
    "684": "UME_TRAFEGO",
    "702": "UME_DISPONIBILIDADES",
    "781": "CONEXAO_TORPEDO_NOVA_PLATAFORMA"
}

NmeOnda = onda_mapping.get(CodOnda)
if not NmeOnda:
    logging.error("Essa onda não está no escopo do projeto.")
    sys.exit(1)

# Função para carregar o arquivo de configuração
def load_config(config_path):
    if not os.path.exists(config_path):
        logging.error(f"Arquivo de configuração não encontrado: {config_path}")
        sys.exit(1)
    with open(config_path, 'r') as config_file:
        return json.load(config_file)

# Obtém o caminho do arquivo de configuração
config_path = os.getenv('CONFIG_PATH')
config = load_config(config_path)

# Configurações para conexão com MySQL
mysql_config = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE')
}

# Nome da tabela no banco de dados MySQL
nome_tabela_mysql = f'padronizacao_migracao_{CodOnda}'

# Caminho do arquivo Excel
PathExcelfingerhard = rf'{config["PathExcelfingerhard"]}/{NmeOnda}_PADRONIZACAO-QA.xlsx'

if not os.path.exists(PathExcelfingerhard):
    logging.error(f"Arquivo Excel não encontrado: {PathExcelfingerhard}")
    sys.exit(1)

# Ler a planilha Excel para um DataFrame do Pandas
df_excel = pd.read_excel(PathExcelfingerhard)


# Remover espaços e caracteres especiais dos nomes das colunas
df_excel.columns = [col.replace(' ', '_').replace('.', '').replace('(', '').replace(')', '') for col in df_excel.columns]

# Regras de renomeação dinâmicas
if CodOnda == "653":
    # Regra 1: Para CodOnda 653, UME_RECEITA -> RECEITA_UME
    if "UME_RECEITA" in df_excel.columns:
        df_excel.rename(columns={'UME_RECEITA': 'RECEITA_UME'}, inplace=True)
    else:
        logging.warning("Para CodOnda 653, a coluna 'UME_RECEITA' não foi encontrada no Excel.")
# Regras de renomeação dinâmicas
elif CodOnda == "673":
    # Regra 1: Para CodOnda 653, UME_RECEITA -> RECEITA_UME
    if "UME_FATURAMENTO" in df_excel.columns:
        df_excel.rename(columns={'UME_FATURAMENTO': 'RECEITA_UME'}, inplace=True)
    else:
        logging.warning("Para CodOnda 673, a coluna 'UME_FATURAMENTO' não foi encontrada no Excel.")               
elif CodOnda == "674":
    # Regra 1: Para CodOnda 653, UME_RECEITA -> RECEITA_UME
    if "UME_RENTAB" in df_excel.columns:
        df_excel.rename(columns={'UME_RENTAB': 'RECEITA_UME'}, inplace=True)
    else:
        logging.warning("Para CodOnda 674, a coluna 'UME_RENTABILIDADE' não foi encontrada no Excel.")     
elif CodOnda == "684":
    # Regra 2: Para CodOnda 684, UME_TRAFEGO -> RECEITA_UME
    if "UME_TRAFEGO" in df_excel.columns:
        df_excel.rename(columns={'UME_TRAFEGO': 'RECEITA_UME'}, inplace=True)
    else:
        logging.warning("Para CodOnda 684, a coluna 'UME_TRAFEGO' não foi encontrada no Excel.")
        
    # Regra 3: Para CodOnda 684, DSSGER será mantida como está
    if "TCOGER" in df_excel.columns:
        logging.info("A coluna DSSGER será mantida no DataFrame.")
        df_excel.rename(columns={'TCOGER': 'DSSGER'}, inplace=True)
    else:
        logging.error("A coluna DSSGER não foi encontrada no arquivo Excel.")
        sys.exit(1)
   
elif CodOnda == "781":
    # Regra 2: Para CodOnda 781, CONEXAO_TORPEDO_NOVA_PLATAFORMA
    if "CONEXAO_TORPEDO_NP" in df_excel.columns:
        df_excel.rename(columns={'DWHSTG': 'DWHSTG'}, inplace=True)
    else:
        logging.warning("Para CodOnda 781, a coluna 'CONEXAO_TORPEDO_NP' não foi encontrada no Excel.")
        
    # Regra 3: Para CodOnda 684, DSSGER será mantida como está
    if "DWHSTG" in df_excel.columns:
        logging.info("A coluna DWHSTG será mantida no DataFrame.")
        df_excel.rename(columns={'DWHSTG': 'DWHSTG'}, inplace=True)
    else:
        logging.error("A coluna DWHSTG não foi encontrada no arquivo Excel.")
        sys.exit(1)        
elif CodOnda == "702":
    # Regra 2: Para CodOnda 684, UME_TRAFEGO -> RECEITA_UME
    if "UME_DISPONIB" in df_excel.columns:
        df_excel.rename(columns={'UME_DISPONIB': 'RECEITA_UME'}, inplace=True)
    else:
        logging.warning("Para CodOnda 702, a coluna 'UME_DISPONIB' não foi encontrada no Excel.")
        
    # Regra 3: Para CodOnda 684, DSSGER será mantida como está
    if "GRW" in df_excel.columns:
        logging.info("A coluna GRW será mantida no DataFrame.")
        df_excel.rename(columns={'GRW': 'DSSGER'}, inplace=True)
    else:
        logging.error("A coluna DSSGER não foi encontrada no arquivo Excel.")
        sys.exit(1)
# Log das colunas ajustadas (para verificar as renomeações)
#logging.info(f"Nomes das colunas no DataFrame após ajustes: {df_excel.columns}")

# Tratar valores NaN no DataFrame
df_excel = df_excel.where(pd.notnull(df_excel), None)

# Função auxiliar para tratar valores
def tratar_valores_para_mysql(row):
    return [None if pd.isna(value) else value for value in row]

try:
    # Conectar ao banco de dados MySQL usando pymysql
    conn = pymysql.connect(**mysql_config)
    cursor = conn.cursor()

    # Truncar a tabela antes de inserir novos dados
    cursor.execute(f'TRUNCATE TABLE {nome_tabela_mysql}')
    conn.commit()

    # Escapar colunas para evitar conflitos (ex: `KEY`, `SQL`, etc.)
    colunas = ', '.join([f'`{col}`' for col in df_excel.columns])
    valores = ', '.join(['%s'] * len(df_excel.columns))

    insert_query = f"INSERT INTO {nome_tabela_mysql} ({colunas}) VALUES ({valores})"

    # Inserir os dados linha por linha
    for _, row in df_excel.iterrows():
        cursor.execute(insert_query, tratar_valores_para_mysql(row))

    conn.commit()
    logging.info(f"Dados inseridos com sucesso na tabela {nome_tabela_mysql}")

except pymysql.MySQLError as err:
    logging.error(f"Erro ao conectar ao MySQL: {err}")
    sys.exit(1)

finally:
    if 'cursor' in locals():
        cursor.close()
    if 'conn' in locals():
        conn.close()