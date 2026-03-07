import pandas as pd
import pymysql
from datetime import datetime
import argparse
import os
import json
import sys

# Função para carregar o arquivo de configuração JSON
def load_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        return config
    except FileNotFoundError:
        print(f"Erro: Arquivo de configuração '{config_path}' não encontrado.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Erro: Configuração JSON inválida. Detalhes: {e}")
        sys.exit(1)

# Função principal
def main(assunto):
    # Carregar o arquivo de configuração
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E://engenharia//config//config.json')
    config = load_config(config_path)

    try:
        PathfileConnecion = config["json_file_path"] + f'/{assunto}'
    except KeyError as e:
        print(f"Erro: Chave ausente no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)

    # Configuração da conexão MySQL
    mysql_config = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'user': os.getenv('MYSQL_USER', 'seu_usuario'),
        'password': os.getenv('MYSQL_PASSWORD', 'sua_senha'),
        'database': os.getenv('MYSQL_DATABASE', 'nome_do_banco'),
        'autocommit': False
    }

    try:
        conn = pymysql.connect(**mysql_config)
        cursor = conn.cursor()

        # Processar cada arquivo CSV no diretório
        for filename in os.listdir(PathfileConnecion):
            if filename.endswith('.csv'):
                caminho_arquivo_csv = os.path.join(PathfileConnecion, filename)
                print(f"Processando arquivo: {caminho_arquivo_csv}")

                # Leitura do arquivo CSV usando o pandas
                dados_csv = pd.read_csv(caminho_arquivo_csv)
                print(f"Total de registros no CSV: {len(dados_csv)}")

                # Adicionar campos adicionais
                dados_csv['DateIns'] = datetime.now()
                dados_csv['DateUpd'] = datetime.now()

                # Inserir os dados na tabela existente
                insert_query = """
                INSERT INTO ContentsofExportPackage (
                    id, CodOnda, CodProcess, objectPath, objectName, objectType, Dt_Inserted, Dt_updated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    objectPath = VALUES(objectPath),
                    objectName = VALUES(objectName),
                    Dt_updated = VALUES(Dt_updated);
                """

                registros_processados = 0
                registros_inseridos = 0

                # Inserir cada linha no banco de dados
                for index, row in dados_csv.iterrows():
                    registros_processados += 1
                    
                    if row['objectType'] in ['MTT', 'TASKFLOW', 'DTEMPLATE']:
                        parts = row['objectName'].split('_')

                        if row['objectType'] == 'TASKFLOW':
                            CodOnda = parts[1] if len(parts) > 1 else None
                            CodProcess = parts[2] if len(parts) > 2 else None
                        else:
                            CodOnda = parts[3] if len(parts) > 3 else None
                            CodProcess = parts[4] if len(parts) > 4 else None

                        # DEBUG: Mostrar valores extraídos
                        # print(f"objectName: {row['objectName']}, CodOnda: {CodOnda}, CodProcess: {CodProcess}")

                        # CORREÇÃO: Verificar se CodOnda está na lista de valores permitidos
                        if CodOnda not in ['653', '673', '674', '684', '702']:
                            CodOnda = 0
                            CodProcess = 0
                        else:
                            # Converter para inteiro se necessário
                            try:
                                CodOnda = int(CodOnda) if CodOnda else 0
                                CodProcess = int(CodProcess) if CodProcess else 0
                            except (ValueError, TypeError):
                                CodOnda = 0
                                CodProcess = 0

                        try:
                            # Executar a query com os valores
                            cursor.execute(insert_query, (
                                row['id'], CodOnda, CodProcess, row['objectPath'], 
                                row['objectName'], row['objectType'], 
                                row['DateIns'], row['DateUpd']
                            ))
                            registros_inseridos += 1
                            
                            # Mostrar progresso a cada 100 registros
                            if registros_inseridos % 100 == 0:
                                print(f"Registros inseridos: {registros_inseridos}")

                        except pymysql.MySQLError as e:
                            print(f"Erro ao inserir registro {row['id']}: {e}")
                            print(f"Detalhes - CodOnda: {CodOnda}, CodProcess: {CodProcess}, objectName: {row['objectName']}")
                            conn.rollback()
                            # Continue processando outros registros em vez de sair
                            continue

                # Confirmar alterações no banco de dados após cada arquivo
                conn.commit()
                print(f"Arquivo {filename} processado:")
                print(f"  - Registros processados: {registros_processados}")
                print(f"  - Registros inseridos/atualizados: {registros_inseridos}")

        print("Todos os arquivos processados com sucesso.")

    except pymysql.MySQLError as e:
        print(f"Erro ao conectar ao banco de dados MySQL: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Erro inesperado: {e}")
        if 'conn' in locals():
            conn.rollback()
        sys.exit(1)
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Conexão com o banco de dados MySQL encerrada.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Processa e insere dados CSV no banco de dados MySQL.")
    parser.add_argument('assunto', type=str, help='Parâmetro para substituir no caminho do arquivo CSV.')
    args = parser.parse_args()
    main(args.assunto)