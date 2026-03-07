import json
import pymysql
import sys
from datetime import datetime, timedelta
import os

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

# Converter tempo UTC para o horário do Brasil
def convert_to_brazil_time(utc_time_str):
    try:
        # Converter a string UTC para datetime
        utc_time = datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))
        # Ajustar para o fuso horário GMT-3
        brazil_time = utc_time - timedelta(hours=3)
        return brazil_time.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        print(f"Erro ao converter horário UTC '{utc_time_str}': {e}")
        return None

# Processar arquivos JSON e inserir ou atualizar dados no banco
def process_json_files(conn, directory):
    cursor = conn.cursor()
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            file_path = os.path.join(directory, filename)
            print(f"Processando arquivo: {file_path}")

            # Lendo o arquivo JSON
            with open(file_path, 'r') as file:
                records = json.load(file)

                for record in records:
                    if record.get('@type') == 'connection':
                        record_id = record.get('id').strip('@')
                        name = record.get('name')
                        description = record.get('description')
                        runtimeEnvironmentId = record.get('runtimeEnvironmentId')
                        instanceDisplayName = record.get('instanceDisplayName')
                        host = record.get('host')
                        database = record.get('database')
                        codepage = record.get('codepage')
                        authenticationType = record.get('authenticationType')
                        adjustedJdbcHostName = record.get('adjustedJdbcHostName')
                        schema = record.get('schema')
                        shortDescription = record.get('shortDescription')
                        type_ = record.get('type')
                        port = record.get('port')
                        password = record.get('password')
                        username = record.get('username')
                        majorUpdateTime = datetime.fromisoformat(record.get('majorUpdateTime').replace("Z", "+00:00"))
                        majorUpdateTime_brazil = convert_to_brazil_time(record.get('majorUpdateTime'))
                        timeout = record.get('timeout')
                        connParams = json.dumps(record.get('connParams'))
                        internal = record.get('internal')
                        federatedId = f"saas:@{record.get('federatedId')}"
                        retryNetworkError = record.get('retryNetworkError')
                        supportsCCIMultiGroup = record.get('supportsCCIMultiGroup')
                        metadataBrowsable = record.get('metadataBrowsable')
                        supportLabels = record.get('supportLabels')
                        vaultEnabled = record.get('vaultEnabled')
                        vaultEnabledParams = json.dumps(record.get('vaultEnabledParams'))

                        try:
                            # Inserir ou atualizar dados na tabela connections
                            insert_connection_query = """
                            INSERT INTO connections (
                                id, name, description, runtimeEnvironmentId, instanceDisplayName, host, database_name, codepage,
                                authenticationType, adjustedJdbcHostName, schema_name, shortDescription, type, port, password, username,
                                majorUpdateTime, majorUpdateTime_brazil, timeout, connParams, internal, federatedId,
                                retryNetworkError, supportsCCIMultiGroup, metadataBrowsable, supportLabels, vaultEnabled,
                                vaultEnabledParams
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                description = VALUES(description),
                                runtimeEnvironmentId = VALUES(runtimeEnvironmentId),
                                instanceDisplayName = VALUES(instanceDisplayName),
                                host = VALUES(host),
                                database_name = VALUES(database_name),
                                codepage = VALUES(codepage),
                                authenticationType = VALUES(authenticationType),
                                adjustedJdbcHostName = VALUES(adjustedJdbcHostName),
                                schema_name = VALUES(schema_name),
                                shortDescription = VALUES(shortDescription),
                                type = VALUES(type),
                                port = VALUES(port),
                                password = VALUES(password),
                                username = VALUES(username),
                                majorUpdateTime = VALUES(majorUpdateTime),
                                majorUpdateTime_brazil = VALUES(majorUpdateTime_brazil),
                                timeout = VALUES(timeout),
                                connParams = VALUES(connParams),
                                internal = VALUES(internal),
                                federatedId = VALUES(federatedId),
                                retryNetworkError = VALUES(retryNetworkError),
                                supportsCCIMultiGroup = VALUES(supportsCCIMultiGroup),
                                metadataBrowsable = VALUES(metadataBrowsable),
                                supportLabels = VALUES(supportLabels),
                                vaultEnabled = VALUES(vaultEnabled),
                                vaultEnabledParams = VALUES(vaultEnabledParams);
                            """
                            cursor.execute(insert_connection_query, (
                                record_id, name, description, runtimeEnvironmentId, instanceDisplayName, host, database, codepage,
                                authenticationType, adjustedJdbcHostName, schema, shortDescription, type_, port, password, username,
                                majorUpdateTime, majorUpdateTime_brazil, timeout, connParams, internal, federatedId,
                                retryNetworkError, supportsCCIMultiGroup, metadataBrowsable, supportLabels, vaultEnabled,
                                vaultEnabledParams
                            ))

                        except pymysql.MySQLError as e:
                            print(f"Erro ao tentar inserir/atualizar conexão {name}: {e}")

    conn.commit()

# Função principal
def main():
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E://engenharia//config//config.json')
    config = load_config(config_path)

    try:
        PathfileConnecion = config["PathfileConnecion"]
    except KeyError as e:
        print(f"Erro: Chave ausente no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)

    # Configurações do banco MySQL
    mysql_config = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'user': os.getenv('MYSQL_USER', 'seu_usuario'),
        'password': os.getenv('MYSQL_PASSWORD', 'sua_senha'),
        'database': os.getenv('MYSQL_DATABASE', 'nome_do_banco'),
        'autocommit': False
    }

    try:
        # Conectar ao banco de dados MySQL
        conn = pymysql.connect(**mysql_config)

        # Processar os arquivos JSON
        process_json_files(conn, PathfileConnecion)

        print("Processamento concluído com sucesso!")

    except pymysql.MySQLError as e:
        print(f"Erro ao conectar ao banco de dados MySQL: {e}")
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados MySQL encerrada.")

if __name__ == "__main__":
    main()