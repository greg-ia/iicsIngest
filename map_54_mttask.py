import json
import pymysql
import os
import datetime
from datetime import timezone, timedelta
import sys
import codecs  # Para lidar com encodings diferentes

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

# Converter timestamps para horário do Brasil
def convert_timestamp_to_brazil(timestamp_ms):
    if timestamp_ms:
        utc_time = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
        brazil_time = utc_time - timedelta(hours=3)
        return brazil_time.strftime('%Y-%m-%d %H:%M:%S')  # Retorna no formato esperado pelo MySQL
    return '2019-01-01 00:00:00'

# Processar um arquivo JSON e inserir os dados no banco de dados MySQL
def process_json_file(conn, file_path):
    date_ins = datetime.datetime.now()

    # Abrir o arquivo JSON com o encoding correto
    with codecs.open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)

    task = data[0]

    task_dict = {
        'id': task.get('id', ''),
        'class': task.get('@type', ''),
        'name': task.get('name', ''),
        'description': task.get('description', ''),
        'runtimeEnvironmentId': task.get('runtimeEnvironmentId', ''),
        'maxLogs': task.get('maxLogs', 0),
        'is_verbose': bool(task.get('verbose', False)),
        'lastRunTime': convert_timestamp_to_brazil(task.get('lastRunTime', 0)),
        'lastRunTime_brazil': convert_timestamp_to_brazil(task.get('lastRunTime', 0)),
        'mappingId': task.get('mappingId', ''),
        'frsGuid': task.get('frsGuid', ''),
        'shortDescription': task.get('shortDescription', ''),
        'sessionPropertiesList': json.dumps(task.get('sessionPropertiesList', [])),
        'hidden': bool(task.get('hidden', False)),
        'enableCrossSchemaPushdown': bool(task.get('enableCrossSchemaPushdown', False)),
        'enableParallelRun': bool(task.get('enableParallelRun', False)),
        'autoTunedApplied': bool(task.get('autoTunedApplied', False)),
        'autoTunedAppliedType': task.get('autoTunedAppliedType', ''),
        'parameterFileName': task.get('parameterFileName', ''),
        'parameterFileDir': task.get('parameterFileDir', ''),
        'paramFileType': task.get('paramFileType', ''),
        'schemaMode': task.get('schemaMode', ''),
        'valid': bool(task.get('valid', False)),
        'schemaValidationErrorCount': task.get('schemaValidationErrorCount', 0),
        'parameterFileEncoding': task.get('parameterFileEncoding', ''),
        'serverless_properties': json.dumps(task.get('serverlessProperties', {})),
        'taskProperties': json.dumps(task.get('taskProperties', [])),
        'optimizationPlan': task.get('optimizationPlan', ''),
        'isMidstreamPreview': bool(task.get('isMidstreamPreview', False)),
        'parameters': json.dumps(task.get('parameters', [])),
        'sequences': json.dumps(task.get('sequences', [])),
        'inOutParameters': json.dumps(task.get('inOutParameters', [])),
        'connRuntimeAttrs': json.dumps(task.get('connRuntimeAttrs', [])),
        'Dt_Inserted': date_ins.strftime('%Y-%m-%d %H:%M:%S'),
        'Dt_updated': date_ins.strftime('%Y-%m-%d %H:%M:%S')
    }

    # Separar os campos "CodOnda" e "CodProcess" a partir do nome
    parts = task_dict['name'].split('_')
    task_dict['CodOnda'] = parts[3] if len(parts) > 3 else None
    task_dict['CodProcess'] = parts[4] if len(parts) > 4 else None

    columns = ', '.join([f"`{col}`" for col in task_dict.keys()])
    placeholders = ', '.join(['%s'] * len(task_dict))
    update_clause = ', '.join([f"`{col}`=VALUES(`{col}`)" for col in task_dict if col not in ['id', 'CodOnda', 'CodProcess', 'name', 'Dt_Inserted']])

    try:
        insert_task_query = f"""
        INSERT INTO s_task ({columns})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE
        {update_clause};
        """
        cursor = conn.cursor()
        cursor.execute(insert_task_query, tuple(task_dict.values()))
        conn.commit()

    except pymysql.MySQLError as e:
        print(f"CodOnda: {task_dict['CodOnda']}, CodProcess: {task_dict['CodProcess']}, Name: {task_dict['name']}")
        print(f"Erro ao tentar inserir {task_dict['name']}: {e}")
        conn.rollback()
        sys.exit(1)

# Função principal
def main():

    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E:\engenharia\config\config.json')
    config = load_config(config_path)

    try:
        PathfileTask = config["PathfileTask"]
    except KeyError as e:
        print(f"Erro: Chave ausente no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)

    # Configurar conexão com o MySQL
    mysql_config = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'user': os.getenv('MYSQL_USER', 'seu_usuario'),
        'password': os.getenv('MYSQL_PASSWORD', 'sua_senha'),
        'database': os.getenv('MYSQL_DATABASE', 'nome_do_banco'),
        'autocommit': False
    }

    try:
        # Conexão ao banco de dados
        conn = pymysql.connect(**mysql_config)

        # Processar cada arquivo JSON no diretório
        for filename in os.listdir(PathfileTask):
            if filename.endswith('.json'):
                # print(f"Processando arquivo: {filename}")
                file_path = os.path.join(PathfileTask, filename)
                process_json_file(conn, file_path)

        print("Todos os arquivos foram processados com sucesso.")

    except pymysql.MySQLError as e:
        print(f"Erro ao conectar ao banco de dados MySQL: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados MySQL encerrada.")

if __name__ == "__main__":
    main()