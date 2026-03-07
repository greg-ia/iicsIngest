import json
import os
import pymysql
from datetime import datetime, timedelta, timezone
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
        print(f"Erro: Configuração JSON inválida no arquivo fornecido. Detalhes: {e}")
        sys.exit(1)

# Converte o timestamp em milissegundos para horário local brasileiro (UTC-3)
def convert_to_brazil_time(epoch_time_ms):
    try:
        utc_time = datetime.fromtimestamp(epoch_time_ms / 1000, tz=timezone.utc)
        brazil_tz = timezone(timedelta(hours=-3))
        brazil_time = utc_time.astimezone(brazil_tz)
        return brazil_time
    except Exception as e:
        print(f"Erro na conversão de horário: {e}")
        return None

# Processa os arquivos JSON e insere/atualiza os registros no banco de dados MySQL
def process_json_files(conn, directory, deploy):
    # Data de hoje para controle
    today = datetime.today().date()
    yesterday = datetime.today().date() - timedelta(days=3)
    

    # Listar todos os arquivos JSON no diretório
    try:
        arquivos_json = [f for f in os.listdir(directory) if f.endswith('.json')]
    except FileNotFoundError:
        print(f"Erro: Diretório '{directory}' não encontrado.")
        sys.exit(1)

    if not arquivos_json:
        print(f"Atenção: Nenhum arquivo JSON encontrado no diretório '{directory}'.")
        return

    for filename in arquivos_json:
        file_path = os.path.join(directory, filename)

        # Ler cada arquivo JSON
        with open(file_path, 'r', encoding='utf-8') as file:
            try:
                records = json.load(file)  # Carregar registros JSON
            except json.JSONDecodeError as e:
                print(f"Erro ao carregar o arquivo JSON '{filename}': {e}")
                continue
        
            # Copiar registros históricos para transformation_file_records_hist
        if deploy == 'DD':
            try:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                    INSERT INTO transformation_file_records_hist
                    SELECT DISTINCT
                        t.id, t.CodOnda, t.CodProcess, t.name, t.type, t.size, t.updated_time, t.updated_time_brazil
                    FROM transformation_file_records t
                    WHERE DATE(t.updated_time_brazil) >= %s
                    AND t.type NOT IN ('IMAGE')
                    ON DUPLICATE KEY UPDATE 
                        name = VALUES(name),
                        type = VALUES(type),
                        size = VALUES(size),
                        updated_time = VALUES(updated_time),
                        updated_time_brazil = VALUES(updated_time_brazil);
                    """, (yesterday,))
                    print("Copiados registros históricos para transformation_file_records_hist com sucesso.")
            except pymysql.MySQLError as e:
                print(f"Erro ao copiar registros para transformation_file_records_hist: {e}")
                continue

           # Processar cada registro no arquivo JSON
        for record in records:
            if record.get('@type') == 'fileRecord':
                try:
                    # Extrair informações do registro
                    record_id = int(record.get('id').strip('@'))
                    name = record.get('name')
                    type_ = record.get('type')
                    size = record.get('size')
                    updated_time = datetime.utcfromtimestamp(record.get('attachTime') / 1000)
                    updated_time_brazil = convert_to_brazil_time(record.get('attachTime'))

                    # Extrair CodOnda e CodProcess do nome, caso o formato seja válido
                    parts = name.split('_')
                    CodOnda = parts[3] if len(parts) > 3 else None
                    CodProcess = parts[4] if len(parts) > 4 else None
                    
                    # Escolher a tabela com base no tipo de deploy
                         
                    
                    if deploy == "DD":
                        table_name = "transformation_file_records" 
                    else:
                        table_name = "transformation_file_records_double"                    
                    # Inserir ou atualizar registros na tabela

                    insert_file_record_query = f"""
                    INSERT INTO {table_name} (
                        id, name, type, size, updated_time, updated_time_brazil, CodOnda, CodProcess
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        name = VALUES(name),
                        type = VALUES(type),
                        size = VALUES(size),
                        updated_time = VALUES(updated_time),
                        updated_time_brazil = VALUES(updated_time_brazil);
                    """
                    with conn.cursor() as cursor:
                        cursor.execute(insert_file_record_query, (
                            record_id, name, type_, size, updated_time, updated_time_brazil, CodOnda, CodProcess
                        ))
                except Exception as e:
                    print(f"Erro ao processar o registro do arquivo '{filename}': {e}")

    print(f"Processamento concluído! Registros dos arquivos JSON foram tratados com sucesso.")

def main():
    # Carregar o arquivo de configuração
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E://engenharia//config//config.json')
    config = load_config(config_path)

    try:
        # Caminhos definidos no arquivo de configuração
        PathFileRecordsGrava = config["PathFileRecordsGrava"]
        PathFileRecordsDoubleCheckGrava = config["PathFileRecordsDoubleCheckGrava"]

    except KeyError as e:
        print(f"Erro: Chave ausente no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)
    
    # Validar argumentos de linha de comando
    if len(sys.argv) != 2:
        print("Uso: python map_03_transformation_fileRecords.py DD")
        print("Onde <deploy> pode ser 'DD' (Dedo Duro) ou o nome do pacote de deployment.")
        sys.exit(1)

    deploy = sys.argv[1]

    # Determinar o caminho com base no tipo de deploy
    if deploy == "DD":
        directory = PathFileRecordsGrava
    else:
        directory = PathFileRecordsDoubleCheckGrava

    # Configuração de conexão com o MySQL
    mysql_config = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),  # Host do banco de dados MySQL
        'user': os.getenv('MYSQL_USER', 'seu_usuario'),  # Nome do usuário
        'password': os.getenv('MYSQL_PASSWORD', 'sua_senha'),  # Senha do banco
        'database': os.getenv('MYSQL_DATABASE', 'nome_do_banco'),  # Nome do banco de dados
        'autocommit': True  # Habilita autocommit por padrão
    }

    try:
        # Conectar ao banco de dados MySQL
        conn = pymysql.connect(**mysql_config)

        # Truncar a tabela `transformation_file_records_double` para um novo processamento

        if deploy == "DD":
            table_name = "transformation_file_records" 
        else:
            table_name = "transformation_file_records_double"                    
                    # Inserir ou atualizar registros na tabela

        with conn.cursor() as cursor:
            cursor.execute(f'TRUNCATE TABLE {table_name}')
            print(f"Tabela {table_name} truncada com sucesso.")

        # Processar os arquivos JSON
        process_json_files(conn, directory, deploy)

        # Confirmar alterações
        conn.commit()

    except pymysql.MySQLError as e:
        print(f"Erro de MySQL: {e}")
        sys.exit(1)

    finally:
        # Fechar a conexão com o banco de dados MySQL
        if conn:
            conn.close()
            print("Conexão com o banco de dados MySQL fechada.")

if __name__ == "__main__":
    main()