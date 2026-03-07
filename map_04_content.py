import json
import os
import pymysql
import datetime
import codecs  # Para lidar com encodings diferentes
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

# Função para processar o arquivo JSON e inserir/atualizar registros no banco MySQL
def process_json_file(conn, file_path):
    # Data de execução
    date_ins = datetime.datetime.now()

    # Abrir o arquivo usando o utf-8 explicitamente
    with codecs.open(file_path, 'r', encoding='utf-8') as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError as e:
            print(f"Erro ao carregar o arquivo JSON '{file_path}': {e}")
            return

    # Extrair dados do "content"
    content = data['content']
    content_id = content.get('$$IID')
    content_class = content.get('$$class')
    annotations = json.dumps(content.get('annotations'))
    big_int_convert_type = content.get('bigIntConvertType')
    document_type = content.get('documentType')
    eco_system = content.get('ecoSystem')
    name = content.get('name')
    template_origin = content.get('templateOrigin')
    links = json.dumps(content.get('links'))
    transformations = json.dumps(content.get('transformations'))
    variables = json.dumps(content.get('variables'))

    # Extrair CodOnda e CodProcess a partir do nome
    parts = name.split('_')
    CodOnda = parts[3] if len(parts) > 3 else None
    CodProcess = parts[4] if len(parts) > 4 else None

    #print(f"Processando: {name}")

    # Inserir ou atualizar dados na tabela "content"
    try:
        insert_content_query = """
        INSERT INTO content (
            id, class, CodOnda, CodProcess, annotations, big_int_convert_type, document_type, 
            eco_system, name, template_origin, links, transformations, variables, Dt_Inserted, Dt_updated
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            class = VALUES(class),
            annotations = VALUES(annotations),
            big_int_convert_type = VALUES(big_int_convert_type),
            document_type = VALUES(document_type),
            eco_system = VALUES(eco_system),
            template_origin = VALUES(template_origin),
            links = VALUES(links),
            transformations = VALUES(transformations),
            variables = VALUES(variables),
            Dt_updated = VALUES(Dt_updated);
        """
        with conn.cursor() as cursor:
            cursor.execute(insert_content_query, (
                content_id, content_class, CodOnda, CodProcess, annotations, big_int_convert_type, document_type,
                eco_system, name, template_origin, links, transformations, variables, date_ins, date_ins
            ))

    except pymysql.MySQLError as e:
        print(f"Erro ao tentar inserir o registro '{name}': {e}")

# Função principal
def main():
    # Carregar o arquivo de configuração
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E://engenharia//config//config.json')
    config = load_config(config_path)

    try:
        # Caminhos definidos no arquivo de configuração
        PathMapsGrava = config["PathMapsGrava"]
    except KeyError as e:
        print(f"Erro: Chave ausente no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)

    # Diretório contendo os arquivos JSON
    directory = PathMapsGrava

    # Configuração de conexão com banco MySQL
    mysql_config = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),  # Host do banco de dados MySQL
        'user': os.getenv('MYSQL_USER', 'seu_usuario'),  # Nome do usuário
        'password': os.getenv('MYSQL_PASSWORD', 'sua_senha'),  # Senha do banco
        'database': os.getenv('MYSQL_DATABASE', 'nome_do_banco'),  # Nome do banco de dados
        'autocommit': True  # Habilitar autocommit
    }

    # Conectar ao banco de dados MySQL
    try:
        conn = pymysql.connect(**mysql_config)
    except pymysql.MySQLError as e:
        print(f"Erro ao conectar ao banco de dados MySQL: {e}")
        sys.exit(1)

    # Processar cada arquivo JSON no diretório
    try:
        for filename in os.listdir(directory):
            if filename.endswith('.json'):
                file_path = os.path.join(directory, filename)
                process_json_file(conn, file_path)

        # Persistir as alterações no banco de dados
        conn.commit()
        print("Processamento concluído com sucesso!")

    except Exception as e:
        print(f"Erro ao processar os arquivos JSON: {e}")
    finally:
        # Fechar a conexão com o banco de dados MySQL
        if conn:
            conn.close()
            print("Conexão com o banco de dados MySQL finalizada.")

if __name__ == "__main__":
    main()