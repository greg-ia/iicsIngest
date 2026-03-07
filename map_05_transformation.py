import json
import datetime
import pymysql
import sys
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
        print(f"Erro: Configuração JSON inválida no arquivo fornecido. Detalhes: {e}")
        sys.exit(1)

# Função para validar timestamps
def validate_timestamp(value):
    """Valida e retorna um valor timestamp correto ou o valor padrão."""
    if not value or value.strip() == "":
        print("Timestamp ausente ou vazio. Usando valor padrão.")
        return "1990-01-01 00:00:00"

    try:
        # Tenta converter o formato ISO 8601
        if value.endswith('Z'):
            value = value[:-1]  # Remove o sufixo 'Z'
        parsed = datetime.datetime.fromisoformat(value)
        return parsed.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        print(f"Erro ao validar timestamp '{value}': {e}")
        return "1990-01-01 00:00:00"

# Função para converter o horário UTC para o horário do Brasil
def convert_to_brazil_time(utc_time_str):
    """Converte a data UTC para o timezone do Brasil ou retorna uma data padrão."""
    if not utc_time_str or utc_time_str.strip() == "":
        return "1990-01-01 00:00:00"
    try:
        utc_time = datetime.datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))
        brazil_time = utc_time - datetime.timedelta(hours=3)
        return brazil_time.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError as e:
        return "1990-01-01 00:00:00"

# Função principal
def main():
    """Função principal do script."""
    if len(sys.argv) != 2:
        print("Por favor, forneça o código da onda como argumento.")
        sys.exit(1)

    CodigoOnda = sys.argv[1]

    # Carregar o caminho para o JSON de configuração
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E:\engenharia\config\config.json')
    config = load_config(config_path)

    try:
        # Conexão e configurações
        PathMapsGrava = config["PathMapsGrava"]
    except KeyError as e:
        print(f"Erro: Chave ausente no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)

    # Configuração e conexão ao banco de dados MySQL
    mysql_config = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),  # Host do banco de dados MySQL
        'user': os.getenv('MYSQL_USER', 'seu_usuario'),  # Usuário do banco
        'password': os.getenv('MYSQL_PASSWORD', 'sua_senha'),  # Senha do banco
        'database': os.getenv('MYSQL_DATABASE', 'nome_do_banco'),  # Nome do banco
        'autocommit': True
    }

    try:
        conn = pymysql.connect(**mysql_config)
        cursor = conn.cursor()

        # Movendo dados históricos para tabela `transformation_hist`
        query_ins = f"""
        INSERT INTO transformation_hist
        SELECT DISTINCT t.*
        FROM transformation t
        JOIN transformation_file_records tfr 
            ON t.CodOnda = tfr.CodOnda AND t.CodProcess = tfr.CodProcess
        WHERE DATE(tfr.updated_time_brazil) = (
            SELECT MAX(DATE(updated_time_brazil)) FROM transformation_file_records
        ) AND tfr.TYPE NOT IN ('IMAGE')
          AND t.CodOnda = %s;
        """
        cursor.execute(query_ins, (CodigoOnda,))

        # Limpando dados da onda na tabela principal
        delete_query = "DELETE FROM transformation WHERE CodOnda = %s;"
        cursor.execute(delete_query, (CodigoOnda,))

        # Lendo dados da tabela `content`
        today = datetime.date.today().strftime('%Y-%m-%d')
        content_query = f"""
        SELECT DISTINCT a.CodOnda, a.CodProcess, a.name, a.transformations
        FROM content a
        WHERE a.CodOnda = %s AND DATE(a.dt_updated) = %s;
        """
        cursor.execute(content_query, (CodigoOnda, today))
        contents = cursor.fetchall()

        print(f"Registros lidos da tabela content: {len(contents)}")

        for content in contents:
            CodOnda, CodProcess, content_name, transformations = content
            try:
                transformations_list = json.loads(transformations)
            except json.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON para content_name '{content_name}': {e}")
                continue

            for transformation in transformations_list:
                try:
                    transformation_id = transformation.get('$$ID')
                    transformation_class = transformation.get('$$class')
                    annotations = json.dumps(transformation.get('annotations'))

                    # Pega a data original ou ajusta para "1990-01-01 00:00:00" se estiver inválida/nula
                    create_time = transformation.get('createTime', '')
                    create_time = validate_timestamp(create_time)
                    
                    # Converte para timezone do Brasil ou usa valor padrão
                    create_time_brazil = convert_to_brazil_time(create_time)

                    name = transformation.get('name')
                    advanced_properties = json.dumps(transformation.get('advancedProperties'))
                    groups = json.dumps(transformation.get('groups'))
                    session_properties = json.dumps(transformation.get('sessionProperties'))
                    generate_filename_port = transformation.get('generateFilenamePort') == "true"
                    use_labels = transformation.get('useLabels') == "true"
                    use_sequence_fields = transformation.get('useSequenceFields') == "true"
                    fields = json.dumps(transformation.get('fields'))
                    data_adapter = json.dumps(transformation.get('dataAdapter'))
                    input_sorted = json.dumps(transformation.get('inputSorted')).strip('"')

                    insert_transformation_query = """
                    INSERT INTO transformation (
                        id, class, annotations, create_time, create_time_brazil, name, inputSorted, advanced_properties, _groups,
                        session_properties, generate_filename_port, use_labels, use_sequence_fields,
                        fields, data_adapter, content_name, CodOnda, CodProcess, Dt_Inserted, Dt_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        content_name = VALUES(content_name),
                        class = VALUES(class),
                        annotations = VALUES(annotations),
                        create_time = VALUES(create_time),
                        create_time_brazil = VALUES(create_time_brazil),
                        name = VALUES(name),
                        inputSorted = VALUES(inputSorted),
                        advanced_properties = VALUES(advanced_properties),
                        _groups = VALUES(_groups),
                        session_properties = VALUES(session_properties),
                        generate_filename_port = VALUES(generate_filename_port),
                        use_labels = VALUES(use_labels),
                        use_sequence_fields = VALUES(use_sequence_fields),
                        fields = VALUES(fields),
                        data_adapter = VALUES(data_adapter),
                        Dt_updated = VALUES(Dt_updated);
                    """
                    cursor.execute(insert_transformation_query, (
                        transformation_id, transformation_class, annotations, create_time,
                        create_time_brazil, name, input_sorted, advanced_properties, groups,
                        session_properties, generate_filename_port, use_labels, use_sequence_fields,
                        fields, data_adapter, content_name, CodOnda, CodProcess, datetime.datetime.now(), datetime.datetime.now()
                    ))
                except Exception as e:
                    print(f"Erro ao inserir: {name} - Mapa: {content_name}. Detalhes: {e}")

        # Persistir alterações
        conn.commit()
        print("Dados processados com sucesso!")

    except pymysql.MySQLError as e:
        print(f"Erro no MySQL: {e}")
    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados MySQL encerrada.")

if __name__ == "__main__":
    main()