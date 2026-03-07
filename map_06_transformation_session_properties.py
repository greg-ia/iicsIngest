import json
import pymysql
import sys
import datetime
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

# Função principal
def main():
    # Validar os argumentos de linha de comando
    if len(sys.argv) != 2:
        print("Exemplo de chamada:python map_06_transformation_session_properties.py 684")
        sys.exit(1)

    CodigoOnda = sys.argv[1]

    # Carregar o arquivo de configuração
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E://engenharia//config//config.json')
    config = load_config(config_path)

    # Configurações do MySQL
    mysql_config = {
        'host': os.getenv('MYSQL_HOST', 'localhost'),
        'user': os.getenv('MYSQL_USER', 'seu_usuario'),
        'password': os.getenv('MYSQL_PASSWORD', 'sua_senha'),
        'database': os.getenv('MYSQL_DATABASE', 'nome_do_banco'),
        'autocommit': True
    }

    try:
        # Conectar ao banco de dados MySQL
        conn = pymysql.connect(**mysql_config)
        cursor = conn.cursor()

        # Excluir dados antigos da tabela `transformation_session_properties` para o código da onda
        delete_query = """
        DELETE FROM transformation_session_properties WHERE CodOnda = %s;
        """
        cursor.execute(delete_query, (CodigoOnda,))

        # Buscar dados da tabela `transformation`
        today = datetime.date.today().strftime('%Y-%m-%d')
        transformation_query = """
        SELECT DISTINCT a.id, a.CodOnda, a.CodProcess, a.content_name, a.session_properties, a.create_time_brazil
        FROM transformation a
        WHERE a.CodOnda = %s AND DATE(a.dt_updated) = %s;
        """
        cursor.execute(transformation_query, (CodigoOnda, today))
        transformations = cursor.fetchall()

        print(f"Registros lidos da tabela transformation: {len(transformations)}")

        # Inserir ou atualizar dados na tabela `transformation_session_properties`
        for transformation in transformations:
            id_transf, CodOnda, CodProcess, content_name, session_properties, create_time_brazil = transformation
            #print(id_transf)
            # Verificar se session_properties é válido
            if session_properties and session_properties.strip():
                try:
                    session_properties_list = json.loads(session_properties)

                    #print(session_properties)
                    if isinstance(session_properties_list, list):
                        for session_property in session_properties_list:
                            session_property_id = session_property.get('$$ID')
                            session_property_class = session_property.get('$$class')
                            name = session_property.get('name')
                            value = session_property.get('value')

                            #print(name)
                            try:
                                # Inserir ou atualizar dados na tabela
                                print(f"gravei - codOnda {CodOnda} e codProcesso {CodProcess}")
                                insert_session_property_query = """
                                INSERT INTO transformation_session_properties (
                                    id, id_transf, class, name, value, content_name, CodOnda, CodProcess, Dt_Inserted, Dt_updated
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    content_name = VALUES(content_name),
                                    class = VALUES(class),
                                    value = VALUES(value),
                                    Dt_updated = VALUES(Dt_updated);
                                """
                                cursor.execute(insert_session_property_query, (
                                    session_property_id, id_transf, session_property_class, name, value,
                                    content_name, CodOnda, CodProcess, create_time_brazil, datetime.datetime.now()
                                ))

                            except pymysql.MySQLError as e:
                                print(f"Erro ao tentar inserir {name}: {e}")
                                continue

                except json.JSONDecodeError as e:
                    print(f"Erro ao decodificar JSON para content_name {content_name}: {e}")

        # Confirmar as alterações
        conn.commit()
        print("Processamento concluído com sucesso!")

    except pymysql.MySQLError as e:
        print(f"Erro de MySQL: {e}")
    finally:
        # Fechar a conexão com o banco de dados MySQL
        if conn:
            conn.close()
            print("Conexão com o banco de dados MySQL encerrada.")

if __name__ == "__main__":
    main()