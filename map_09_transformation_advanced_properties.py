import json
import pymysql
import sys
from datetime import datetime
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

# Obter o `date_ins`
date_ins = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# Função principal
def main():
    # Verificar argumentos de linha de comando
    if len(sys.argv) != 2:
        print("Exemplo de Chamada:python map_09_transformation_advanced_properties.py 684.")
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

        # Excluir registros antigos na tabela `transformation_advanced_properties` relacionados ao `CodigoOnda`
        delete_query = """
        DELETE FROM transformation_advanced_properties WHERE CodOnda = %s;
        """
        cursor.execute(delete_query, (CodigoOnda,))

        # Buscar dados da tabela `transformation`
        transformation_query = """
        SELECT id, CodOnda, CodProcess, content_name, advanced_properties, create_time_brazil
        FROM transformation
        WHERE CodOnda = %s AND DATE(dt_updated) = DATE(%s);
        """
        cursor.execute(transformation_query, (CodigoOnda, date_ins))
        transformations = cursor.fetchall()

        print(f"Registros lidos da tabela transformation: {len(transformations)}")

        # Inserir dados na tabela `transformation_advanced_properties`
        for transformation in transformations:
            id_transf, CodOnda, CodProcess, content_name, advanced_properties, create_time_brazil = transformation

            # Verificar se advanced_properties é válido
            if advanced_properties and advanced_properties.strip():
                try:
                    advanced_properties_list = json.loads(advanced_properties)

                    if isinstance(advanced_properties_list, list):
                        for advanced_property in advanced_properties_list:
                            advanced_property_id = advanced_property.get('$$ID')
                            advanced_property_class = advanced_property.get('$$class')
                            name = advanced_property.get('name')
                            value = advanced_property.get('value')

                            try:
                                # Inserir ou atualizar dados na tabela `transformation_advanced_properties`
                                insert_advanced_property_query = """
                                INSERT INTO transformation_advanced_properties (
                                    Id, id_transf, CodOnda, CodProcess, class, name, value, content_name, Dt_Inserted, Dt_updated
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE
                                    class = VALUES(class),
                                    name = VALUES(name),
                                    value = VALUES(value),
                                    content_name = VALUES(content_name),
                                    Dt_updated = VALUES(Dt_updated);
                                """
                                cursor.execute(insert_advanced_property_query, (
                                    advanced_property_id, id_transf, CodOnda, CodProcess, advanced_property_class,
                                    name, value, content_name, create_time_brazil, date_ins
                                ))
                            except pymysql.MySQLError as e:
                                print(f"Erro ao tentar inserir {name}: {e}")
                except json.JSONDecodeError as e:
                    print(f"Erro ao decodificar o JSON para content_name {content_name}: {e}")

        # Confirmar as alterações no banco de dados
        conn.commit()
        print("Processamento concluído com sucesso!")

    except pymysql.MySQLError as e:
        print(f"Erro no MySQL: {e}")
    finally:
        # Fechar conexão com o banco de dados
        if conn:
            conn.close()
            print("Conexão com o banco de dados MySQL encerrada.")

if __name__ == "__main__":
    main()