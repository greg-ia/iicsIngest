import json
import pymysql
import datetime
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
        print(f"Erro: Configuração JSON inválida. Detalhes: {e}")
        sys.exit(1)

# Função principal
def main():
    # Carregar o arquivo de configuração
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E:\engenharia\config\config.json')
    config = load_config(config_path)

    try:
        connection_params = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'user': os.getenv('MYSQL_USER', 'seu_usuario'),
            'password': os.getenv('MYSQL_PASSWORD', 'sua_senha'),
            'database': os.getenv('MYSQL_DATABASE', 'controlequalidade'),
            'autocommit': False
        }
    except KeyError as e:
        print(f"Erro: Chave ausente no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)

    try:
        # Conexão com MySQL
        conn = pymysql.connect(**connection_params)
        cursor = conn.cursor()

        # Query para buscar tasks
        query_tasks = """
        SELECT 
            a.CodOnda, a.CodProcess, a.name, a.sessionPropertiesList 
        FROM 
            s_task a
        LEFT JOIN 
            s_task b 
        ON 
            a.name = b.name
        WHERE 
            a.name IN (
                SELECT 
                    name 
                FROM 
                    s_task 
                WHERE 
                    DATE(Dt_Updated) = (
                        SELECT 
                            MAX(DATE(Dt_Updated)) 
                        FROM 
                            s_task
                    )
            );
        """
        cursor.execute(query_tasks)
        tasks = cursor.fetchall()

        print(f"Registros lidos da tabela s_task: {len(tasks)}")

        # Processar cada task e inserir na tabela `s_task_sessionPropertiesList`
        date_ins = datetime.datetime.now()
        for s_task in tasks:
            CodOnda, CodProcess, task_name, session_properties_list = s_task

            try:
                session_properties_list = json.loads(session_properties_list)
            except json.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON para task_name {task_name}: {e}")
                continue

            for prop in session_properties_list:
                try:
                    name = prop.get('name', '')
                    value = prop.get('value', '')
                    recommended = int(bool(prop.get('recommended', False)))  # Convertendo para 0 ou 1

                    # Query de inserção ou atualização
                    insert_query = """
                    INSERT INTO s_task_sessionPropertiesList (
                        CodOnda, CodProcess, task_name, name, value, recommended, Dt_Inserted, Dt_Updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        task_name = VALUES(task_name),
                        value = VALUES(value),
                        recommended = VALUES(recommended),
                        Dt_Updated = VALUES(Dt_Updated);
                    """

                    # Executar a query
                    cursor.execute(insert_query, (CodOnda, CodProcess, task_name, name, value, recommended, date_ins, date_ins))

                except pymysql.MySQLError as e:
                    print(f"Erro ao inserir propriedade '{name}' na task '{task_name}': {e}")
                    conn.rollback()  # Reverter alterações em caso de erro
                    sys.exit(1)

        conn.commit()  # Confirmar as alterações
        print("Inserções concluídas com sucesso!")

    except pymysql.MySQLError as e:
        print(f"Erro ao conectar ao banco de dados MySQL: {e}")
        sys.exit(1)

    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados MySQL encerrada.")

if __name__ == "__main__":
    main()