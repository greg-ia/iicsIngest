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

# Obter o date_ins
date_ins = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

# Função principal
def main():
    # Verificar argumentos de linha de comando
    if len(sys.argv) != 2:
        print("Exemplo de Chamada:python map_08_transformation_data_adpter_objects.py 684")
        sys.exit(1)

    CodigoOnda = sys.argv[1]

    # Carregar o arquivo de configuração
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E://engenharia//config//config.json')
    config = load_config(config_path)

    # Configurações para conexão MySQL
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

        # Excluir registros antigos da tabela `transformation_data_adapter_object` relacionados ao `CodigoOnda`
        delete_query = """
        DELETE FROM transformation_data_adapter_object WHERE CodOnda = %s;
        """
        cursor.execute(delete_query, (CodigoOnda,))

        # Ler dados da tabela `transformation_data_adapter`
        transformation_data_adapter_query = """
        SELECT 
            id, id_transf, CodOnda, CodProcess, content_name, object, Dt_Inserted
        FROM 
            transformation_data_adapter
        WHERE 
            CodOnda = %s AND DATE(Dt_updated) = DATE(%s);
        """
        cursor.execute(transformation_data_adapter_query, (CodigoOnda, date_ins))
        data_adapters = cursor.fetchall()

        # Verificar o número de registros lidos
        print(f"Registros lidos da tabela transformation_data_adapter: {len(data_adapters)}")

        # Processar cada registro retornado
        for data_adapter in data_adapters:
            id_data_adapter, id_transf, CodOnda, CodProcess, content_name, object_data, dt_inserted = data_adapter

            # Verificar se o `object_data` é válido
            if object_data and object_data.strip():
                try:
                    object_data_obj = json.loads(object_data)

                    if isinstance(object_data_obj, dict):
                        object_id = object_data_obj.get('$$ID')
                        object_class = object_data_obj.get('$$class')
                        name = object_data_obj.get('name')
                        custom_query = object_data_obj.get('customQuery')
                        label = object_data_obj.get('label')
                        object_name = object_data_obj.get('objectName')
                        object_type = object_data_obj.get('objectType')
                        parent_path = object_data_obj.get('parentPath')
                        path = object_data_obj.get('path')
                        retain_metadata = object_data_obj.get('retainMetadata')
                        fields = json.dumps(object_data_obj.get('fields'))
                        file_attrs = json.dumps(object_data_obj.get('fileAttrs'))

                        # Inserir ou atualizar dados na tabela `transformation_data_adapter_object`
                        insert_object_query = """
                        INSERT INTO transformation_data_adapter_object (
                            id, id_transf, id_data_adapter, class, name, custom_query, label, object_name, object_type, 
                            parent_path, path, retain_metadata, fields, file_attrs, content_name, CodOnda, CodProcess, Dt_Inserted, Dt_updated
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            content_name = VALUES(content_name),
                            class = VALUES(class),
                            name = VALUES(name),
                            custom_query = VALUES(custom_query),
                            label = VALUES(label),
                            object_name = VALUES(object_name),
                            object_type = VALUES(object_type),
                            parent_path = VALUES(parent_path),
                            path = VALUES(path),
                            retain_metadata = VALUES(retain_metadata),
                            fields = VALUES(fields),
                            file_attrs = VALUES(file_attrs),
                            Dt_updated = VALUES(Dt_updated);
                        """
                        cursor.execute(insert_object_query, (
                            object_id, id_transf, id_data_adapter, object_class, name, custom_query, label, object_name, 
                            object_type, parent_path, path, retain_metadata, fields, file_attrs, content_name, 
                            CodOnda, CodProcess, dt_inserted, date_ins
                        ))
                        #print(f"Objeto '{name}' inserido/atualizado com sucesso.")
                    else:
                        print(f"O object_data não é um objeto JSON válido para content_name: {content_name}")

                except json.JSONDecodeError as e:
                    print(f"Erro ao decodificar JSON para content_name {content_name}: {e}")
                    continue

        # Confirmar alterações no banco de dados
        conn.commit()
        print("Processamento concluído com sucesso!")

    except pymysql.MySQLError as e:
        print(f"Erro no MySQL: {e}")
    finally:
        # Fechar a conexão
        if conn:
            conn.close()
            print("Conexão com o banco de dados MySQL encerrada.")

if __name__ == "__main__":
    main()