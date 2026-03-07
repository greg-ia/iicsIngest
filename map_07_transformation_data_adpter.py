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

# Obter DateIns
date_ins = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Função principal
def main():
    # Verificar argumentos de linha de comando
    if len(sys.argv) != 2:
        print("Exemplo de Chamada:python map_07_transformation_data_adpter.py 684")
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

        # Excluir registros antigos na tabela `transformation_data_adapter`
        delete_query = """
        DELETE FROM transformation_data_adapter WHERE CodOnda = %s;
        """
        cursor.execute(delete_query, (CodigoOnda,))

        # Query para buscar dados da tabela `transformation`
        transformation_query = """
        SELECT id, CodOnda, CodProcess, content_name, data_adapter, name, create_time_brazil
        FROM transformation
        WHERE CodOnda = %s AND DATE(dt_updated) = DATE(%s);
        """
        cursor.execute(transformation_query, (CodigoOnda, date_ins))
        transformations = cursor.fetchall()

        print(f"Registros lidos da tabela transformation: {len(transformations)}")

        # Processar cada transação retornada
        for transformation in transformations:
            id_transf, CodOnda, CodProcess, content_name, data_adapter, name, create_time_brazil = transformation

            if not name == 'BI_CTRL_TRANSFORMACAO' and not name[:3].lower() == 'ff_':
                # Adapter Padrão
                Adapter_Padrao = ["agg_", "exp_", "fil_", "jnr_", "rtr_", "seq_", "str_",  "trc_", "uni_", "srt_", "nor_", "tgt_", "src_", "lkp_"]

                if not name[:4].lower() in Adapter_Padrao:
                    try:
                        # Inserir registro na tabela de inconsistência
                        insert_anomaly_query = """
                        INSERT INTO AnomalyInsightTable (
                            CodOnda, InconsistencyCode, Note, ProcessName, ObjectType, CodProcess, DateIns
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
                        cursor.execute(insert_anomaly_query, (
                            CodOnda, 5000, 
                            f"As 3 primeiras posições de {name} devem seguir: src, tgt, agg, exp, fil, jnr, rtr, seq, str, uni, srt, nor, trc",
                            content_name, 'Map', CodProcess, date_ins
                        ))
                    except pymysql.MySQLError as e:
                        print(f"Registro duplicado na tabela de inconsistência. Detalhes: {e}")

            # Processar `data_adapter`
            if data_adapter and data_adapter.strip():
                try:
                    data_adapter_obj = json.loads(data_adapter)

                    if isinstance(data_adapter_obj, dict):
                        data_adapter_id = data_adapter_obj.get('$$ID')
                        data_adapter_class = data_adapter_obj.get('$$class')
                        compatible_engine = data_adapter_obj.get('compatibleEngine')
                        connection_id = data_adapter_obj.get('connectionId')
                        exclude_dynamic_file_name_field = data_adapter_obj.get('excludeDynamicFileNameField')
                        fw_config_id = data_adapter_obj.get('fwConfigId')
                        multiple_object = data_adapter_obj.get('multipleObject')
                        object_type = data_adapter_obj.get('objectType')
                        type_system = data_adapter_obj.get('typeSystem')
                        use_dynamic_file_name = data_adapter_obj.get('useDynamicFileName')
                        object_data = json.dumps(data_adapter_obj.get('object'))
                        opr_runtime_attributes = json.dumps(data_adapter_obj.get('oprRuntimeAttributes'))
                        read_options = json.dumps(data_adapter_obj.get('readOptions'))
                        runtime_attributes = json.dumps(data_adapter_obj.get('runtimeAttributes'))

                        try:
                            # Inserir ou atualizar dados na tabela `transformation_data_adapter`
                            insert_data_adapter_query = """
                            INSERT INTO transformation_data_adapter (
                                id, id_transf, _class, compatible_engine, connection_id, exclude_dynamic_file_name_field,
                                fw_config_id, multiple_object, object_type, type_system, use_dynamic_file_name,
                                object, opr_runtime_attributes, read_options, runtime_attributes, content_name, CodOnda, CodProcess, Dt_Inserted, Dt_updated
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON DUPLICATE KEY UPDATE
                                content_name = VALUES(content_name),
                                _class = VALUES(_class),
                                compatible_engine = VALUES(compatible_engine),
                                connection_id = VALUES(connection_id),
                                exclude_dynamic_file_name_field = VALUES(exclude_dynamic_file_name_field),
                                fw_config_id = VALUES(fw_config_id),
                                multiple_object = VALUES(multiple_object),
                                object_type = VALUES(object_type),
                                type_system = VALUES(type_system),
                                use_dynamic_file_name = VALUES(use_dynamic_file_name),
                                object = VALUES(object),
                                opr_runtime_attributes = VALUES(opr_runtime_attributes),
                                read_options = VALUES(read_options),
                                runtime_attributes = VALUES(runtime_attributes),
                                Dt_updated = VALUES(Dt_updated);
                            """
                            cursor.execute(insert_data_adapter_query, (
                                data_adapter_id, id_transf, data_adapter_class, compatible_engine, connection_id,
                                exclude_dynamic_file_name_field, fw_config_id, multiple_object, object_type,
                                type_system, use_dynamic_file_name, object_data, opr_runtime_attributes,
                                read_options, runtime_attributes, content_name, CodOnda, CodProcess, create_time_brazil, date_ins
                            ))
                        except pymysql.MySQLError as e:
                            print(f"Erro ao tentar inserir {name}: {e}")

                except json.JSONDecodeError as e:
                    print(f"Erro ao decodificar JSON para content_name {content_name}: {e}")

        # Confirmar alterações no banco de dados
        conn.commit()
        print("Processamento concluído com sucesso!")

    except pymysql.MySQLError as e:
        print(f"Erro no MySQL: {e}")
    finally:
        # Fechar a conexão com o banco
        if conn:
            conn.close()
            print("Conexão com o banco de dados MySQL encerrada.")

if __name__ == "__main__":
    main()