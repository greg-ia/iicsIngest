import json
import pymysql
import datetime
import sys
import os

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
    
def main():
    # Carregar o arquivo de configuração
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E://engenharia//config//config.json')
    config = load_config(config_path)
    
    try:
        connection_params = {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'user': os.getenv('MYSQL_USER', 'seu_usuario'),
            'password': os.getenv('MYSQL_PASSWORD', 'sua_senha'),
            'database': os.getenv('MYSQL_DATABASE', 'controlequalidade'),
            'autocommit': False  # Usar controle de transações
        }
    except KeyError as e:
        print(f"Erro: Chave ausente no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)
    
    try:
        # Conectar ao banco de dados MySQL
        conn = pymysql.connect(**connection_params)
        cursor = conn.cursor()

        # Consultar tasks
        query_tasks = """
        SELECT 
            a.CodOnda, a.CodProcess, a.name, a.parameters 
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
                    DATE(Dt_updated) = (
                        SELECT 
                            MAX(DATE(Dt_updated)) 
                        FROM 
                            s_task
                    )
            );
        """
        cursor.execute(query_tasks)
        tasks = cursor.fetchall()

        print(f"Registros lidos da tabela s_task: {len(tasks)}")

        # Processar cada task e parâmetros
        date_ins = datetime.datetime.now()
        for s_task in tasks:
            CodOnda, CodProcess, task_name, parameters_list = s_task

            try:
                parameters_list = json.loads(parameters_list)
            except json.JSONDecodeError as e:
                print(f"Erro ao decodificar JSON para task_name {task_name}: {e}")
                continue
            
            for param in parameters_list:
                try:
                    # Extração dos dados do parâmetro
                    id = param.get('id', '')
                    name = param.get('name', '')
                    type = param.get('type', '')
                    label = param.get('label', '')
                    uiProperties = json.dumps(param.get('uiProperties', {}))
                    sourceConnectionId = param.get('sourceConnectionId', '')
                    newFlatFile = int(param.get('newFlatFile', False))  # Converter para inteiro (0 ou 1)
                    newObject = int(param.get('newObject', False))
                    showBusinessNames = int(param.get('showBusinessNames', False))
                    naturalOrder = int(param.get('naturalOrder', False))
                    truncateTarget = int(param.get('truncateTarget', False))
                    bulkApiDBTarget = int(param.get('bulkApiDBTarget', False))
                    srcFFAttrs = json.dumps(param.get('srcFFAttrs', {}))
                    customFuncCfg = json.dumps(param.get('customFuncCfg', {}))
                    targetRefsV2 = json.dumps(param.get('targetRefsV2', {}))
                    targetUpdateColumns = json.dumps(param.get('targetUpdateColumns', []))
                    extendedObject = json.dumps(param.get('extendedObject', {}))
                    runtimeAttrs = json.dumps(param.get('runtimeAttrs', {}))
                    isRESTModernSource = int(param.get('isRESTModernSource', False))
                    isFileList = int(param.get('isFileList', False))
                    handleSpecialChars = int(param.get('handleSpecialChars', False))
                    handleDecimalRoundOff = int(param.get('handleDecimalRoundOff', False))
                    frsAsset = int(param.get('frsAsset', False))
                    dynamicFileName = int(param.get('dynamicFileName', False))
                    excludeDynamicFileNameField = int(param.get('excludeDynamicFileNameField', False))
                    currentlyProcessedFileName = int(param.get('currentlyProcessedFileName', False))
                    retainFieldMetadata = int(param.get('retainFieldMetadata', False))
                    useExactSrcNames = int(param.get('useExactSrcNames', False))
                    tgtObjectAttributes = json.dumps(param.get('tgtObjectAttributes', {}))
                    runtimeParameterData = json.dumps(param.get('runtimeParameterData', {}))
                    overridableProperties = json.dumps(param.get('overridableProperties', []))
                    overriddenFields = json.dumps(param.get('overriddenFields', []))
                    
                    # Query de inserção/atualização
                    insert_query = """
                    INSERT INTO s_task_parameters (
                        CodOnda, CodProcess, task_name, id, name, type, label,
                        uiProperties, sourceConnectionId, newFlatFile, newObject,
                        showBusinessNames, naturalOrder, truncateTarget, bulkApiDBTarget,
                        srcFFAttrs, customFuncCfg, targetRefsV2, targetUpdateColumns,
                        extendedObject, runtimeAttrs, isRESTModernSource, isFileList,
                        handleSpecialChars, handleDecimalRoundOff, frsAsset, dynamicFileName,
                        excludeDynamicFileNameField, currentlyProcessedFileName, retainFieldMetadata,
                        useExactSrcNames, tgtObjectAttributes, runtimeParameterData,
                        overridableProperties, overriddenFields, Dt_Inserted, Dt_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        type = VALUES(type),
                        label = VALUES(label),
                        uiProperties = VALUES(uiProperties),
                        sourceConnectionId = VALUES(sourceConnectionId),
                        newFlatFile = VALUES(newFlatFile),
                        newObject = VALUES(newObject),
                        showBusinessNames = VALUES(showBusinessNames),
                        naturalOrder = VALUES(naturalOrder),
                        truncateTarget = VALUES(truncateTarget),
                        bulkApiDBTarget = VALUES(bulkApiDBTarget),
                        srcFFAttrs = VALUES(srcFFAttrs),
                        customFuncCfg = VALUES(customFuncCfg),
                        targetRefsV2 = VALUES(targetRefsV2),
                        targetUpdateColumns = VALUES(targetUpdateColumns),
                        extendedObject = VALUES(extendedObject),
                        runtimeAttrs = VALUES(runtimeAttrs),
                        isRESTModernSource = VALUES(isRESTModernSource),
                        isFileList = VALUES(isFileList),
                        Dt_updated = VALUES(Dt_updated);
                    """
                    # Executar a query
                    cursor.execute(insert_query, (
                        CodOnda, CodProcess, task_name, id, name, type, label,
                        uiProperties, sourceConnectionId, newFlatFile, newObject,
                        showBusinessNames, naturalOrder, truncateTarget, bulkApiDBTarget,
                        srcFFAttrs, customFuncCfg, targetRefsV2, targetUpdateColumns,
                        extendedObject, runtimeAttrs, isRESTModernSource, isFileList,
                        handleSpecialChars, handleDecimalRoundOff, frsAsset, dynamicFileName,
                        excludeDynamicFileNameField, currentlyProcessedFileName, retainFieldMetadata,
                        useExactSrcNames, tgtObjectAttributes, runtimeParameterData,
                        overridableProperties, overriddenFields, date_ins, date_ins
                    ))
                except pymysql.MySQLError as e:
                    print(f"Erro ao inserir parâmetro '{name}' para task '{task_name}': {e}")
                    conn.rollback()
                    sys.exit(1)

        conn.commit()  # Confirmar transações
        print("Inserções realizadas com sucesso!")

    except pymysql.MySQLError as e:
        print(f"Erro ao conectar ao banco de dados MySQL: {e}")
        sys.exit(1)

    finally:
        if conn:
            conn.close()
            print("Conexão com o banco de dados MySQL encerrada.")

if __name__ == "__main__":
    main()