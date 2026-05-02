import json
import datetime
import sys
import os
import re
import pymysql
from dotenv import load_dotenv

# ==================== CONFIGURAÇÃO INICIAL ====================
load_dotenv()

config_path_env = os.getenv('CONFIG_PATH', '')

# Se a variável já aponta para um arquivo .json, usa direto; senão, assume que é diretório e monta o caminho
if config_path_env.endswith('.json'):
    config_file_path = config_path_env
else:
    config_file_path = os.path.join(config_path_env, 'config.json')

class CustomLogger:
    def __init__(self):
        self.level_map = {
            'INFO': '[INFO]',
            'ERROR': '[ERROR]',
            'WARNING': '[WARNING]',
            'DEBUG': '[DEBUG]'
        }
    def log(self, msg, level='INFO'):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        prefix = self.level_map.get(level.upper(), '[INFO]')
        print(f"{timestamp} {prefix} {msg}")

logger = CustomLogger()

# ==================== IMPORTAÇÃO DA gregLib ====================
ENV = os.getenv('ENV', 'DEV')

if ENV == 'DEV':
    greg_path = 'D:/opt/engenharia/config/gregLib'
else:
    greg_path = '/opt/engenharia/config/gregLib'

if greg_path not in sys.path:
    sys.path.insert(0, greg_path)

try:
    from gregLib import validar_nmeProjeto, carregar_configuracao
    logger.log("gregLib importada com sucesso", "INFO")
except ImportError as e:
    logger.log(f"Erro ao importar gregLib: {e}", "ERROR")
    sys.exit(1)

def extract_cod_projeto_processo(object_name):
    """
    Extrai primeiro bloco numérico como cod_projeto,
    segundo bloco numérico como cod_processo.
    Retorna (cod_projeto, cod_processo) ou (None, None)
    """
    numbers = re.findall(r'\d+', object_name)
    cod_projeto = numbers[0] if len(numbers) >= 1 else None
    cod_processo = numbers[1] if len(numbers) >= 2 else None
    return cod_projeto, cod_processo

# ==================== MAIN ====================
if __name__ == "__main__":
    logger.log(f"Ambiente: {ENV}", "INFO")

    if len(sys.argv) != 2:
        logger.log("Uso: python iics_exported_objects.py <cod_projeto>", "ERROR")
        sys.exit(1)

    cod_projeto = sys.argv[1]

    # 1. Obter nome do projeto via gregLib
    try:
        sucesso, nme_projeto = validar_nmeProjeto(cod_projeto, logger)
        if not sucesso:
            logger.log(f"Projeto não encontrado para código {cod_projeto}", "ERROR")
            sys.exit(1)
        logger.log(f"Projeto: {nme_projeto} (código {cod_projeto})", "INFO")
    except Exception as e:
        logger.log(f"Falha ao validar nome do projeto: {e}", "ERROR")
        sys.exit(1)

    # 2. Configuração do MySQL
    mysql_config = {
        'host': os.getenv('MYSQL_RG_HOST'),
        'user': os.getenv('MYSQL_RG_USER'),
        'password': os.getenv('MYSQL_RG_PASSWORD'),
        'database': os.getenv('MYSQL_RG_DATABASE'),
        'port': int(os.getenv('MYSQL_RG_PORT', 3306)),
        'autocommit': True,
        'charset': 'utf8mb4',
        'collation': 'utf8mb4_general_ci'
    }

    if not all([mysql_config['host'], mysql_config['user'], mysql_config['password'], mysql_config['database']]):
        logger.log("Variáveis MYSQL_RG_* não definidas no .env", "ERROR")
        sys.exit(1)

    # 3. Caminho do JSON via leitura direta do config.json
    config_file_path = os.getenv('CONFIG_PATH', '')
    if config_file_path.endswith('.json'):
        config_file_path = config_file_path  # já é o arquivo
    else:
        config_file_path = os.path.join(config_file_path, 'config.json')

    try:
        with open(config_file_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.log(f"Configuração carregada de {config_file_path}", "INFO")
    except Exception as e:
        logger.log(f"Erro ao ler configuração: {e}", "ERROR")
        sys.exit(1)

    json_file_path = config.get("json_file_path")
    if not json_file_path:
        logger.log("json_file_path não encontrado no config.json", "ERROR")
        sys.exit(1)

    # Se o caminho for relativo, torna absoluto com base no diretório do config.json
    if not os.path.isabs(json_file_path):
        base_dir = os.path.dirname(config_file_path)
        json_file_path = os.path.join(base_dir, json_file_path)

    caminho_json = os.path.join(json_file_path, f"{cod_projeto}_{nme_projeto}", "exportMetadata.v2.json")
    logger.log(f"JSON: {caminho_json}", "INFO")

    # 4. Carregar JSON
    try:
        with open(caminho_json, 'r', encoding='utf-8') as f:
            dados_json = json.load(f)
    except Exception as e:
        logger.log(f"Erro ao ler JSON: {e}", "ERROR")
        sys.exit(1)

    objetos = dados_json.get('exportedObjects', [])
    logger.log(f"Total de objetos no JSON: {len(objetos)}", "INFO")

    # 5. Conectar ao MySQL e truncar tabela
    conn = None
    try:
        conn = pymysql.connect(**mysql_config)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM fingerhard.exported_objects where cod_projeto = %s", (cod_projeto,))
        logger.log(f"Tabela fingerhard.exported_objects truncada para projeto {cod_projeto}", "INFO")
        conn.commit()

        logger.log("Tabela fingerhard.exported_objects truncada", "INFO")

        insert_sql = """
            INSERT INTO fingerhard.exported_objects
            (object_guid, object_name, object_type, path,
             description, content_type, document_state,
             additional_info_json, cod_projeto, cod_processo, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for obj in objetos:
            # Campos principais
            object_guid = obj.get('objectGuid')
            object_name = obj.get('objectName')
            object_type = obj.get('objectType')
            path = obj.get('path')

            # Extrai cod_projeto e cod_processo do object_name
            extracted_cod_projeto, extracted_cod_processo = extract_cod_projeto_processo(object_name)
            # Usa o código passado como parâmetro como fallback para cod_projeto
            final_cod_projeto = extracted_cod_projeto if extracted_cod_projeto else cod_projeto
            final_cod_processo = extracted_cod_processo

            # Campos do nó additionalInfo
            additional_info = obj.get('metadata', {}).get('additionalInfo', {})
            description = additional_info.get('description')
            content_type = additional_info.get('contentType')
            document_state = additional_info.get('documentState')
            # Guarda o nó inteiro como JSON string
            additional_info_json = json.dumps(additional_info, ensure_ascii=False)

            created_at = datetime.datetime.now()
            
            if cod_projeto == final_cod_projeto:
                cursor.execute(insert_sql, (
                    object_guid, object_name, object_type, path,
                    description, content_type, document_state,
                    additional_info_json, final_cod_projeto, final_cod_processo, created_at
                ))

        conn.commit()
        logger.log(f"{len(objetos)} registros inseridos com sucesso", "INFO")

    except Exception as e:
        logger.log(f"Erro no processamento: {e}", "ERROR")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()