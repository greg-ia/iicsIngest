#!/usr/bin/env python3
"""
Script para processar objetos (objects) dos data_adapters
Carrega dados na tabela fingerhard.transformation_data_adapter_object
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import pymysql
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()


class ConfigLoader:
    """Gerenciador de configurações"""
    
    @staticmethod
    def load_config() -> Dict:
        """
        Carrega o arquivo de configuração JSON
        
        Returns:
            Dict com as configurações
        """
        config_path = os.getenv('CONFIG_PATH_ENGENHARIA')
        
        if not config_path:
            config_path = os.getenv('CONFIG_PATH')
        
        if not config_path:
            print("❌ Error: Variável de configuração não definida no .env")
            print("   Configure uma das opções:")
            print("     CONFIG_PATH_ENGENHARIA=/caminho/para/config.json")
            print("     CONFIG_PATH=/caminho/para/config.json")
            sys.exit(1)
        
        # Normalizar o caminho
        config_path = str(config_path).replace('\\', '/').rstrip('/')
        
        # Se for um diretório, procura por arquivos de configuração
        if os.path.isdir(config_path):
            possible_files = ['config.json', 'configuracoes.json', 'settings.json']
            found = False
            
            for filename in possible_files:
                test_path = os.path.join(config_path, filename)
                if os.path.isfile(test_path):
                    config_path = test_path
                    found = True
                    print(f"✓ Arquivo de configuração encontrado: {config_path}")
                    break
            
            if not found:
                print(f"❌ Error: CONFIG_PATH aponta para um diretório ({config_path})")
                print(f"       Mas nenhum dos arquivos foi encontrado: {', '.join(possible_files)}")
                sys.exit(1)
        
        # Verifica se o arquivo existe
        if not os.path.exists(config_path):
            print(f"❌ Error: Arquivo de configuração não encontrado: {config_path}")
            sys.exit(1)
        
        # Verifica permissão de leitura
        if not os.access(config_path, os.R_OK):
            print(f"❌ Error: Sem permissão de leitura para o arquivo: {config_path}")
            sys.exit(1)
        
        # Tenta ler o arquivo
        try:
            with open(config_path, 'r', encoding='utf-8') as config_file:
                config = json.load(config_file)
            print(f"✓ Configurações carregadas com sucesso de: {config_path}")
            return config
        except json.JSONDecodeError as e:
            print(f"❌ Error: Configuração JSON inválida. Detalhes: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error: Não foi possível ler o arquivo de configuração: {e}")
            sys.exit(1)


class DatabaseManager:
    """Gerenciador de operações com banco de dados"""
    
    def __init__(self):
        """Inicializa o gerenciador com configurações do MySQL"""
        self.config = {
            'host': os.getenv('MYSQL_RG_HOST'),
            'user': os.getenv('MYSQL_RG_USER'),
            'password': os.getenv('MYSQL_RG_PASSWORD'),
            'database': os.getenv('MYSQL_RG_DATABASE'),
            'autocommit': True,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        self.conn = None
        self.date_ins = datetime.now()
        
        # Validar credenciais
        missing = [k for k, v in self.config.items() 
                   if not v and k not in ['autocommit', 'charset', 'cursorclass']]
        if missing:
            print(f"❌ Erro: Variáveis de ambiente MySQL obrigatórias faltando: {missing}")
            print("   Configure no arquivo .env:")
            print("     MYSQL_RG_HOST=localhost")
            print("     MYSQL_RG_USER=seu_usuario")
            print("     MYSQL_RG_PASSWORD=sua_senha")
            print("     MYSQL_RG_DATABASE=ruleguardian")
            sys.exit(1)
    
    def connect(self) -> bool:
        """
        Estabelece conexão com o MySQL
        
        Returns:
            bool: True se conectou com sucesso
        """
        try:
            self.conn = pymysql.connect(**self.config)
            print("✓ Conexão com MySQL estabelecida com sucesso.")
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao conectar ao MySQL: {e}")
            return False
    
    def get_cursor(self):
        """Retorna um cursor para execução de queries"""
        return self.conn.cursor()
    
    def commit(self):
        """Confirma as alterações no banco"""
        if self.conn:
            self.conn.commit()
    
    def close(self):
        """Fecha a conexão com o banco"""
        if self.conn:
            self.conn.close()
            print("✓ Conexão com o banco de dados MySQL fechada.")


class DataAdapterObjectProcessor:
    """Processador de objetos (objects) dos data_adapters"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Inicializa o processador
        
        Args:
            db_manager: Instância do gerenciador de banco de dados
        """
        self.db_manager = db_manager
    
    def delete_old_records(self, codigo_onda: str) -> bool:
        """
        Remove registros antigos da tabela para o código da onda
        
        Args:
            codigo_onda: Código da onda
            
        Returns:
            bool: True se removeu com sucesso
        """
        query = "DELETE FROM fingerhard.transformation_data_adapter_object WHERE CodOnda = %s"
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (codigo_onda,))
                rows_deleted = cursor.rowcount
            print(f"✓ Registros antigos removidos: {rows_deleted} registros")
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao remover registros antigos: {e}")
            return False
    
    def get_data_adapters(self, codigo_onda: str) -> List[Dict]:
        """
        Busca data_adapters da tabela principal
        
        Args:
            codigo_onda: Código da onda
            
        Returns:
            Lista de dicionários com os data_adapters
        """
        query = """
            SELECT 
                id, id_transf, CodOnda, CodProcess, content_name, object, Dt_Inserted
            FROM 
                fingerhard.transformation_data_adapter
            WHERE 
                CodOnda = %s 
                AND DATE(Dt_updated) = DATE(%s)
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (codigo_onda, self.db_manager.date_ins))
                data_adapters = cursor.fetchall()
            print(f"✓ Data adapters encontrados: {len(data_adapters)} registro(s)")
            return data_adapters
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao buscar data_adapters: {e}")
            return []
    
    def parse_object(self, object_data_json: str, content_name: str) -> Optional[Dict]:
        """
        Parse do JSON do objeto
        
        Args:
            object_data_json: String JSON do objeto
            content_name: Nome do conteúdo para logging
            
        Returns:
            Dict com dados do objeto ou None em caso de erro
        """
        if not object_data_json or not object_data_json.strip():
            return None
        
        try:
            object_data = json.loads(object_data_json)
            
            # Se for uma lista, pega o primeiro elemento
            if isinstance(object_data, list):
                object_data = object_data[0] if object_data else None
            
            if not isinstance(object_data, dict):
                print(f"⚠️  Formato inesperado de objeto para {content_name}")
                return None
            
            return object_data
        except json.JSONDecodeError as e:
            print(f"❌ Erro ao decodificar JSON para {content_name}: {e}")
            return None
    
    def convert_retain_metadata(self, value: Any) -> str:
        """
        Converte o valor de retain_metadata para o formato ENUM esperado pela tabela
        
        A coluna retain_metadata é ENUM('true','false')
        
        Args:
            value: Valor original (pode ser bool, string, int, etc)
            
        Returns:
            str: 'true' ou 'false'
        """
        if value is None:
            return 'false'
        
        # Se for booleano
        if isinstance(value, bool):
            return 'true' if value else 'false'
        
        # Se for string
        if isinstance(value, str):
            value_lower = value.lower().strip()
            if value_lower in ['true', '1', 'yes', 'on']:
                return 'true'
            return 'false'
        
        # Se for número
        if isinstance(value, (int, float)):
            return 'true' if value else 'false'
        
        # Qualquer outro caso
        return 'false'
    
    def convert_to_json_string(self, value: Any, default: str = '{}') -> str:
        """
        Converte um valor para string JSON
        
        Args:
            value: Valor a ser convertido
            default: Valor padrão se não for possível converter
            
        Returns:
            str: String JSON
        """
        if value is None:
            return default
        
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        
        if isinstance(value, str):
            # Se já for uma string JSON, mantém
            if value.strip().startswith(('{', '[')):
                return value
            return json.dumps(value)
        
        return str(value)
    
    def insert_object(self, object_data: Dict) -> bool:
        """
        Insere ou atualiza um objeto na tabela
        
        Args:
            object_data: Dicionário com os dados do objeto
            
        Returns:
            bool: True se inseriu/atualizou com sucesso
        """
        # Ajustar a ordem dos campos conforme a estrutura da tabela
        query = """
            INSERT INTO fingerhard.transformation_data_adapter_object (
                id, id_transf, id_data_adapter, CodOnda, CodProcess, class, name, 
                custom_query, label, object_name, object_type, parent_path, path, 
                retain_metadata, fields, file_attrs, content_name, Dt_Inserted, Dt_updated
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
                Dt_updated = VALUES(Dt_updated)
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (
                    object_data['id'],                    # id
                    object_data['id_transf'],             # id_transf
                    object_data['id_data_adapter'],       # id_data_adapter
                    object_data['cod_onda'],              # CodOnda
                    object_data['cod_process'],           # CodProcess
                    object_data['class'],                 # class
                    object_data['name'],                  # name
                    object_data['custom_query'],          # custom_query
                    object_data['label'],                 # label
                    object_data['object_name'],           # object_name
                    object_data['object_type'],           # object_type
                    object_data['parent_path'],           # parent_path
                    object_data['path'],                  # path
                    object_data['retain_metadata'],       # retain_metadata (ENUM)
                    object_data['fields'],                # fields
                    object_data['file_attrs'],            # file_attrs
                    object_data['content_name'],          # content_name
                    object_data['dt_inserted'],           # Dt_Inserted
                    object_data['dt_updated']             # Dt_updated
                ))
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao inserir objeto '{object_data['name']}': {e}")
            return False
    
    def process_data_adapter(self, data_adapter: Dict) -> bool:
        """
        Processa um data_adapter, extraindo e inserindo seu objeto
        
        Args:
            data_adapter: Dicionário com dados do data_adapter
            
        Returns:
            bool: True se processou com sucesso
        """
        id_data_adapter = data_adapter['id']
        id_transf = data_adapter['id_transf']
        cod_onda = data_adapter['CodOnda']
        cod_process = data_adapter['CodProcess']
        content_name = data_adapter['content_name']
        object_data_json = data_adapter['object']
        dt_inserted = data_adapter['Dt_Inserted']
        
        # Parse do objeto
        object_obj = self.parse_object(object_data_json, content_name)
        
        if not object_obj:
            return False
        
        # Extrair campos do objeto
        object_id = object_obj.get('$$ID')
        if not object_id:
            print(f"⚠️  Objeto sem $$ID no data_adapter {id_data_adapter}")
            return False
        
        # Converter class para inteiro (se necessário)
        object_class = object_obj.get('$$class', 0)
        if isinstance(object_class, str):
            try:
                object_class = int(object_class) if object_class.isdigit() else 0
            except (ValueError, TypeError):
                object_class = 0
        
        # Converter retain_metadata para ENUM('true','false')
        retain_metadata_raw = object_obj.get('retainMetadata', False)
        retain_metadata = self.convert_retain_metadata(retain_metadata_raw)
        
        # Converter fields e file_attrs para JSON string
        fields = object_obj.get('fields', [])
        fields_json = self.convert_to_json_string(fields, '[]')
        
        file_attrs = object_obj.get('fileAttrs', {})
        file_attrs_json = self.convert_to_json_string(file_attrs, '{}')
        
        # Preparar dados para inserção
        object_record = {
            'id': str(object_id),  # Converter para string para evitar problemas
            'id_transf': id_transf,
            'id_data_adapter': id_data_adapter,
            'cod_onda': cod_onda,
            'cod_process': cod_process,
            'class': object_class,
            'name': object_obj.get('name', ''),
            'custom_query': object_obj.get('customQuery'),
            'label': object_obj.get('label'),
            'object_name': object_obj.get('objectName'),
            'object_type': object_obj.get('objectType'),
            'parent_path': object_obj.get('parentPath'),
            'path': object_obj.get('path'),
            'retain_metadata': retain_metadata,  # Agora é 'true' ou 'false'
            'fields': fields_json,
            'file_attrs': file_attrs_json,
            'content_name': content_name,
            'dt_inserted': dt_inserted if dt_inserted else self.db_manager.date_ins,
            'dt_updated': self.db_manager.date_ins
        }
        
        # Log para debug
        # print(f"   Processando objeto: {object_record['name']}")
        # print(f"      ID: {object_record['id']}")
        # print(f"      retain_metadata: {object_record['retain_metadata']}")
        
        return self.insert_object(object_record)
    
    def process(self, codigo_onda: str) -> bool:
        """
        Executa o processamento completo para uma onda
        
        Args:
            codigo_onda: Código da onda
            
        Returns:
            bool: True se processou com sucesso
        """
        print(f"\n📌 Processando onda: {codigo_onda}")
        print("-" * 60)
        
        # Remover registros antigos
        if not self.delete_old_records(codigo_onda):
            print("❌ Erro ao remover registros antigos. Abortando...")
            return False
        
        # Buscar data_adapters
        data_adapters = self.get_data_adapters(codigo_onda)
        
        if not data_adapters:
            print("⚠️  Nenhum data_adapter encontrado para processar")
            return True
        
        # Processar cada data_adapter
        total = len(data_adapters)
        processados = 0
        objetos_processados = 0
        
        print("\n📦 Processando data_adapters...")
        for idx, data_adapter in enumerate(data_adapters, 1):
            print(f"\n   [{idx}/{total}] Processando data_adapter ID: {data_adapter['id']}")
            if self.process_data_adapter(data_adapter):
                processados += 1
                objetos_processados += 1
        
        print("\n" + "-" * 60)
        print(f"📊 Resumo do processamento:")
        print(f"   Total de data_adapters processados: {total}")
        print(f"   Data_adapters com objetos processados: {processados}")
        print(f"   Total de objetos inseridos/atualizados: {objetos_processados}")
        
        return processados > 0 or total == 0


class DataAdapterObjectLoader:
    """Classe principal para carga de objetos dos data_adapters"""
    
    def __init__(self):
        self.config = None
        self.db_manager = None
        self.processor = None
    
    def load_configuration(self):
        """Carrega as configurações do sistema"""
        self.config = ConfigLoader.load_config()
        # Este script não usa configurações específicas, apenas o banco
    
    def setup_database(self):
        """Configura a conexão com o banco de dados"""
        self.db_manager = DatabaseManager()
        if not self.db_manager.connect():
            sys.exit(1)
        
        self.processor = DataAdapterObjectProcessor(self.db_manager)
    
    def run(self):
        """Executa o fluxo principal de processamento"""
        print("\n" + "=" * 60)
        print("🚀 DATA ADAPTER OBJECT LOADER - IICS")
        print("=" * 60)
        print("\n📌 Processando objetos para tabela fingerhard.transformation_data_adapter_object")
        
        # Validar argumentos
        if len(sys.argv) != 2:
            print("\n❌ Uso correto:")
            print(f"  python {sys.argv[0]} <codigo_onda>")
            print("\nExemplo:")
            print(f"  python {sys.argv[0]} 653")
            print(f"  python {sys.argv[0]} 684")
            print("=" * 60)
            sys.exit(1)
        
        codigo_onda = sys.argv[1]
        
        # Validar se é número
        if not codigo_onda.isdigit():
            print(f"\n❌ Error: codigo_onda deve ser um número inteiro")
            print(f"   Recebido: '{codigo_onda}'")
            sys.exit(1)
        
        # Carregar configurações
        self.load_configuration()
        
        # Configurar banco de dados
        self.setup_database()
        
        try:
            # Processar objetos
            success = self.processor.process(codigo_onda)
            
            # Commit final
            self.db_manager.commit()
            
            print("\n" + "=" * 60)
            if success:
                print("✨ Processamento concluído com SUCESSO!")
            else:
                print("⚠️  Processamento concluído com falhas!")
                sys.exit(1)
            print("=" * 60)
            
        except Exception as e:
            print(f"\n❌ Erro inesperado: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            self.db_manager.close()


def main():
    """Função principal"""
    loader = DataAdapterObjectLoader()
    loader.run()


if __name__ == "__main__":
    main()