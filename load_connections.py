#!/usr/bin/env python3
"""
Script para processar arquivos JSON de conexões do IICS
Carrega dados na tabela fingerhard.connections
"""

import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

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
            'autocommit': False,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        self.conn = None
        
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
    
    def rollback(self):
        """Desfaz as alterações no banco"""
        if self.conn:
            self.conn.rollback()
    
    def close(self):
        """Fecha a conexão com o banco"""
        if self.conn:
            self.conn.close()
            print("✓ Conexão com o banco de dados MySQL fechada.")


class TimeConverter:
    """Conversor de timezone"""
    
    @staticmethod
    def convert_to_brazil_time(utc_time_str: str) -> Optional[str]:
        """
        Converte data UTC para timezone do Brasil (UTC-3)
        
        Args:
            utc_time_str: String do timestamp UTC (formato ISO)
            
        Returns:
            Timestamp no horário brasileiro ou None em caso de erro
        """
        if not utc_time_str:
            return None
        
        try:
            # Converte string para datetime
            time_str = utc_time_str.replace("Z", "+00:00")
            utc_time = datetime.fromisoformat(time_str)
            # Ajustar para o fuso horário GMT-3
            brazil_time = utc_time - timedelta(hours=3)
            return brazil_time.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError) as e:
            print(f"⚠️  Erro ao converter horário UTC '{utc_time_str}': {e}")
            return None


class ConnectionProcessor:
    """Processador de arquivos JSON de conexões"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Inicializa o processador
        
        Args:
            db_manager: Instância do gerenciador de banco de dados
        """
        self.db_manager = db_manager
        self.time_converter = TimeConverter()
    
    def parse_connection(self, record: Dict, filename: str) -> Optional[Dict]:
        """
        Converte um registro de conexão JSON para o formato de banco de dados
        
        Args:
            record: Registro do JSON
            filename: Nome do arquivo de origem (para logging)
            
        Returns:
            Dict com dados processados ou None em caso de erro
        """
        # Verificar se é uma conexão
        if record.get('@type') != 'connection':
            return None
        
        try:
            # Extrair ID (remover @ do início)
            record_id = record.get('id', '')
            if record_id.startswith('@'):
                record_id = record_id[1:]
            
            # Extrair campos básicos
            name = record.get('name', '')
            description = record.get('description')
            runtime_environment_id = record.get('runtimeEnvironmentId')
            instance_display_name = record.get('instanceDisplayName')
            
            # Extrair campos de conexão
            host = record.get('host')
            database = record.get('database')
            codepage = record.get('codepage')
            authentication_type = record.get('authenticationType')
            adjusted_jdbc_host_name = record.get('adjustedJdbcHostName')
            schema = record.get('schema')
            short_description = record.get('shortDescription')
            type_ = record.get('type')
            port = record.get('port')
            password = record.get('password')
            username = record.get('username')
            timeout = record.get('timeout')
            internal = record.get('internal', False)
            retry_network_error = record.get('retryNetworkError', False)
            supports_cci_multi_group = record.get('supportsCCIMultiGroup', False)
            metadata_browsable = record.get('metadataBrowsable', False)
            support_labels = record.get('supportLabels', False)
            vault_enabled = record.get('vaultEnabled', False)
            
            # Processar timestamps
            major_update_time_str = record.get('majorUpdateTime', '')
            major_update_time = None
            major_update_time_brazil = None
            
            if major_update_time_str:
                try:
                    time_str = major_update_time_str.replace("Z", "+00:00")
                    major_update_time = datetime.fromisoformat(time_str)
                    major_update_time_brazil = self.time_converter.convert_to_brazil_time(major_update_time_str)
                except (ValueError, TypeError) as e:
                    print(f"⚠️  Erro ao processar timestamp para conexão {name}: {e}")
            
            # Converter campos para JSON
            conn_params = record.get('connParams', {})
            conn_params_json = json.dumps(conn_params) if conn_params else None
            
            vault_enabled_params = record.get('vaultEnabledParams', {})
            vault_enabled_params_json = json.dumps(vault_enabled_params) if vault_enabled_params else None
            
            # Processar federated_id
            federated_id = record.get('federatedId')
            if federated_id:
                federated_id = f"saas:@{federated_id}"
            
            return {
                'id': record_id,
                'name': name,
                'description': description,
                'runtime_environment_id': runtime_environment_id,
                'instance_display_name': instance_display_name,
                'host': host,
                'database': database,
                'codepage': codepage,
                'authentication_type': authentication_type,
                'adjusted_jdbc_host_name': adjusted_jdbc_host_name,
                'schema': schema,
                'short_description': short_description,
                'type': type_,
                'port': port,
                'password': password,
                'username': username,
                'major_update_time': major_update_time,
                'major_update_time_brazil': major_update_time_brazil,
                'timeout': timeout,
                'conn_params': conn_params_json,
                'internal': internal,
                'federated_id': federated_id,
                'retry_network_error': retry_network_error,
                'supports_cci_multi_group': supports_cci_multi_group,
                'metadata_browsable': metadata_browsable,
                'support_labels': support_labels,
                'vault_enabled': vault_enabled,
                'vault_enabled_params': vault_enabled_params_json
            }
        except Exception as e:
            print(f"❌ Erro ao processar registro do arquivo '{filename}': {e}")
            return None
    
    def insert_connection(self, connection: Dict) -> bool:
        """
        Insere ou atualiza uma conexão na tabela
        
        Args:
            connection: Dicionário com os dados da conexão
            
        Returns:
            bool: True se inseriu/atualizou com sucesso
        """
        query = """
            INSERT INTO fingerhard.connections (
                id, name, description, runtimeEnvironmentId, instanceDisplayName, 
                host, database_name, codepage, authenticationType, adjustedJdbcHostName, 
                schema_name, shortDescription, type, port, password, username,
                majorUpdateTime, majorUpdateTime_brazil, timeout, connParams, 
                internal, federatedId, retryNetworkError, supportsCCIMultiGroup, 
                metadataBrowsable, supportLabels, vaultEnabled, vaultEnabledParams
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                description = VALUES(description),
                runtimeEnvironmentId = VALUES(runtimeEnvironmentId),
                instanceDisplayName = VALUES(instanceDisplayName),
                host = VALUES(host),
                database_name = VALUES(database_name),
                codepage = VALUES(codepage),
                authenticationType = VALUES(authenticationType),
                adjustedJdbcHostName = VALUES(adjustedJdbcHostName),
                schema_name = VALUES(schema_name),
                shortDescription = VALUES(shortDescription),
                type = VALUES(type),
                port = VALUES(port),
                password = VALUES(password),
                username = VALUES(username),
                majorUpdateTime = VALUES(majorUpdateTime),
                majorUpdateTime_brazil = VALUES(majorUpdateTime_brazil),
                timeout = VALUES(timeout),
                connParams = VALUES(connParams),
                internal = VALUES(internal),
                federatedId = VALUES(federatedId),
                retryNetworkError = VALUES(retryNetworkError),
                supportsCCIMultiGroup = VALUES(supportsCCIMultiGroup),
                metadataBrowsable = VALUES(metadataBrowsable),
                supportLabels = VALUES(supportLabels),
                vaultEnabled = VALUES(vaultEnabled),
                vaultEnabledParams = VALUES(vaultEnabledParams)
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (
                    connection['id'],
                    connection['name'],
                    connection['description'],
                    connection['runtime_environment_id'],
                    connection['instance_display_name'],
                    connection['host'],
                    connection['database'],
                    connection['codepage'],
                    connection['authentication_type'],
                    connection['adjusted_jdbc_host_name'],
                    connection['schema'],
                    connection['short_description'],
                    connection['type'],
                    connection['port'],
                    connection['password'],
                    connection['username'],
                    connection['major_update_time'],
                    connection['major_update_time_brazil'],
                    connection['timeout'],
                    connection['conn_params'],
                    connection['internal'],
                    connection['federated_id'],
                    connection['retry_network_error'],
                    connection['supports_cci_multi_group'],
                    connection['metadata_browsable'],
                    connection['support_labels'],
                    connection['vault_enabled'],
                    connection['vault_enabled_params']
                ))
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao inserir conexão '{connection['name']}': {e}")
            return False
    
    def process_file(self, file_path: str, filename: str) -> Tuple[int, int]:
        """
        Processa um arquivo JSON e insere as conexões no banco
        
        Args:
            file_path: Caminho completo do arquivo
            filename: Nome do arquivo
            
        Returns:
            Tuple (total_conexoes, conexoes_inseridas)
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                records = json.load(file)
        except json.JSONDecodeError as e:
            print(f"❌ Erro ao carregar o arquivo JSON '{filename}': {e}")
            return 0, 0
        except Exception as e:
            print(f"❌ Erro ao ler o arquivo '{filename}': {e}")
            return 0, 0
        
        # Garantir que records é uma lista
        if not isinstance(records, list):
            if isinstance(records, dict):
                records = [records]
            else:
                print(f"⚠️  Formato inesperado no arquivo '{filename}'")
                return 0, 0
        
        total_conexoes = 0
        conexoes_inseridas = 0
        
        for record in records:
            total_conexoes += 1
            parsed_connection = self.parse_connection(record, filename)
            
            if parsed_connection:
                if self.insert_connection(parsed_connection):
                    conexoes_inseridas += 1
        
        return total_conexoes, conexoes_inseridas
    
    def process_directory(self, directory: str) -> Tuple[int, int, int]:
        """
        Processa todos os arquivos JSON de um diretório
        
        Args:
            directory: Diretório com os arquivos JSON
            
        Returns:
            Tuple (total_arquivos, total_conexoes, conexoes_inseridas)
        """
        # Normalizar o caminho do diretório
        directory = str(directory).replace('\\', '/')
        
        print(f"\n📂 Diretório de origem: {directory}")
        print("-" * 60)
        
        # Verificar se diretório existe
        if not os.path.exists(directory):
            print(f"❌ Erro: Diretório '{directory}' não encontrado.")
            return 0, 0, 0
        
        # Listar arquivos JSON
        try:
            arquivos_json = [f for f in os.listdir(directory) if f.endswith('.json')]
        except PermissionError:
            print(f"❌ Erro: Sem permissão para ler o diretório '{directory}'.")
            return 0, 0, 0
        
        if not arquivos_json:
            print(f"⚠️  Atenção: Nenhum arquivo JSON encontrado no diretório '{directory}'.")
            return 0, 0, 0
        
        print(f"\n📁 Processando {len(arquivos_json)} arquivo(s) JSON...")
        print("-" * 60)
        
        total_arquivos = len(arquivos_json)
        total_conexoes = 0
        total_inseridas = 0
        
        for filename in sorted(arquivos_json):
            file_path = os.path.join(directory, filename)
            print(f"\n📄 Processando: {filename}")
            
            conexoes, inseridas = self.process_file(file_path, filename)
            total_conexoes += conexoes
            total_inseridas += inseridas
            
            print(f"   Conexões encontradas: {conexoes}, Inseridas/Atualizadas: {inseridas}")
        
        return total_arquivos, total_conexoes, total_inseridas


class ConnectionLoader:
    """Classe principal para carga de conexões"""
    
    def __init__(self):
        self.config = None
        self.db_manager = None
        self.processor = None
    
    def load_configuration(self):
        """Carrega as configurações do sistema"""
        self.config = ConfigLoader.load_config()
        
        # Validar chave obrigatória
        if 'PathfileConnecion' not in self.config:
            print(f"❌ Erro: Chave 'PathfileConnecion' ausente no arquivo de configuração")
            sys.exit(1)
        
        print(f"\n✓ Configuração carregada:")
        print(f"   Diretório de conexões: {self.config['PathfileConnecion']}")
    
    def setup_database(self):
        """Configura a conexão com o banco de dados"""
        self.db_manager = DatabaseManager()
        if not self.db_manager.connect():
            sys.exit(1)
        
        self.processor = ConnectionProcessor(self.db_manager)
    
    def run(self):
        """Executa o fluxo principal de processamento"""
        print("\n" + "=" * 60)
        print("🚀 CONNECTION LOADER - IICS")
        print("=" * 60)
        print("\n📌 Processando conexões para tabela fingerhard.connections")
        
        # Carregar configurações
        self.load_configuration()
        
        # Configurar banco de dados
        self.setup_database()
        
        try:
            # Processar arquivos JSON
            directory = self.config['PathfileConnecion']
            total_arquivos, total_conexoes, total_inseridas = self.processor.process_directory(directory)
            
            # Commit final
            self.db_manager.commit()
            
            # Relatório final
            print("\n" + "=" * 60)
            print("📊 RESUMO FINAL DO PROCESSAMENTO")
            print("=" * 60)
            print(f"📄 Total de arquivos processados: {total_arquivos}")
            print(f"🔌 Total de conexões encontradas: {total_conexoes}")
            print(f"✅ Total de conexões inseridas/atualizadas: {total_inseridas}")
            print(f"📁 Tabela destino: fingerhard.connections")
            print("=" * 60)
            
            if total_inseridas == total_conexoes and total_conexoes > 0:
                print("\n✨ Processamento concluído com SUCESSO!")
            elif total_conexoes == 0:
                print("\n⚠️  Nenhuma conexão foi processada!")
                sys.exit(1)
            else:
                print(f"\n⚠️  Processamento com {total_conexoes - total_inseridas} falha(s)!")
                sys.exit(1)
            
        except Exception as e:
            print(f"\n❌ Erro inesperado: {e}")
            self.db_manager.rollback()
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            self.db_manager.close()


def main():
    """Função principal"""
    loader = ConnectionLoader()
    loader.run()


if __name__ == "__main__":
    main()