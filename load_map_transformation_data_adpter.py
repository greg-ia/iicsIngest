#!/usr/bin/env python3
"""
Script para processar data_adapter dos transformations
Carrega dados na tabela fingerhard.transformation_data_adapter
e registra anomalias de nomenclatura na tabela AnomalyInsightTable
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


class NamingValidator:
    """Validador de nomenclatura de transformations"""
    
    # Prefixos válidos para nomes de transformations
    VALID_PREFIXES = ["agg_", "exp_", "fil_", "jnr_", "rtr_", "seq_", 
                      "str_", "trc_", "uni_", "srt_", "nor_", "tgt_", 
                      "src_", "lkp_"]
    
    # Prefixos/nomes que devem ser ignorados na validação
    IGNORED_NAMES = ["BI_CTRL_TRANSFORMACAO"]
    
    @classmethod
    def is_valid_name(cls, name: str) -> bool:
        """
        Verifica se o nome do transformation segue o padrão esperado
        
        Args:
            name: Nome do transformation
            
        Returns:
            bool: True se o nome é válido
        """
        if not name:
            return True
        
        # Verificar se é um nome ignorado
        if name in cls.IGNORED_NAMES:
            return True
        
        # Verificar se começa com 'ff_' (ignorar)
        if name.lower().startswith('ff_'):
            return True
        
        # Verificar se tem prefixo válido
        name_lower = name.lower()
        return any(name_lower.startswith(prefix) for prefix in cls.VALID_PREFIXES)
    
    @classmethod
    def get_validation_message(cls, name: str) -> str:
        """
        Retorna a mensagem de erro para nomes inválidos
        
        Args:
            name: Nome do transformation
            
        Returns:
            str: Mensagem de erro
        """
        valid_prefixes_str = ", ".join(cls.VALID_PREFIXES)
        return (f"As 3 primeiras posições de {name} devem seguir os prefixos válidos: "
                f"{valid_prefixes_str}. Nomes ignorados: {', '.join(cls.IGNORED_NAMES)} e prefixo 'ff_'")


class DataAdapterProcessor:
    """Processador de data_adapter dos transformations"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Inicializa o processador
        
        Args:
            db_manager: Instância do gerenciador de banco de dados
        """
        self.db_manager = db_manager
        self.naming_validator = NamingValidator()
    
    def delete_old_records(self, codigo_onda: str) -> bool:
        """
        Remove registros antigos da tabela para o código da onda
        
        Args:
            codigo_onda: Código da onda
            
        Returns:
            bool: True se removeu com sucesso
        """
        query = "DELETE FROM fingerhard.transformation_data_adapter WHERE CodOnda = %s"
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (codigo_onda,))
                rows_deleted = cursor.rowcount
            print(f"✓ Registros antigos removidos: {rows_deleted} registros")
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao remover registros antigos: {e}")
            return False
    
    def get_transformations(self, codigo_onda: str) -> List[Dict]:
        """
        Busca transformations da tabela principal
        
        Args:
            codigo_onda: Código da onda
            
        Returns:
            Lista de dicionários com os transformations
        """
        query = """
            SELECT 
                id, CodOnda, CodProcess, content_name, data_adapter, 
                name, create_time_brazil
            FROM fingerhard.transformation
            WHERE CodOnda = %s 
              AND DATE(dt_updated) = DATE(%s)
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (codigo_onda, self.db_manager.date_ins))
                transformations = cursor.fetchall()
            print(f"✓ Transformations encontrados: {len(transformations)} registro(s)")
            return transformations
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao buscar transformations: {e}")
            return []
    
    def register_naming_anomaly(self, cod_onda: str, cod_process: str, 
                                 content_name: str, transformation_name: str) -> bool:
        """
        Registra uma anomalia de nomenclatura na tabela AnomalyInsightTable
        
        Args:
            cod_onda: Código da onda
            cod_process: Código do processo
            content_name: Nome do conteúdo
            transformation_name: Nome do transformation
            
        Returns:
            bool: True se registrou com sucesso
        """
        query = """
            INSERT INTO fingerhard.AnomalyInsightTable (
                CodOnda, InconsistencyCode, Note, ProcessName, ObjectType, 
                CodProcess, DateIns
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        note = self.naming_validator.get_validation_message(transformation_name)
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (
                    cod_onda,
                    5000,  # Código de inconsistência para nomenclatura inválida
                    note,
                    content_name,
                    'Map',
                    cod_process,
                    self.db_manager.date_ins
                ))
            return True
        except pymysql.MySQLError as e:
            # Ignora erro de duplicata (já registrado anteriormente)
            if e.args[0] != 1062:  # ER_DUP_ENTRY
                print(f"⚠️  Erro ao registrar anomalia: {e}")
            return False
    
    def parse_data_adapter(self, data_adapter_json: str, content_name: str) -> Optional[Dict]:
        """
        Parse do JSON de data_adapter
        
        Args:
            data_adapter_json: String JSON do data_adapter
            content_name: Nome do conteúdo para logging
            
        Returns:
            Dict com dados do data_adapter ou None em caso de erro
        """
        if not data_adapter_json or not data_adapter_json.strip():
            return None
        
        try:
            data_adapter = json.loads(data_adapter_json)
            
            # Se for uma lista, pega o primeiro elemento
            if isinstance(data_adapter, list):
                data_adapter = data_adapter[0] if data_adapter else None
            
            if not isinstance(data_adapter, dict):
                print(f"⚠️  Formato inesperado de data_adapter para {content_name}")
                return None
            
            return data_adapter
        except json.JSONDecodeError as e:
            print(f"❌ Erro ao decodificar JSON para {content_name}: {e}")
            return None
    
    def insert_data_adapter(self, adapter_data: Dict) -> bool:
        """
        Insere ou atualiza um data_adapter na tabela
        
        Args:
            adapter_data: Dicionário com os dados do data_adapter
            
        Returns:
            bool: True se inseriu/atualizou com sucesso
        """
        query = """
            INSERT INTO fingerhard.transformation_data_adapter (
                id, id_transf, _class, compatible_engine, connection_id, 
                exclude_dynamic_file_name_field, fw_config_id, multiple_object, 
                object_type, type_system, use_dynamic_file_name, object, 
                opr_runtime_attributes, read_options, runtime_attributes, 
                content_name, CodOnda, CodProcess, Dt_Inserted, Dt_updated
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
                Dt_updated = VALUES(Dt_updated)
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (
                    adapter_data['id'],
                    adapter_data['id_transf'],
                    adapter_data['class'],
                    adapter_data['compatible_engine'],
                    adapter_data['connection_id'],
                    adapter_data['exclude_dynamic_file_name_field'],
                    adapter_data['fw_config_id'],
                    adapter_data['multiple_object'],
                    adapter_data['object_type'],
                    adapter_data['type_system'],
                    adapter_data['use_dynamic_file_name'],
                    adapter_data['object'],
                    adapter_data['opr_runtime_attributes'],
                    adapter_data['read_options'],
                    adapter_data['runtime_attributes'],
                    adapter_data['content_name'],
                    adapter_data['cod_onda'],
                    adapter_data['cod_process'],
                    adapter_data['dt_inserted'],
                    adapter_data['dt_updated']
                ))
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao inserir data_adapter: {e}")
            return False
    
    def process_transformation(self, transformation: Dict) -> Tuple[bool, bool]:
        """
        Processa um transformation, validando nome e extraindo data_adapter
        
        Args:
            transformation: Dicionário com dados do transformation
            
        Returns:
            Tuple (nome_valido, data_adapter_processado)
        """
        id_transf = transformation['id']
        cod_onda = transformation['CodOnda']
        cod_process = transformation['CodProcess']
        content_name = transformation['content_name']
        data_adapter_json = transformation['data_adapter']
        transformation_name = transformation['name']
        create_time_brazil = transformation['create_time_brazil']
        
        # Validar nome do transformation
        nome_valido = self.naming_validator.is_valid_name(transformation_name)
        
        if not nome_valido:
            print(f"   ⚠️  Nome inválido: '{transformation_name}' - Registrando anomalia")
            self.register_naming_anomaly(cod_onda, cod_process, content_name, transformation_name)
        
        # Processar data_adapter
        data_adapter_processed = False
        
        if data_adapter_json and data_adapter_json.strip():
            data_adapter = self.parse_data_adapter(data_adapter_json, content_name)
            
            if data_adapter:
                # Extrair campos do data_adapter
                adapter_data = {
                    'id': data_adapter.get('$$ID'),
                    'id_transf': id_transf,
                    'class': data_adapter.get('$$class', ''),
                    'compatible_engine': data_adapter.get('compatibleEngine'),
                    'connection_id': data_adapter.get('connectionId'),
                    'exclude_dynamic_file_name_field': data_adapter.get('excludeDynamicFileNameField'),
                    'fw_config_id': data_adapter.get('fwConfigId'),
                    'multiple_object': data_adapter.get('multipleObject'),
                    'object_type': data_adapter.get('objectType'),
                    'type_system': data_adapter.get('typeSystem'),
                    'use_dynamic_file_name': data_adapter.get('useDynamicFileName'),
                    'object': json.dumps(data_adapter.get('object', {})),
                    'opr_runtime_attributes': json.dumps(data_adapter.get('oprRuntimeAttributes', {})),
                    'read_options': json.dumps(data_adapter.get('readOptions', {})),
                    'runtime_attributes': json.dumps(data_adapter.get('runtimeAttributes', {})),
                    'content_name': content_name,
                    'cod_onda': cod_onda,
                    'cod_process': cod_process,
                    'dt_inserted': create_time_brazil if create_time_brazil else self.db_manager.date_ins,
                    'dt_updated': self.db_manager.date_ins
                }
                
                if self.insert_data_adapter(adapter_data):
                    data_adapter_processed = True
        
        return nome_valido, data_adapter_processed
    
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
        
        # Buscar transformations
        transformations = self.get_transformations(codigo_onda)
        
        if not transformations:
            print("⚠️  Nenhum transformation encontrado para processar")
            return True
        
        # Processar cada transformation
        total = len(transformations)
        nomes_validos = 0
        data_adapters_processados = 0
        
        print("\n📦 Processando transformations...")
        for transformation in transformations:
            transformation_name = transformation['name']
            nome_valido, adapter_processed = self.process_transformation(transformation)
            
            if nome_valido:
                nomes_validos += 1
            
            if adapter_processed:
                data_adapters_processados += 1
            
            # Log resumido
            status = []
            if not nome_valido:
                status.append("⚠️ nome inválido")
            if adapter_processed:
                status.append("✓ adapter processado")
            
            if status:
                print(f"   📊 {transformation_name}: {', '.join(status)}")
        
        print("\n" + "-" * 60)
        print(f"📊 Resumo do processamento:")
        print(f"   Total de transformations processados: {total}")
        print(f"   Transformations com nomenclatura válida: {nomes_validos}")
        print(f"   Transformations com nomenclatura inválida: {total - nomes_validos}")
        print(f"   Data_adapters processados: {data_adapters_processados}")
        
        return True


class DataAdapterLoader:
    """Classe principal para carga de data_adapters"""
    
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
        
        self.processor = DataAdapterProcessor(self.db_manager)
    
    def run(self):
        """Executa o fluxo principal de processamento"""
        print("\n" + "=" * 60)
        print("🚀 DATA ADAPTER LOADER - IICS")
        print("=" * 60)
        print("\n📌 Processando data_adapters para tabela fingerhard.transformation_data_adapter")
        print("📌 Validando nomenclatura de transformations (anomalias registradas)")
        
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
            # Processar data_adapters
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
    loader = DataAdapterLoader()
    loader.run()


if __name__ == "__main__":
    main()