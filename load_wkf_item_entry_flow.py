#!/usr/bin/env python3
"""
Script para processar arquivos XML de taskflows do ActiveMatrix
Carrega dados nas tabelas: Item, Entry, TempFields, Deployment, Flow, 
Service, ServiceInput, ServiceOperation, ServiceOutput, Dependencies
"""

import xml.etree.ElementTree as ET
import os
import sys
import json
import pymysql
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
import time
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
            'cursorclass': pymysql.cursors.Cursor,
            'connect_timeout': 30,
            'read_timeout': 300,
            'write_timeout': 300
        }
        self.conn = None
        
        # Validar credenciais
        missing = [k for k, v in self.config.items() 
                   if not v and k not in ['autocommit', 'charset', 'cursorclass', 
                                          'connect_timeout', 'read_timeout', 'write_timeout']]
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
            
            # Configurações de performance
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute('SET SESSION bulk_insert_buffer_size = 1024 * 1024 * 32')
                    cursor.execute('SET SESSION unique_checks = 0')
                    cursor.execute('SET SESSION foreign_key_checks = 0')
                self.conn.commit()
            except:
                pass  # Se não funcionar, continuamos mesmo assim
            
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao conectar ao MySQL: {e}")
            return False
    
    def get_cursor(self):
        """Retorna um cursor para execução de queries"""
        return self.conn.cursor()
    
    def get_projeto_info(self, cod_projeto: str) -> Optional[Dict]:
        """
        Busca informações do projeto no banco ruleguardian.projetos
        
        Args:
            cod_projeto: Código do projeto
            
        Returns:
            Dict com informações do projeto ou None
        """
        query = """
            SELECT 
                cod_projeto,
                nme_projeto
            FROM ruleguardian.projetos
            WHERE cod_projeto = %s
        """
        
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, (cod_projeto,))
                result = cursor.fetchone()
                
                if not result:
                    print(f"❌ Projeto com código {cod_projeto} não encontrado.")
                    return None
                
                # Formar o assunto: cod_projeto_nme_projeto
                assunto = f"{result[0]}_{result[1]}"
                
                print(f"✓ Projeto encontrado:")
                print(f"   Código: {result[0]}")
                print(f"   Nome: {result[1]}")
                print(f"   Assunto: {assunto}")
                
                return {
                    'cod_projeto': result[0],
                    'nme_projeto': result[1],
                    'assunto': assunto
                }
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao buscar projeto: {e}")
            return None
    
    def commit(self):
        """Confirma as alterações no banco"""
        if self.conn:
            self.conn.commit()
    
    def rollback(self):
        """Desfaz as alterações no banco"""
        if self.conn:
            self.conn.rollback()
    
    def restore_settings(self):
        """Restaura configurações do MySQL"""
        try:
            with self.conn.cursor() as cursor:
                cursor.execute('SET SESSION unique_checks = 1')
                cursor.execute('SET SESSION foreign_key_checks = 1')
            self.conn.commit()
        except:
            pass
    
    def close(self):
        """Fecha a conexão com o banco"""
        if self.conn:
            self.conn.close()
            print("✓ Conexão com o banco de dados MySQL fechada.")


class TimeConverter:
    """Conversor de timestamps"""
    
    @staticmethod
    def to_mysql_datetime(dt_str: Optional[str]) -> Optional[datetime]:
        """
        Converte string ISO para format DATETIME do MySQL ou None
        
        Args:
            dt_str: String de data/hora no formato ISO
            
        Returns:
            datetime ou None
        """
        if not dt_str or dt_str in ["", "1900-01-01T00:00:00Z"]:
            return None
        
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1]
        dt_str = dt_str.replace("T", " ")
        
        try:
            return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            try:
                return datetime.strptime(dt_str, "%Y-%m-%d")
            except Exception:
                return None
    
    @staticmethod
    def safe_text(elem) -> str:
        """Retorna texto limpo ou string vazia se None"""
        return elem.text.strip() if elem is not None and elem.text is not None else ''


class BatchProcessor:
    """Processador em lote para otimizar operações de banco"""
    
    def __init__(self, conn, batch_size: int = 500):
        """
        Inicializa o processador em lote
        
        Args:
            conn: Conexão com o banco de dados
            batch_size: Tamanho do lote para commit
        """
        self.conn = conn
        self.batch_size = batch_size
        self.batches: Dict[str, List[Tuple]] = {
            'Item': [], 'Entry': [], 'TempFields': [], 'Deployment': [], 
            'Flow': [], 'Service': [], 'ServiceInput': [], 'ServiceOperation': [], 
            'ServiceOutput': [], 'Dependencies': []
        }
        self.sql_statements = self._prepare_sql_statements()
        self.stats = {table: 0 for table in self.batches}
    
    def _prepare_sql_statements(self) -> Dict[str, str]:
        """Prepara as instruções SQL para cada tabela"""
        return {
            'Item': """
                INSERT INTO fingerhard.Item (
                    EntryId, Name, MimeType, Description, AppliesTo, Tags, VersionLabel, 
                    State, ProcessGroup, CreatedBy, CreationDate, ModifiedBy, ModificationDate, 
                    PublicationStatus, PublishedBy, PublicationDate, PublishedContributionId, 
                    GUID, DisplayName, CurrentServerDateTime
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Name=VALUES(Name), MimeType=VALUES(MimeType), Description=VALUES(Description), 
                    AppliesTo=VALUES(AppliesTo), Tags=VALUES(Tags), VersionLabel=VALUES(VersionLabel),
                    State=VALUES(State), ProcessGroup=VALUES(ProcessGroup), CreatedBy=VALUES(CreatedBy), 
                    CreationDate=VALUES(CreationDate), ModifiedBy=VALUES(ModifiedBy), 
                    ModificationDate=VALUES(ModificationDate), PublicationStatus=VALUES(PublicationStatus), 
                    PublishedBy=VALUES(PublishedBy), PublicationDate=VALUES(PublicationDate), 
                    PublishedContributionId=VALUES(PublishedContributionId), GUID=VALUES(GUID),
                    DisplayName=VALUES(DisplayName), CurrentServerDateTime=VALUES(CurrentServerDateTime)
            """,
            'Entry': """
                INSERT INTO fingerhard.Entry (
                    EntryId, DisplayName, EntryName, OverrideAPIName, Description, Tags, Generator
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    DisplayName=VALUES(DisplayName), EntryName=VALUES(EntryName), 
                    OverrideAPIName=VALUES(OverrideAPIName), Description=VALUES(Description), 
                    Tags=VALUES(Tags), Generator=VALUES(Generator)
            """,
            'TempFields': """
                INSERT INTO fingerhard.TempFields (
                    EntryId, Description, Name, Type, Options
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Description=VALUES(Description), Type=VALUES(Type), Options=VALUES(Options)
            """,
            'Deployment': """
                INSERT INTO fingerhard.Deployment (
                    EntryId, SkipIfRunning, SuspendOnFault, TracingLevel, AllowedGroups
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    SkipIfRunning=VALUES(SkipIfRunning), SuspendOnFault=VALUES(SuspendOnFault), 
                    TracingLevel=VALUES(TracingLevel), AllowedGroups=VALUES(AllowedGroups)
            """,
            'Flow': """
                INSERT INTO fingerhard.Flow (
                    EntryId, FlowId, StartId, StartTargetId
                ) VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    FlowId=VALUES(FlowId), StartId=VALUES(StartId), StartTargetId=VALUES(StartTargetId)
            """,
            'Service': """
                INSERT INTO fingerhard.Service (
                    EntryId, ServiceId, Title, ServiceName, ServiceGUID
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Title=VALUES(Title), ServiceName=VALUES(ServiceName), ServiceGUID=VALUES(ServiceGUID)
            """,
            'ServiceInput': """
                INSERT INTO fingerhard.ServiceInput (
                    EntryId, ServiceId, ParameterName, Source, Value
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Source=VALUES(Source), Value=VALUES(Value)
            """,
            'ServiceOperation': """
                INSERT INTO fingerhard.ServiceOperation (
                    EntryId, ServiceId, ParameterName, OperationSource, OperationTarget, Value
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Value=VALUES(Value)
            """,
            'ServiceOutput': """
                INSERT INTO fingerhard.ServiceOutput (
                    EntryId, ServiceId, OperationSource, OperationTarget, Value
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Value=VALUES(Value)
            """,
            'Dependencies': """
                INSERT IGNORE INTO fingerhard.Dependencies (
                    EntryId, ProcessObject
                ) VALUES (%s, %s)
            """
        }
    
    def _add_to_batch(self, table: str, params: Tuple):
        """Adiciona parâmetros ao batch e executa se atingir o tamanho"""
        self.batches[table].append(params)
        if len(self.batches[table]) >= self.batch_size:
            self._flush_table(table)
    
    def _flush_table(self, table: str):
        """Executa batch para uma tabela específica"""
        if not self.batches[table]:
            return
        
        try:
            with self.conn.cursor() as cursor:
                sql = self.sql_statements[table]
                cursor.executemany(sql, self.batches[table])
            self.conn.commit()
            count = len(self.batches[table])
            self.stats[table] += count
            self.batches[table] = []
        except Exception as e:
            print(f"❌ Erro no batch para {table}: {e}")
            self.conn.rollback()
            self._insert_one_by_one(table)
    
    def _insert_one_by_one(self, table: str):
        """Fallback: insere registros um por um em caso de erro no batch"""
        print(f"   Fallback: inserindo registros de {table} um por um...")
        sql = self.sql_statements[table]
        
        success = 0
        for params in self.batches[table]:
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    success += 1
            except Exception as e:
                print(f"     Erro ao inserir registro individual: {e}")
        
        self.conn.commit()
        self.stats[table] += success
        print(f"   Inseridos {success}/{len(self.batches[table])} registros de {table}")
        self.batches[table] = []
    
    def add_item(self, params: Tuple):
        self._add_to_batch('Item', params)
    
    def add_entry(self, params: Tuple):
        self._add_to_batch('Entry', params)
    
    def add_tempfields(self, params: Tuple):
        self._add_to_batch('TempFields', params)
    
    def add_deployment(self, params: Tuple):
        self._add_to_batch('Deployment', params)
    
    def add_flow(self, params: Tuple):
        self._add_to_batch('Flow', params)
    
    def add_service(self, params: Tuple):
        self._add_to_batch('Service', params)
    
    def add_serviceinput(self, params: Tuple):
        self._add_to_batch('ServiceInput', params)
    
    def add_serviceoperation(self, params: Tuple):
        self._add_to_batch('ServiceOperation', params)
    
    def add_serviceoutput(self, params: Tuple):
        self._add_to_batch('ServiceOutput', params)
    
    def add_dependencies(self, params: Tuple):
        self._add_to_batch('Dependencies', params)
    
    def flush_all(self):
        """Executa todos os batches pendentes"""
        for table in self.batches:
            self._flush_table(table)
    
    def print_stats(self):
        """Imprime estatísticas do processamento"""
        print("\n📊 ESTATÍSTICAS DO PROCESSAMENTO")
        print("=" * 60)
        total = 0
        for table, count in self.stats.items():
            if count > 0:
                print(f"   {table}: {count} registros")
                total += count
        print("-" * 60)
        print(f"   TOTAL: {total} registros")
        print("=" * 60)


class XMLProcessor:
    """Processador de arquivos XML de taskflows"""
    
    # Namespaces do XML
    NAMESPACES = {
        'aetgt': 'http://schemas.active-endpoints.com/appmodules/repository/2010/10/avrepository.xsd',
        'types1': 'http://schemas.active-endpoints.com/appmodules/repository/2010/10/avrepository.xsd',
        'ns0': 'http://schemas.active-endpoints.com/appmodules/screenflow/2010/10/avosScreenflow.xsd',
        'ns1': 'http://schemas.active-endpoints.com/appmodules/screenflow/2021/04/taskflowModel.xsd',
        'ns2': 'http://schemas.active-endpoints.com/appmodules/screenflow/2011/06/avosHostEnvironment.xsd'
    }
    
    def __init__(self, processor: BatchProcessor):
        """
        Inicializa o processador de XML
        
        Args:
            processor: Processador de batch
        """
        self.processor = processor
        self.time_converter = TimeConverter()
    
    def delete_old_data(self, conn, cod_onda: str) -> bool:
        """
        Deleta dados antigos de forma otimizada
        
        Args:
            conn: Conexão com o banco
            cod_onda: Código da onda
            
        Returns:
            bool: True se deletou com sucesso
        """
        print(f"\n🗑️  Deletando dados da onda {cod_onda}...")
        start_time = time.time()
        
        try:
            with conn.cursor() as cursor:
                # Padrão: ____{cod_onda}% (4 caracteres + código da onda + qualquer coisa)
                pattern = f'____{cod_onda}%'
                
                # Primeiro obter os EntryIds que serão deletados
                cursor.execute("SELECT EntryId FROM fingerhard.Item WHERE Name LIKE %s", (pattern,))
                entry_ids = [row[0] for row in cursor.fetchall()]
                
                total_to_delete = len(entry_ids)
                if total_to_delete == 0:
                    print("   Nenhum registro para deletar")
                    conn.commit()
                    return True
                
                print(f"   {total_to_delete} registros identificados para deleção")
                
                # Criar string com placeholders
                placeholders = ', '.join(['%s'] * len(entry_ids))
                
                # Ordem das tabelas para manter integridade referencial
                tabelas = [
                    "fingerhard.ServiceOperation", "fingerhard.ServiceOutput", "fingerhard.ServiceInput", "fingerhard.Service",
                    "fingerhard.Dependencies", "fingerhard.Flow", "fingerhard.Deployment", "fingerhard.TempFields", "fingerhard.Entry"
                ]
                
                # Deletar das tabelas dependentes
                for tabela in tabelas:
                    cursor.execute(
                        f"DELETE FROM {tabela} WHERE EntryId IN ({placeholders})",
                        entry_ids
                    )
                    print(f"   Deletados {cursor.rowcount} registros de {tabela}")
                
                # Finalmente deletar da tabela Item
                cursor.execute(
                    f"DELETE FROM fingerhard.Item WHERE EntryId IN ({placeholders})",
                    entry_ids
                )
                print(f"   Deletados {cursor.rowcount} registros de Item")
                
                conn.commit()
                
                elapsed = time.time() - start_time
                print(f"   ✅ Deleção concluída em {elapsed:.1f} segundos")
                return True
                
        except Exception as e:
            print(f"❌ Erro na deleção: {e}")
            conn.rollback()
            return False
    
    def process_xml_file(self, file_path: str):
        """
        Processa um arquivo XML e adiciona dados ao processador em lote
        
        Args:
            file_path: Caminho do arquivo XML
        """
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            for item in root.findall('types1:Item', self.NAMESPACES):
                try:
                    # Processar Item
                    entry_id = self.time_converter.safe_text(item.find('types1:EntryId', self.NAMESPACES))
                    if not entry_id:  # Pular se não tiver EntryId
                        continue
                        
                    name = self.time_converter.safe_text(item.find('types1:Name', self.NAMESPACES))
                    mime_type = self.time_converter.safe_text(item.find('types1:MimeType', self.NAMESPACES))
                    description = self.time_converter.safe_text(item.find('types1:Description', self.NAMESPACES))
                    applies_to = self.time_converter.safe_text(item.find('types1:AppliesTo', self.NAMESPACES))
                    tags = self.time_converter.safe_text(item.find('types1:Tags', self.NAMESPACES))
                    version_label = self.time_converter.safe_text(item.find('types1:VersionLabel', self.NAMESPACES))
                    state = self.time_converter.safe_text(item.find('types1:State', self.NAMESPACES))
                    process_group = self.time_converter.safe_text(item.find('types1:ProcessGroup', self.NAMESPACES))
                    created_by = self.time_converter.safe_text(item.find('types1:CreatedBy', self.NAMESPACES))
                    
                    creation_date_elem = item.find('types1:CreationDate', self.NAMESPACES)
                    creation_date = self.time_converter.to_mysql_datetime(
                        creation_date_elem.text if creation_date_elem is not None else None
                    )
                    
                    modified_by = self.time_converter.safe_text(item.find('types1:ModifiedBy', self.NAMESPACES))
                    
                    modification_date_elem = item.find('types1:ModificationDate', self.NAMESPACES)
                    modification_date = self.time_converter.to_mysql_datetime(
                        modification_date_elem.text if modification_date_elem is not None else None
                    )
                    
                    publication_status = self.time_converter.safe_text(item.find('types1:PublicationStatus', self.NAMESPACES))
                    published_by = self.time_converter.safe_text(item.find('types1:PublishedBy', self.NAMESPACES))
                    
                    publication_date_elem = item.find('types1:PublicationDate', self.NAMESPACES)
                    publication_date = self.time_converter.to_mysql_datetime(
                        publication_date_elem.text if publication_date_elem is not None else None
                    )
                    
                    published_contribution_id = self.time_converter.safe_text(item.find('types1:PublishedContributionId', self.NAMESPACES))
                    guid = self.time_converter.safe_text(item.find('types1:GUID', self.NAMESPACES))
                    display_name = self.time_converter.safe_text(item.find('types1:DisplayName', self.NAMESPACES))
                    
                    current_server_date_time_elem = root.find('types1:CurrentServerDateTime', self.NAMESPACES)
                    current_server_date_time = self.time_converter.to_mysql_datetime(
                        current_server_date_time_elem.text if current_server_date_time_elem is not None else None
                    )
                    
                    # Adicionar ao batch do Item
                    self.processor.add_item((
                        entry_id, name, mime_type, description, applies_to, tags, 
                        version_label, state, process_group, created_by, creation_date,
                        modified_by, modification_date, publication_status, published_by,
                        publication_date, published_contribution_id, guid, display_name,
                        current_server_date_time
                    ))
                    
                    # Processar Entry
                    entry = item.find('types1:Entry', self.NAMESPACES)
                    if entry is not None:
                        taskflow = entry.find('ns0:taskflow', self.NAMESPACES)
                        if taskflow is not None:
                            entry_display_name = taskflow.get('displayName', '')
                            entry_name = taskflow.get('name', '')
                            entry_override_api_name = 1 if taskflow.get('overrideAPIName', '').lower() == 'true' else 0
                            entry_description = self.time_converter.safe_text(
                                taskflow.find('ns0:description', self.NAMESPACES)
                            )
                            entry_tags = self.time_converter.safe_text(
                                taskflow.find('ns0:tags', self.NAMESPACES)
                            )
                            entry_generator = self.time_converter.safe_text(
                                taskflow.find('ns0:generator', self.NAMESPACES)
                            )
                            
                            self.processor.add_entry((
                                entry_id, entry_display_name, entry_name, entry_override_api_name,
                                entry_description, entry_tags, entry_generator
                            ))
                            
                            # Processar TempFields
                            temp_fields = taskflow.find('ns0:tempFields', self.NAMESPACES)
                            if temp_fields is not None:
                                for field in temp_fields.findall('ns0:field', self.NAMESPACES):
                                    field_description = field.get('description', '') or ''
                                    field_name = field.get('name', '') or ''
                                    field_type = field.get('type', '') or ''
                                    options = []
                                    options_element = field.find('ns0:options', self.NAMESPACES)
                                    if options_element is not None:
                                        for option in options_element.findall('ns0:option', self.NAMESPACES):
                                            option_name = option.get('name', '') or ''
                                            option_value = option.text or ''
                                            options.append(f"{option_name}: {option_value}")
                                    options_str = '; '.join(options)
                                    
                                    self.processor.add_tempfields((
                                        entry_id, field_description, field_name, field_type, options_str
                                    ))
                            
                            # Processar Deployment
                            deployment = taskflow.find('ns0:deployment', self.NAMESPACES)
                            if deployment is not None:
                                skip_if_running = 1 if deployment.get('skipIfRunning') == 'true' else 0
                                suspend_on_fault = 1 if deployment.get('suspendOnFault') == 'true' else 0
                                tracing_level = self.time_converter.safe_text(
                                    deployment.find('ns0:tracingLevel', self.NAMESPACES)
                                )
                                
                                allowed_groups = []
                                allowed_groups_elem = deployment.find('ns0:allowedGroups', self.NAMESPACES)
                                if allowed_groups_elem is not None:
                                    for group in allowed_groups_elem.findall('ns0:group', self.NAMESPACES):
                                        if group.text is not None:
                                            allowed_groups.append(group.text.strip())
                                allowed_groups_str = ', '.join(allowed_groups) if allowed_groups else ''
                                
                                self.processor.add_deployment((
                                    entry_id, skip_if_running, suspend_on_fault, tracing_level, allowed_groups_str
                                ))
                            
                            # Processar Flow
                            flow = taskflow.find('ns0:flow', self.NAMESPACES)
                            if flow is not None:
                                flow_id = flow.get('id', '') or ''
                                start = flow.find('ns0:start', self.NAMESPACES)
                                start_id = start.get('id', '') if start is not None else ''
                                start_target_id = ''
                                if start is not None:
                                    link = start.find('ns0:link', self.NAMESPACES)
                                    if link is not None:
                                        start_target_id = link.get('targetId', '') or ''
                                
                                self.processor.add_flow((
                                    entry_id, flow_id, start_id, start_target_id
                                ))
                                
                                # Processar Services dentro do Flow
                                for event_container in flow.findall('ns0:eventContainer', self.NAMESPACES):
                                    for service in event_container.findall('ns0:service', self.NAMESPACES):
                                        service_id = service.get('id', '') or ''
                                        title = self.time_converter.safe_text(service.find('ns0:title', self.NAMESPACES))
                                        service_name = self.time_converter.safe_text(service.find('ns0:serviceName', self.NAMESPACES))
                                        service_guid = self.time_converter.safe_text(service.find('ns0:serviceGUID', self.NAMESPACES))
                                        
                                        self.processor.add_service((
                                            entry_id, service_id, title, service_name, service_guid
                                        ))
                                        
                                        # ServiceInput
                                        service_input = service.find('ns0:serviceInput', self.NAMESPACES)
                                        if service_input is not None:
                                            for parameter in service_input.findall('ns0:parameter', self.NAMESPACES):
                                                param_name = parameter.get('name', '') or ''
                                                source = parameter.get('source', '') or ''
                                                value = parameter.text or ''
                                                
                                                self.processor.add_serviceinput((
                                                    entry_id, service_id, param_name, source, value
                                                ))
                                                
                                                # ServiceOperation dentro do parameter
                                                for operation in parameter.findall('ns0:operation', self.NAMESPACES):
                                                    operation_source = operation.get('source', '') or ''
                                                    operation_target = operation.get('to', '') or ''
                                                    operation_value = operation.text or ''
                                                    
                                                    self.processor.add_serviceoperation((
                                                        entry_id, service_id, param_name, 
                                                        operation_source, operation_target, operation_value
                                                    ))
                                        
                                        # ServiceOutput
                                        service_output = service.find('ns0:serviceOutput', self.NAMESPACES)
                                        if service_output is not None:
                                            for operation in service_output.findall('ns0:operation', self.NAMESPACES):
                                                operation_source = operation.get('source', '') or ''
                                                operation_target = operation.get('to', '') or ''
                                                value = operation.text or ''
                                                
                                                self.processor.add_serviceoutput((
                                                    entry_id, service_id, operation_source, operation_target, value
                                                ))
                            
                            # Processar Dependencies
                            dependencies = taskflow.find('ns0:dependencies', self.NAMESPACES)
                            if dependencies is not None:
                                for process_object in dependencies.findall('ns2:processObject', self.NAMESPACES):
                                    process_object_name = process_object.get('name', '') or ''
                                    
                                    self.processor.add_dependencies((
                                        entry_id, process_object_name
                                    ))
                                    
                except Exception as e:
                    print(f"   ⚠️ Erro ao processar item no arquivo {os.path.basename(file_path)}: {e}")
                    continue
                    
        except Exception as e:
            print(f"❌ Erro ao processar o arquivo {file_path}: {e}")
    
    def process_directory(self, directory: str, conn) -> bool:
        """
        Processa todos os arquivos XML de um diretório
        
        Args:
            directory: Diretório com os arquivos XML
            conn: Conexão com o banco de dados
            
        Returns:
            bool: True se processou com sucesso
        """
        # Normalizar o caminho do diretório
        directory = str(directory).replace('\\', '/')
        
        print(f"\n📂 Diretório de origem: {directory}")
        print("-" * 60)
        
        # Verificar se diretório existe
        if not os.path.exists(directory):
            print(f"❌ Erro: Diretório '{directory}' não encontrado.")
            return False
        
        # Listar arquivos XML
        try:
            arquivos_xml = [f for f in os.listdir(directory) if f.endswith('.xml')]
        except PermissionError:
            print(f"❌ Erro: Sem permissão para ler o diretório '{directory}'.")
            return False
        
        if not arquivos_xml:
            print(f"⚠️  Atenção: Nenhum arquivo XML encontrado no diretório '{directory}'.")
            return True
        
        print(f"\n📁 Processando {len(arquivos_xml)} arquivo(s) XML...")
        print("-" * 60)
        
        start_time = time.time()
        files_processed = 0
        
        for filename in sorted(arquivos_xml):
            file_path = os.path.join(directory, filename)
            print(f"\n📄 [{files_processed + 1}/{len(arquivos_xml)}] Processando: {filename}")
            
            file_start = time.time()
            self.process_xml_file(file_path)
            files_processed += 1
            print(f"   ⏱️  Tempo: {time.time() - file_start:.1f} segundos")
            
            # Fazer flush a cada 5 arquivos para não consumir muita memória
            if files_processed % 5 == 0:
                print(f"\n   💾 Flush periódico após {files_processed} arquivos...")
                self.processor.flush_all()
        
        print(f"\n⏱️  Tempo total: {time.time() - start_time:.1f} segundos")
        return True


class XMLTaskflowLoader:
    """Classe principal para carga de taskflows a partir de XML"""
    
    def __init__(self):
        self.config = None
        self.db_manager = None
        self.processor = None
        self.xml_processor = None
    
    def load_configuration(self):
        """Carrega as configurações do sistema"""
        self.config = ConfigLoader.load_config()
        
        # Validar chave obrigatória
        if 'json_file_path' not in self.config:
            print(f"❌ Erro: Chave 'json_file_path' ausente no arquivo de configuração")
            sys.exit(1)
        
        print(f"\n✓ Configuração carregada:")
        print(f"   Base path: {self.config['json_file_path']}")
    
    def setup_database(self):
        """Configura a conexão com o banco de dados"""
        self.db_manager = DatabaseManager()
        if not self.db_manager.connect():
            sys.exit(1)
    
    def run(self):
        """Executa o fluxo principal de processamento"""
        print("\n" + "=" * 60)
        print("🚀 XML TASKFLOW LOADER - ActiveMatrix")
        print("=" * 60)
        
        # Validar argumentos
        if len(sys.argv) != 2:
            print("\n❌ Uso correto:")
            print(f"  python {sys.argv[0]} <cod_projeto>")
            print("\nExemplo:")
            print(f"  python {sys.argv[0]} 653")
            print(f"  python {sys.argv[0]} 684")
            print("=" * 60)
            sys.exit(1)
        
        cod_projeto = sys.argv[1]
        
        # Validar se é número
        if not cod_projeto.isdigit():
            print(f"\n❌ Error: cod_projeto deve ser um número inteiro")
            print(f"   Recebido: '{cod_projeto}'")
            sys.exit(1)
        
        # Carregar configurações
        self.load_configuration()
        
        # Configurar banco de dados
        self.setup_database()
        
        try:
            # Buscar informações do projeto
            projeto = self.db_manager.get_projeto_info(cod_projeto)
            
            if not projeto:
                print(f"\n❌ Projeto com código {cod_projeto} não encontrado.")
                sys.exit(1)
            
            assunto = projeto['assunto']
            
            # Montar caminho do diretório
            directory = os.path.join(self.config['json_file_path'], assunto, 'Explore', assunto, 'Geral')
            
            print(f"\n📌 Processando projeto: {assunto}")
            print(f"📂 Caminho completo: {directory}")
            
            # Deletar dados antigos
            if not self.xml_processor:
                batch_processor = BatchProcessor(self.db_manager.conn, batch_size=500)
                self.xml_processor = XMLProcessor(batch_processor)
            
            if not self.xml_processor.delete_old_data(self.db_manager.conn, cod_projeto):
                print("⚠️  Continuando mesmo com erro na deleção...")
            
            # Processar arquivos XML
            success = self.xml_processor.process_directory(directory, self.db_manager.conn)
            
            # Flush final
            print("\n💾 Flush final...")
            self.xml_processor.processor.flush_all()
            
            # Restaurar configurações do MySQL
            self.db_manager.restore_settings()
            
            # Estatísticas finais
            self.xml_processor.processor.print_stats()
            
            print("\n" + "=" * 60)
            if success:
                print("✨ Processamento concluído com SUCESSO!")
            else:
                print("⚠️  Processamento concluído com falhas!")
                sys.exit(1)
            print("=" * 60)
            
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
    loader = XMLTaskflowLoader()
    loader.run()


if __name__ == "__main__":
    main()