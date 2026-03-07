import xml.etree.ElementTree as ET
import os
import sys
import json
import pymysql
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
import time


def load_config(config_path):
    with open(config_path, 'r') as config_file:
        return json.load(config_file)


def mysql_connect():
    host = os.getenv('MYSQL_HOST', 'localhost')
    user = os.getenv('MYSQL_USER', 'seu_usuario')
    password = os.getenv('MYSQL_PASSWORD', 'sua_senha')
    database = os.getenv('MYSQL_DATABASE', 'controlequalidade')
    
    # Conexão simples, manteremos a mesma da versão original
    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        autocommit=False,  # Importante: desabilitar autocommit para batch
        charset='utf8mb4',
        cursorclass=pymysql.cursors.Cursor,
        # Aumentar timeouts para lidar com latência
        connect_timeout=30,
        read_timeout=300,
        write_timeout=300
    )
    
    # Configurações de performance executadas separadamente
    try:
        with conn.cursor() as cursor:
            cursor.execute('SET SESSION bulk_insert_buffer_size = 1024 * 1024 * 32')
            cursor.execute('SET SESSION unique_checks = 0')
            cursor.execute('SET SESSION foreign_key_checks = 0')
        conn.commit()
    except:
        pass  # Se não funcionar, continuamos mesmo assim
    
    return conn


def to_mysql_datetime(dt):
    """Converte string ISO para format DATETIME do MySQL ou None"""
    if not dt or dt in ["", "1900-01-01T00:00:00Z"]:
        return None
    if dt.endswith("Z"):
        dt = dt[:-1]
    dt = dt.replace("T", " ")
    try:
        return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(dt, "%Y-%m-%d")
        except Exception:
            return None


def safe_text(elem):
    """Retorna texto limpo ou string vazia se None"""
    return elem.text.strip() if elem is not None and elem.text is not None else ''


class BatchProcessor:
    """Processador em lote para otimizar operações de banco"""
    
    def __init__(self, conn, batch_size=500):  # Reduzido para 500 por segurança
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
                INSERT INTO Item (
                    EntryId, Name, MimeType, Description, AppliesTo, Tags, VersionLabel, State, ProcessGroup, CreatedBy,
                    CreationDate, ModifiedBy, ModificationDate, PublicationStatus, PublishedBy, PublicationDate,
                    PublishedContributionId, GUID, DisplayName, CurrentServerDateTime
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
                INSERT INTO Entry (
                    EntryId, DisplayName, EntryName, OverrideAPIName, Description, Tags, Generator
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    DisplayName=VALUES(DisplayName), EntryName=VALUES(EntryName), 
                    OverrideAPIName=VALUES(OverrideAPIName), Description=VALUES(Description), 
                    Tags=VALUES(Tags), Generator=VALUES(Generator)
            """,
            'TempFields': """
                INSERT INTO TempFields (
                    EntryId, Description, Name, Type, Options
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Description=VALUES(Description), Type=VALUES(Type), Options=VALUES(Options)
            """,
            'Deployment': """
                INSERT INTO Deployment (
                    EntryId, SkipIfRunning, SuspendOnFault, TracingLevel, AllowedGroups
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    SkipIfRunning=VALUES(SkipIfRunning), SuspendOnFault=VALUES(SuspendOnFault), 
                    TracingLevel=VALUES(TracingLevel), AllowedGroups=VALUES(AllowedGroups)
            """,
            'Flow': """
                INSERT INTO Flow (
                    EntryId, FlowId, StartId, StartTargetId
                ) VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    FlowId=VALUES(FlowId), StartId=VALUES(StartId), StartTargetId=VALUES(StartTargetId)
            """,
            'Service': """
                INSERT INTO Service (
                    EntryId, ServiceId, Title, ServiceName, ServiceGUID
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Title=VALUES(Title), ServiceName=VALUES(ServiceName), ServiceGUID=VALUES(ServiceGUID)
            """,
            'ServiceInput': """
                INSERT INTO ServiceInput (
                    EntryId, ServiceId, ParameterName, Source, Value
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Source=VALUES(Source), Value=VALUES(Value)
            """,
            'ServiceOperation': """
                INSERT INTO ServiceOperation (
                    EntryId, ServiceId, ParameterName, OperationSource, OperationTarget, Value
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Value=VALUES(Value)
            """,
            'ServiceOutput': """
                INSERT INTO ServiceOutput (
                    EntryId, ServiceId, OperationSource, OperationTarget, Value
                ) VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    Value=VALUES(Value)
            """,
            'Dependencies': """
                INSERT IGNORE INTO Dependencies (
                    EntryId, ProcessObject
                ) VALUES (%s, %s)
            """
        }
    
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
            print(f"Erro no batch para {table}: {e}")
            self.conn.rollback()
            # Fallback: tentar inserir um por um
            self._insert_one_by_one(table)
    
    def _insert_one_by_one(self, table: str):
        """Fallback: insere registros um por um em caso de erro no batch"""
        print(f"  Fallback: inserindo registros de {table} um por um...")
        sql = self.sql_statements[table]
        
        success = 0
        for params in self.batches[table]:
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    success += 1
            except Exception as e:
                print(f"    Erro ao inserir registro individual: {e}")
        
        self.conn.commit()
        self.stats[table] += success
        print(f"  Inseridos {success}/{len(self.batches[table])} registros de {table}")
        self.batches[table] = []
    
    def flush_all(self):
        """Executa todos os batches pendentes"""
        for table in self.batches:
            self._flush_table(table)
    
    def print_stats(self):
        """Imprime estatísticas do processamento"""
        print("\n=== ESTATÍSTICAS ===")
        total = 0
        for table, count in self.stats.items():
            if count > 0:
                print(f"{table}: {count} registros")
                total += count
        print(f"TOTAL: {total} registros")
        print("====================\n")


def delete_old_data_optimized(conn, cod_onda: str):
    """
    Deleta dados antigos de forma otimizada.
    Substitui MID(Name,5,3) por LIKE para melhor performance.
    """
    print(f"Deletando dados da onda {cod_onda}...")
    start_time = time.time()
    
    try:
        with conn.cursor() as cursor:
            # Usar LIKE em vez de MID - muito mais rápido
            # Padrão: ____{cod_onda}% (4 caracteres + código da onda + qualquer coisa)
            pattern = f'____{cod_onda}%'
            
            # Primeiro obter os EntryIds que serão deletados
            cursor.execute("SELECT EntryId FROM Item WHERE Name LIKE %s", (pattern,))
            entry_ids = [row[0] for row in cursor.fetchall()]
            
            total_to_delete = len(entry_ids)
            if total_to_delete == 0:
                print("Nenhum registro para deletar")
                conn.commit()
                return
            
            print(f"  {total_to_delete} registros identificados para deleção")
            
            # Criar string com placeholders
            placeholders = ', '.join(['%s'] * len(entry_ids))
            
            # Ordem das tabelas para manter integridade referencial
            tabelas = [
                "ServiceOperation", "ServiceOutput", "ServiceInput", "Service",
                "Dependencies", "Flow", "Deployment", "TempFields", "Entry"
            ]
            
            # Deletar das tabelas dependentes
            for tabela in tabelas:
                cursor.execute(
                    f"DELETE FROM {tabela} WHERE EntryId IN ({placeholders})",
                    entry_ids
                )
                print(f"  Deletados {cursor.rowcount} registros de {tabela}")
            
            # Finalmente deletar da tabela Item
            cursor.execute(
                f"DELETE FROM Item WHERE EntryId IN ({placeholders})",
                entry_ids
            )
            print(f"  Deletados {cursor.rowcount} registros de Item")
            
            conn.commit()
            
            elapsed = time.time() - start_time
            print(f"Deleção concluída em {elapsed:.1f} segundos")
            
    except Exception as e:
        print(f"Erro na deleção otimizada: {e}")
        conn.rollback()
        # Fallback para o método original se o otimizado falhar
        delete_old_data_fallback(conn, cod_onda)


def delete_old_data_fallback(conn, cod_onda: str):
    """Método fallback usando a lógica original"""
    print("Usando método de deleção fallback...")
    cursor = conn.cursor()
    
    tabelas = [
        "TempFields", "Deployment", "Flow", "Service", "ServiceInput",
        "ServiceOperation", "ServiceOutput", "Dependencies", "Entry", "Item"
    ]

    for tab in tabelas:
        if tab == "Item":
            cursor.execute(
                "DELETE FROM Item WHERE MID(Name,5,3) = %s", (cod_onda,)
            )
        else:
            cursor.execute(
                f"DELETE FROM {tab} WHERE EntryId IN (SELECT EntryId FROM Item WHERE MID(Name,5,3) = %s)", 
                (cod_onda,)
            )
    
    cursor.close()
    conn.commit()
    print("Deleção fallback concluída")


def process_xml_file(processor: BatchProcessor, file_path: str):
    """Processa um arquivo XML e adiciona dados ao processador em lote"""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        ns = {
            'aetgt': 'http://schemas.active-endpoints.com/appmodules/repository/2010/10/avrepository.xsd',
            'types1': 'http://schemas.active-endpoints.com/appmodules/repository/2010/10/avrepository.xsd',
            'ns0': 'http://schemas.active-endpoints.com/appmodules/screenflow/2010/10/avosScreenflow.xsd',
            'ns1': 'http://schemas.active-endpoints.com/appmodules/screenflow/2021/04/taskflowModel.xsd',
            'ns2': 'http://schemas.active-endpoints.com/appmodules/screenflow/2011/06/avosHostEnvironment.xsd'
        }

        for item in root.findall('types1:Item', ns):
            try:
                # Processar Item
                entry_id = safe_text(item.find('types1:EntryId', ns))
                if not entry_id:  # Pular se não tiver EntryId
                    continue
                    
                name = safe_text(item.find('types1:Name', ns))
                mime_type = safe_text(item.find('types1:MimeType', ns))
                description = safe_text(item.find('types1:Description', ns))
                applies_to = safe_text(item.find('types1:AppliesTo', ns))
                tags = safe_text(item.find('types1:Tags', ns))
                version_label = safe_text(item.find('types1:VersionLabel', ns))
                state = safe_text(item.find('types1:State', ns))
                process_group = safe_text(item.find('types1:ProcessGroup', ns))
                created_by = safe_text(item.find('types1:CreatedBy', ns))

                creation_date = to_mysql_datetime(item.find('types1:CreationDate', ns).text if item.find('types1:CreationDate', ns) is not None else None)
                modified_by = safe_text(item.find('types1:ModifiedBy', ns))
                modification_date = to_mysql_datetime(item.find('types1:ModificationDate', ns).text if item.find('types1:ModificationDate', ns) is not None else None)
                publication_status = safe_text(item.find('types1:PublicationStatus', ns))
                published_by = safe_text(item.find('types1:PublishedBy', ns))
                publication_date = to_mysql_datetime(item.find('types1:PublicationDate', ns).text if item.find('types1:PublicationDate', ns) is not None else None)
                published_contribution_id = safe_text(item.find('types1:PublishedContributionId', ns))
                guid = safe_text(item.find('types1:GUID', ns))
                display_name = safe_text(item.find('types1:DisplayName', ns))
                current_server_date_time = to_mysql_datetime(root.find('types1:CurrentServerDateTime', ns).text if root.find('types1:CurrentServerDateTime', ns) is not None else None)

                # Adicionar ao batch do Item
                processor.add_item((
                    entry_id, name, mime_type, description, applies_to, tags, 
                    version_label, state, process_group, created_by, creation_date,
                    modified_by, modification_date, publication_status, published_by,
                    publication_date, published_contribution_id, guid, display_name,
                    current_server_date_time
                ))

                # Processar Entry
                entry = item.find('types1:Entry', ns)
                if entry is not None:
                    taskflow = entry.find('ns0:taskflow', ns)
                    if taskflow is not None:
                        entry_display_name = taskflow.get('displayName') if taskflow is not None else ''
                        entry_name = taskflow.get('name') if taskflow is not None else ''
                        entry_override_api_name = 0
                        if taskflow is not None:
                            val = taskflow.get('overrideAPIName')
                            if val is not None:
                                entry_override_api_name = 1 if val.lower() == 'true' else 0
                        entry_description = safe_text(taskflow.find('ns0:description', ns)) if taskflow is not None else ''
                        entry_tags = safe_text(taskflow.find('ns0:tags', ns)) if taskflow is not None else ''
                        entry_generator = safe_text(taskflow.find('ns0:generator', ns)) if taskflow is not None else ''

                        processor.add_entry((
                            entry_id, entry_display_name, entry_name, entry_override_api_name,
                            entry_description, entry_tags, entry_generator
                        ))

                        # Processar TempFields
                        temp_fields = taskflow.find('ns0:tempFields', ns)
                        if temp_fields is not None:
                            for field in temp_fields.findall('ns0:field', ns):
                                field_description = field.get('description', '') or ''
                                field_name = field.get('name') or ''
                                field_type = field.get('type') or ''
                                options = []
                                options_element = field.find('ns0:options', ns)
                                if options_element is not None:
                                    for option in options_element.findall('ns0:option', ns):
                                        option_name = option.get('name') or ''
                                        option_value = option.text or ''
                                        options.append(f"{option_name}: {option_value}")
                                options_str = '; '.join(options)

                                processor.add_tempfields((
                                    entry_id, field_description, field_name, field_type, options_str
                                ))
                        
                        # Processar Deployment
                        deployment = taskflow.find('ns0:deployment', ns)
                        if deployment is not None:
                            skip_if_running = 1 if deployment.get('skipIfRunning') == 'true' else 0
                            suspend_on_fault = 1 if deployment.get('suspendOnFault') == 'true' else 0
                            tracing_level = safe_text(deployment.find('ns0:tracingLevel', ns)) if deployment.find('ns0:tracingLevel', ns) is not None else ''

                            allowed_groups = []
                            allowed_groups_elem = deployment.find('ns0:allowedGroups', ns)
                            if allowed_groups_elem is not None:
                                for group in allowed_groups_elem.findall('ns0:group', ns):
                                    if group.text is not None:
                                        allowed_groups.append(group.text.strip())
                            allowed_groups_str = ', '.join(allowed_groups) if allowed_groups else ''

                            processor.add_deployment((
                                entry_id, skip_if_running, suspend_on_fault, tracing_level, allowed_groups_str
                            ))

                        # Processar Flow
                        flow = taskflow.find('ns0:flow', ns)
                        if flow is not None:
                            flow_id = flow.get('id') or ''
                            start = flow.find('ns0:start', ns)
                            start_id = start.get('id') if start is not None else ''
                            start_target_id = ''
                            if start is not None:
                                link = start.find('ns0:link', ns)
                                if link is not None:
                                    start_target_id = link.get('targetId') or ''

                            processor.add_flow((
                                entry_id, flow_id, start_id, start_target_id
                            ))

                            # Processar Services dentro do Flow
                            for event_container in flow.findall('ns0:eventContainer', ns):
                                for service in event_container.findall('ns0:service', ns):
                                    service_id = service.get('id') or ''
                                    title = safe_text(service.find('ns0:title', ns))
                                    service_name = safe_text(service.find('ns0:serviceName', ns))
                                    service_guid = safe_text(service.find('ns0:serviceGUID', ns))

                                    processor.add_service((
                                        entry_id, service_id, title, service_name, service_guid
                                    ))

                                    # ServiceInput
                                    service_input = service.find('ns0:serviceInput', ns)
                                    if service_input is not None:
                                        for parameter in service_input.findall('ns0:parameter', ns):
                                            param_name = parameter.get('name') or ''
                                            source = parameter.get('source') or ''
                                            value = parameter.text or ''

                                            processor.add_serviceinput((
                                                entry_id, service_id, param_name, source, value
                                            ))

                                            # ServiceOperation dentro do parameter
                                            for operation in parameter.findall('ns0:operation', ns):
                                                operation_source = operation.get('source') or ''
                                                operation_target = operation.get('to') or ''
                                                operation_value = operation.text or ''

                                                processor.add_serviceoperation((
                                                    entry_id, service_id, param_name, 
                                                    operation_source, operation_target, operation_value
                                                ))

                                    # ServiceOutput
                                    service_output = service.find('ns0:serviceOutput', ns)
                                    if service_output is not None:
                                        for operation in service_output.findall('ns0:operation', ns):
                                            operation_source = operation.get('source') or ''
                                            operation_target = operation.get('to') or ''
                                            value = operation.text or ''

                                            processor.add_serviceoutput((
                                                entry_id, service_id, operation_source, operation_target, value
                                            ))

                        # Processar Dependencies
                        dependencies = taskflow.find('ns0:dependencies', ns)
                        if dependencies is not None:
                            for process_object in dependencies.findall('ns2:processObject', ns):
                                process_object_name = process_object.get('name') or ''

                                processor.add_dependencies((
                                    entry_id, process_object_name
                                ))

            except Exception as e:
                print(f"Erro ao processar item em {file_path}: {e}")
                continue

    except Exception as e:
        print(f"Erro ao processar o arquivo {file_path}: {e}")
        return


def main():
    if len(sys.argv) != 2:
        print("Uso: python 61_ExtrairXML.py <NOME_DA_PASTA>")
        sys.exit(1)

    pasta = sys.argv[1]
    CodOnda = pasta[:3]

    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', '/engenharia/config/config.json')
    config = load_config(config_path)

    try:
        json_file_path = config["json_file_path"] + f'/{pasta}/Explore/{pasta}/Geral'
    except KeyError as e:
        print(f"Error: Missing key in config file. Details: {e}")
        sys.exit(1)

    # Conectar ao banco
    print("Conectando ao banco de dados...")
    conn = mysql_connect()
    
    try:
        # 1. Deletar dados antigos (forma otimizada)
        delete_old_data_optimized(conn, CodOnda)
        
        # 2. Processar arquivos XML com processamento em lote
        print(f"\nIniciando processamento dos arquivos da pasta: {json_file_path}")
        processor = BatchProcessor(conn, batch_size=500)
        
        # Listar todos os arquivos XML
        xml_files = [f for f in os.listdir(json_file_path) if f.endswith('.xml')]
        total_files = len(xml_files)
        
        if total_files == 0:
            print("Nenhum arquivo XML encontrado!")
            conn.close()
            return
        
        print(f"Total de arquivos XML: {total_files}")
        
        start_time = time.time()
        files_processed = 0
        
        # Processar cada arquivo
        for filename in xml_files:
            file_path = os.path.join(json_file_path, filename)
            print(f"Processando: {filename}")
            
            file_start = time.time()
            process_xml_file(processor, file_path)
            files_processed += 1
            
            # Fazer flush a cada 5 arquivos para não consumir muita memória
            if files_processed % 5 == 0:
                print(f"  Flush periódico após {files_processed} arquivos...")
                processor.flush_all()
        
        # Flush final para garantir que todos os dados sejam inseridos
        print("Flush final...")
        processor.flush_all()
        
        # Restaurar configurações do MySQL
        with conn.cursor() as cursor:
            cursor.execute('SET SESSION unique_checks = 1')
            cursor.execute('SET SESSION foreign_key_checks = 1')
        conn.commit()
        
        elapsed = time.time() - start_time
        print(f"\nProcessamento concluído!")
        print(f"Arquivos processados: {total_files}")
        print(f"Tempo total: {elapsed:.1f} segundos")
        
        processor.print_stats()
        
    except Exception as e:
        print(f"Erro durante o processamento: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()
        print("Conexão com o banco encerrada.")


if __name__ == "__main__":
    main()