#!/usr/bin/env python3
"""
Script para processar parameters das tasks do IICS
Carrega dados na tabela fingerhard.s_task_parameters
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
    
    # Nome da tabela
    TABLE_NAME = "fingerhard.s_task_parameters"
    
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
    
    def truncate_table(self) -> bool:
        """
        Remove todos os registros da tabela (TRUNCATE - mais rápido)
        
        Returns:
            bool: True se truncou com sucesso
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute(f"TRUNCATE TABLE {self.TABLE_NAME}")
            print(f"✓ Tabela {self.TABLE_NAME} truncada com sucesso.")
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao truncar tabela: {e}")
            return False
    
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


class TaskParametersProcessor:
    """Processador de parameters das tasks"""
    
    # Commit a cada N registros
    COMMIT_INTERVAL = 100
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Inicializa o processador
        
        Args:
            db_manager: Instância do gerenciador de banco de dados
        """
        self.db_manager = db_manager
        self.total_inseridos = 0
        self.total_parameters = 0
    
    def clear_table(self) -> bool:
        """
        Limpa a tabela antes de inserir novos dados usando TRUNCATE
        
        Returns:
            bool: True se limpou com sucesso
        """
        print("\n🗑️  Limpando tabela antes da nova carga...")
        print(f"   Tabela: {self.db_manager.TABLE_NAME}")
        
        # Usar TRUNCATE para limpar a tabela
        result = self.db_manager.truncate_table()
        
        if result:
            print("   ✅ Tabela limpa com sucesso!")
        else:
            print("   ❌ Falha ao limpar tabela!")
        
        return result
    
    def get_tasks(self) -> List[Dict]:
        """
        Busca tasks da tabela s_task com os dados mais recentes
        
        Returns:
            Lista de dicionários com as tasks
        """
        query = """
            SELECT 
                a.CodOnda, 
                a.CodProcess, 
                a.name, 
                a.parameters,
                a.Dt_Inserted
            FROM 
                fingerhard.s_task a
            WHERE 
                a.name IN (
                    SELECT 
                        name 
                    FROM 
                        fingerhard.s_task 
                    WHERE 
                        DATE(Dt_Updated) = (
                            SELECT 
                                MAX(DATE(Dt_Updated)) 
                            FROM 
                                fingerhard.s_task
                        )
                )
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query)
                tasks = cursor.fetchall()
            print(f"✓ Tasks encontradas: {len(tasks)} registro(s)")
            return tasks
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao buscar tasks: {e}")
            return []
    
    def parse_parameters(self, parameters_json: str, task_name: str) -> Optional[List[Dict]]:
        """
        Parse do JSON de parameters
        
        Args:
            parameters_json: String JSON dos parameters
            task_name: Nome da task para logging
            
        Returns:
            Lista de parameters ou None em caso de erro
        """
        if not parameters_json or not parameters_json.strip():
            return None
        
        try:
            parameters = json.loads(parameters_json)
            
            # Se for um dicionário, converte para lista
            if isinstance(parameters, dict):
                parameters = [parameters]
            
            # Se for uma lista, retorna
            if isinstance(parameters, list):
                return parameters
            else:
                print(f"⚠️  Formato inesperado de parameters para task {task_name}")
                return None
                
        except json.JSONDecodeError as e:
            print(f"❌ Erro ao decodificar JSON para task {task_name}: {e}")
            return None
    
    def convert_to_int(self, value: Any) -> int:
        """
        Converte um valor para inteiro (0 ou 1) para campos booleanos
        
        Args:
            value: Valor a ser convertido
            
        Returns:
            int: 1 se True, 0 caso contrário
        """
        if isinstance(value, bool):
            return 1 if value else 0
        elif isinstance(value, (int, float)):
            return 1 if value else 0
        elif isinstance(value, str):
            return 1 if value.lower() in ['true', '1', 'yes'] else 0
        else:
            return 0
    
    def insert_parameter(self, parameter_data: Dict) -> bool:
        """
        Insere um parameter na tabela
        
        Args:
            parameter_data: Dicionário com os dados do parâmetro
            
        Returns:
            bool: True se inseriu com sucesso
        """
        query = """
            INSERT INTO fingerhard.s_task_parameters (
                CodOnda, CodProcess, task_name, id, name, type, label,
                uiProperties, sourceConnectionId, newFlatFile, newObject,
                showBusinessNames, naturalOrder, truncateTarget, bulkApiDBTarget,
                srcFFAttrs, customFuncCfg, targetRefsV2, targetUpdateColumns,
                extendedObject, runtimeAttrs, isRESTModernSource, isFileList,
                handleSpecialChars, handleDecimalRoundOff, frsAsset, dynamicFileName,
                excludeDynamicFileNameField, currentlyProcessedFileName, retainFieldMetadata,
                useExactSrcNames, tgtObjectAttributes, runtimeParameterData,
                overridableProperties, overriddenFields, Dt_Inserted, Dt_updated
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (
                    parameter_data['cod_onda'],
                    parameter_data['cod_process'],
                    parameter_data['task_name'],
                    parameter_data['id'],
                    parameter_data['name'],
                    parameter_data['type'],
                    parameter_data['label'],
                    parameter_data['ui_properties'],
                    parameter_data['source_connection_id'],
                    parameter_data['new_flat_file'],
                    parameter_data['new_object'],
                    parameter_data['show_business_names'],
                    parameter_data['natural_order'],
                    parameter_data['truncate_target'],
                    parameter_data['bulk_api_db_target'],
                    parameter_data['src_ff_attrs'],
                    parameter_data['custom_func_cfg'],
                    parameter_data['target_refs_v2'],
                    parameter_data['target_update_columns'],
                    parameter_data['extended_object'],
                    parameter_data['runtime_attrs'],
                    parameter_data['is_rest_modern_source'],
                    parameter_data['is_file_list'],
                    parameter_data['handle_special_chars'],
                    parameter_data['handle_decimal_round_off'],
                    parameter_data['frs_asset'],
                    parameter_data['dynamic_file_name'],
                    parameter_data['exclude_dynamic_file_name_field'],
                    parameter_data['currently_processed_file_name'],
                    parameter_data['retain_field_metadata'],
                    parameter_data['use_exact_src_names'],
                    parameter_data['tgt_object_attributes'],
                    parameter_data['runtime_parameter_data'],
                    parameter_data['overridable_properties'],
                    parameter_data['overridden_fields'],
                    parameter_data['dt_inserted'],
                    parameter_data['dt_updated']
                ))
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao inserir parâmetro '{parameter_data['name']}': {e}")
            return False
    
    def commit_if_needed(self):
        """Commit a cada COMMIT_INTERVAL registros"""
        if self.total_inseridos > 0 and self.total_inseridos % self.COMMIT_INTERVAL == 0:
            self.db_manager.commit()
            print(f"   💾 Commit realizado: {self.total_inseridos} registros inseridos até agora")
    
    def process_task(self, task: Dict) -> Tuple[int, int]:
        """
        Processa uma task, extraindo e inserindo seus parameters
        
        Args:
            task: Dicionário com dados da task
            
        Returns:
            Tuple (total_parameters, parameters_inseridos)
        """
        cod_onda = task['CodOnda']
        cod_process = task['CodProcess']
        task_name = task['name']
        parameters_json = task['parameters']
        task_dt_inserted = task['Dt_Inserted']
        
        # Parse dos parameters
        parameters = self.parse_parameters(parameters_json, task_name)
        
        if not parameters:
            return 0, 0
        
        total = len(parameters)
        inseridos = 0
        
        for param in parameters:
            # Extrair dados do parâmetro
            param_id = param.get('id', '')
            name = param.get('name', '')
            type_ = param.get('type', '')
            label = param.get('label', '')
            
            # Converter campos para JSON
            ui_properties = json.dumps(param.get('uiProperties', {}))
            src_ff_attrs = json.dumps(param.get('srcFFAttrs', {}))
            custom_func_cfg = json.dumps(param.get('customFuncCfg', {}))
            target_refs_v2 = json.dumps(param.get('targetRefsV2', {}))
            target_update_columns = json.dumps(param.get('targetUpdateColumns', []))
            extended_object = json.dumps(param.get('extendedObject', {}))
            runtime_attrs = json.dumps(param.get('runtimeAttrs', {}))
            tgt_object_attributes = json.dumps(param.get('tgtObjectAttributes', {}))
            runtime_parameter_data = json.dumps(param.get('runtimeParameterData', {}))
            overridable_properties = json.dumps(param.get('overridableProperties', []))
            overridden_fields = json.dumps(param.get('overriddenFields', []))
            
            # Converter campos booleanos para inteiro
            new_flat_file = self.convert_to_int(param.get('newFlatFile', False))
            new_object = self.convert_to_int(param.get('newObject', False))
            show_business_names = self.convert_to_int(param.get('showBusinessNames', False))
            natural_order = self.convert_to_int(param.get('naturalOrder', False))
            truncate_target = self.convert_to_int(param.get('truncateTarget', False))
            bulk_api_db_target = self.convert_to_int(param.get('bulkApiDBTarget', False))
            is_rest_modern_source = self.convert_to_int(param.get('isRESTModernSource', False))
            is_file_list = self.convert_to_int(param.get('isFileList', False))
            handle_special_chars = self.convert_to_int(param.get('handleSpecialChars', False))
            handle_decimal_round_off = self.convert_to_int(param.get('handleDecimalRoundOff', False))
            frs_asset = self.convert_to_int(param.get('frsAsset', False))
            dynamic_file_name = self.convert_to_int(param.get('dynamicFileName', False))
            exclude_dynamic_file_name_field = self.convert_to_int(param.get('excludeDynamicFileNameField', False))
            currently_processed_file_name = self.convert_to_int(param.get('currentlyProcessedFileName', False))
            retain_field_metadata = self.convert_to_int(param.get('retainFieldMetadata', False))
            use_exact_src_names = self.convert_to_int(param.get('useExactSrcNames', False))
            
            # Preparar dados para inserção
            parameter_data = {
                'cod_onda': cod_onda,
                'cod_process': cod_process,
                'task_name': task_name,
                'id': param_id,
                'name': name,
                'type': type_,
                'label': label,
                'ui_properties': ui_properties,
                'source_connection_id': param.get('sourceConnectionId', ''),
                'new_flat_file': new_flat_file,
                'new_object': new_object,
                'show_business_names': show_business_names,
                'natural_order': natural_order,
                'truncate_target': truncate_target,
                'bulk_api_db_target': bulk_api_db_target,
                'src_ff_attrs': src_ff_attrs,
                'custom_func_cfg': custom_func_cfg,
                'target_refs_v2': target_refs_v2,
                'target_update_columns': target_update_columns,
                'extended_object': extended_object,
                'runtime_attrs': runtime_attrs,
                'is_rest_modern_source': is_rest_modern_source,
                'is_file_list': is_file_list,
                'handle_special_chars': handle_special_chars,
                'handle_decimal_round_off': handle_decimal_round_off,
                'frs_asset': frs_asset,
                'dynamic_file_name': dynamic_file_name,
                'exclude_dynamic_file_name_field': exclude_dynamic_file_name_field,
                'currently_processed_file_name': currently_processed_file_name,
                'retain_field_metadata': retain_field_metadata,
                'use_exact_src_names': use_exact_src_names,
                'tgt_object_attributes': tgt_object_attributes,
                'runtime_parameter_data': runtime_parameter_data,
                'overridable_properties': overridable_properties,
                'overridden_fields': overridden_fields,
                'dt_inserted': task_dt_inserted,  # Usando a data da task
                'dt_updated': self.db_manager.date_ins
            }
            
            if self.insert_parameter(parameter_data):
                inseridos += 1
                self.total_inseridos += 1
                
                # Commit a cada COMMIT_INTERVAL registros
                self.commit_if_needed()
        
        if total > 0:
            print(f"   📊 {task_name}: {inseridos}/{total} parameters (Dt_Inserted: {task_dt_inserted})")
        
        return total, inseridos
    
    def process(self) -> bool:
        """
        Executa o processamento completo
        
        Returns:
            bool: True se processou com sucesso
        """
        print("\n📌 Processando parameters das tasks")
        print("-" * 60)
        
        # Limpar tabela antes de inserir novos dados
        if not self.clear_table():
            print("❌ Erro ao limpar tabela. Abortando...")
            return False
        
        # Commit após truncate
        self.db_manager.commit()
        
        # Buscar tasks
        tasks = self.get_tasks()
        
        if not tasks:
            print("⚠️  Nenhuma task encontrada para processar")
            return True
        
        # Processar cada task
        total_parameters = 0
        total_inseridos = 0
        
        print(f"\n📦 Processando tasks (commit a cada {self.COMMIT_INTERVAL} registros)...")
        
        for idx, task in enumerate(tasks, 1):
            print(f"\n   [{idx}/{len(tasks)}] Processando task: {task['name']}")
            total, inseridos = self.process_task(task)
            total_parameters += total
            total_inseridos += inseridos
        
        # Commit final dos registros restantes
        if self.total_inseridos > 0:
            self.db_manager.commit()
            print(f"\n   💾 Commit final: {self.total_inseridos} registros inseridos")
        
        print("\n" + "-" * 60)
        print(f"📊 Resumo do processamento:")
        print(f"   Total de tasks processadas: {len(tasks)}")
        print(f"   Total de parameters encontrados: {total_parameters}")
        print(f"   Total de parameters inseridos: {total_inseridos}")
        
        return total_inseridos > 0 or total_parameters == 0


class TaskParametersLoader:
    """Classe principal para carga de parameters das tasks"""
    
    def __init__(self):
        self.config = None
        self.db_manager = None
        self.processor = None
    
    def load_configuration(self):
        """Carrega as configurações do sistema"""
        self.config = ConfigLoader.load_config()
        # Este script não usa configurações específicas do JSON, apenas o banco
    
    def setup_database(self):
        """Configura a conexão com o banco de dados"""
        self.db_manager = DatabaseManager()
        if not self.db_manager.connect():
            sys.exit(1)
        
        self.processor = TaskParametersProcessor(self.db_manager)
    
    def run(self):
        """Executa o fluxo principal de processamento"""
        print("\n" + "=" * 60)
        print("🚀 TASK PARAMETERS LOADER - IICS")
        print("=" * 60)
        print("\n📌 Processando parameters para tabela fingerhard.s_task_parameters")
        
        # Carregar configurações
        self.load_configuration()
        
        # Configurar banco de dados
        self.setup_database()
        
        try:
            # Processar parameters
            success = self.processor.process()
            
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
    loader = TaskParametersLoader()
    loader.run()


if __name__ == "__main__":
    main()