#!/usr/bin/env python3
"""
Script para processar sessionPropertiesList das tasks do IICS
Carrega dados na tabela fingerhard.s_task_sessionPropertiesList
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
    TABLE_NAME = "fingerhard.s_task_sessionPropertiesList"
    
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
        Remove todos os registros da tabela (TRUNCATE)
        
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
    
    def delete_all_records(self) -> bool:
        """
        Remove todos os registros da tabela (DELETE - alternativa mais segura)
        
        Returns:
            bool: True se removeu com sucesso
        """
        try:
            with self.get_cursor() as cursor:
                cursor.execute(f"DELETE FROM {self.TABLE_NAME}")
                rows_deleted = cursor.rowcount
            print(f"✓ Registros removidos da tabela: {rows_deleted} registros")
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao remover registros: {e}")
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


class TaskSessionPropertiesProcessor:
    """Processador de sessionPropertiesList das tasks"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Inicializa o processador
        
        Args:
            db_manager: Instância do gerenciador de banco de dados
        """
        self.db_manager = db_manager
    
    def clear_table(self) -> bool:
        """
        Limpa a tabela antes de inserir novos dados
        
        Returns:
            bool: True se limpou com sucesso
        """
        print("\n🗑️  Limpando tabela antes da nova carga...")
        # Usando DELETE em vez de TRUNCATE para ter mais controle
        # e poder fazer rollback se necessário
        return self.db_manager.delete_all_records()
    
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
                a.sessionPropertiesList 
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
    
    def parse_session_properties(self, session_properties_json: str, task_name: str) -> Optional[List[Dict]]:
        """
        Parse do JSON de sessionPropertiesList
        
        Args:
            session_properties_json: String JSON das session properties
            task_name: Nome da task para logging
            
        Returns:
            Lista de session properties ou None em caso de erro
        """
        if not session_properties_json or not session_properties_json.strip():
            return None
        
        try:
            session_properties = json.loads(session_properties_json)
            
            # Se for um dicionário, converte para lista
            if isinstance(session_properties, dict):
                session_properties = [session_properties]
            
            # Se for uma lista, retorna
            if isinstance(session_properties, list):
                return session_properties
            else:
                print(f"⚠️  Formato inesperado de sessionPropertiesList para task {task_name}")
                return None
                
        except json.JSONDecodeError as e:
            print(f"❌ Erro ao decodificar JSON para task {task_name}: {e}")
            return None
    
    def insert_session_property(self, property_data: Dict) -> bool:
        """
        Insere uma session property na tabela
        
        Args:
            property_data: Dicionário com os dados da propriedade
            
        Returns:
            bool: True se inseriu com sucesso
        """
        query = """
            INSERT INTO fingerhard.s_task_sessionPropertiesList (
                CodOnda, CodProcess, task_name, name, value, recommended, 
                Dt_Inserted, Dt_Updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (
                    property_data['cod_onda'],
                    property_data['cod_process'],
                    property_data['task_name'],
                    property_data['name'],
                    property_data['value'],
                    property_data['recommended'],
                    property_data['dt_inserted'],
                    property_data['dt_updated']
                ))
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao inserir propriedade '{property_data['name']}': {e}")
            return False
    
    def process_task(self, task: Dict) -> Tuple[int, int]:
        """
        Processa uma task, extraindo e inserindo suas session properties
        
        Args:
            task: Dicionário com dados da task
            
        Returns:
            Tuple (total_properties, properties_inseridas)
        """
        cod_onda = task['CodOnda']
        cod_process = task['CodProcess']
        task_name = task['name']
        session_properties_json = task['sessionPropertiesList']
        
        # Parse das session properties
        session_properties = self.parse_session_properties(session_properties_json, task_name)
        
        if not session_properties:
            return 0, 0
        
        total = len(session_properties)
        inseridas = 0
        
        for prop in session_properties:
            name = prop.get('name', '')
            value = prop.get('value', '')
            
            # Converter recommended para inteiro (0 ou 1)
            recommended_raw = prop.get('recommended', False)
            if isinstance(recommended_raw, bool):
                recommended = 1 if recommended_raw else 0
            elif isinstance(recommended_raw, (int, float)):
                recommended = 1 if recommended_raw else 0
            elif isinstance(recommended_raw, str):
                recommended = 1 if recommended_raw.lower() in ['true', '1', 'yes'] else 0
            else:
                recommended = 0
            
            # Preparar dados para inserção
            property_data = {
                'cod_onda': cod_onda,
                'cod_process': cod_process,
                'task_name': task_name,
                'name': name,
                'value': value,
                'recommended': recommended,
                'dt_inserted': self.db_manager.date_ins,
                'dt_updated': self.db_manager.date_ins
            }
            
            if self.insert_session_property(property_data):
                inseridas += 1
        
        if total > 0:
            print(f"   📊 {task_name}: {inseridas}/{total} properties")
        
        return total, inseridas
    
    def process(self) -> bool:
        """
        Executa o processamento completo
        
        Returns:
            bool: True se processou com sucesso
        """
        print("\n📌 Processando session properties das tasks")
        print("-" * 60)
        
        # Limpar tabela antes de inserir novos dados
        if not self.clear_table():
            print("❌ Erro ao limpar tabela. Abortando...")
            return False
        
        # Buscar tasks
        tasks = self.get_tasks()
        
        if not tasks:
            print("⚠️  Nenhuma task encontrada para processar")
            return True
        
        # Processar cada task
        total_properties = 0
        total_inseridas = 0
        
        print("\n📦 Processando tasks...")
        for idx, task in enumerate(tasks, 1):
            print(f"\n   [{idx}/{len(tasks)}] Processando task: {task['name']}")
            total, inseridas = self.process_task(task)
            total_properties += total
            total_inseridas += inseridas
        
        print("\n" + "-" * 60)
        print(f"📊 Resumo do processamento:")
        print(f"   Total de tasks processadas: {len(tasks)}")
        print(f"   Total de session properties encontradas: {total_properties}")
        print(f"   Total de session properties inseridas: {total_inseridas}")
        
        return total_inseridas > 0 or total_properties == 0


class TaskSessionPropertiesLoader:
    """Classe principal para carga de session properties das tasks"""
    
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
        
        self.processor = TaskSessionPropertiesProcessor(self.db_manager)
    
    def run(self):
        """Executa o fluxo principal de processamento"""
        print("\n" + "=" * 60)
        print("🚀 TASK SESSION PROPERTIES LOADER - IICS")
        print("=" * 60)
        print("\n📌 Processando session properties para tabela fingerhard.s_task_sessionPropertiesList")
        
        # Carregar configurações
        self.load_configuration()
        
        # Configurar banco de dados
        self.setup_database()
        
        try:
            # Processar session properties
            success = self.processor.process()
            
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
            self.db_manager.rollback()
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            self.db_manager.close()


def main():
    """Função principal"""
    loader = TaskSessionPropertiesLoader()
    loader.run()


if __name__ == "__main__":
    main()