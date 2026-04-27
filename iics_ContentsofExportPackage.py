#!/usr/bin/env python3
"""
Script para processar arquivos CSV do pacote de exportação do IICS
Carrega dados na tabela fingerhard.ContentsofExportPackage
"""

import os
import sys
import json
import argparse
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
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
                assunto = f"{result['cod_projeto']}_{result['nme_projeto']}"
                result['assunto'] = assunto
                
                print(f"✓ Projeto encontrado:")
                print(f"   Código: {result['cod_projeto']}")
                print(f"   Nome: {result['nme_projeto']}")
                print(f"   Assunto: {assunto}")
                
                return result
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
    
    def close(self):
        """Fecha a conexão com o banco"""
        if self.conn:
            self.conn.close()
            print("✓ Conexão com o banco de dados MySQL fechada.")


class CSVProcessor:
    """Processador de arquivos CSV do pacote de exportação"""
    
    # Tipos de objetos permitidos
    ALLOWED_OBJECT_TYPES = ['MTT', 'TASKFLOW', 'DTEMPLATE']
    
    # Códigos de onda válidos
    VALID_CODES = ['653', '673', '674', '684', '702']
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Inicializa o processador
        
        Args:
            db_manager: Instância do gerenciador de banco de dados
        """
        self.db_manager = db_manager
    
    def extract_codes_from_name(self, object_name: str, object_type: str) -> Tuple[Optional[int], Optional[int]]:
        """
        Extrai CodOnda e CodProcess do nome do objeto baseado no tipo
        
        Args:
            object_name: Nome do objeto
            object_type: Tipo do objeto (MTT, TASKFLOW, DTEMPLATE)
            
        Returns:
            Tuple (CodOnda, CodProcess)
        """
        parts = object_name.split('_')
        cod_onda = None
        cod_process = None
        
        if object_type == 'TASKFLOW':
            # TASKFLOW: formato esperado algo_CODONDA_CODPROCESS_...
            cod_onda = parts[1] if len(parts) > 1 else None
            cod_process = parts[2] if len(parts) > 2 else None
        else:
            # MTT, DTEMPLATE: formato esperado algo_CODONDA_CODPROCESS_...
            cod_onda = parts[3] if len(parts) > 3 else None
            cod_process = parts[4] if len(parts) > 4 else None
        
        # Validar se o código está na lista de válidos
        if cod_onda not in self.VALID_CODES:
            return 0, 0
        
        # Converter para inteiro
        try:
            cod_onda = int(cod_onda) if cod_onda else 0
            cod_process = int(cod_process) if cod_process else 0
        except (ValueError, TypeError):
            return 0, 0
        
        return cod_onda, cod_process
    
    def insert_record(self, record: Dict) -> bool:
        """
        Insere ou atualiza um registro na tabela ContentsofExportPackage
        
        Args:
            record: Dicionário com os dados do registro
            
        Returns:
            bool: True se inseriu/atualizou com sucesso
        """
        query = """
            INSERT INTO fingerhard.ContentsofExportPackage (
                id, CodOnda, CodProcess, objectPath, objectName, objectType, 
                Dt_Inserted, Dt_updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                objectPath = VALUES(objectPath),
                objectName = VALUES(objectName),
                Dt_updated = VALUES(Dt_updated)
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (
                    record['id'],
                    record['cod_onda'],
                    record['cod_process'],
                    record['object_path'],
                    record['object_name'],
                    record['object_type'],
                    record['dt_inserted'],
                    record['dt_updated']
                ))
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao inserir registro {record['id']}: {e}")
            return False
    
    def process_csv_file(self, file_path: str, filename: str) -> Tuple[int, int]:
        """
        Processa um arquivo CSV e insere os registros no banco
        
        Args:
            file_path: Caminho completo do arquivo CSV
            filename: Nome do arquivo
            
        Returns:
            Tuple (total_registros, registros_inseridos)
        """
        try:
            # Ler arquivo CSV com pandas
            df = pd.read_csv(file_path)
            print(f"   Total de registros no CSV: {len(df)}")
        except Exception as e:
            print(f"❌ Erro ao ler arquivo CSV '{filename}': {e}")
            return 0, 0
        
        # Adicionar campos de data
        now = datetime.now()
        df['Dt_Inserted'] = now
        df['Dt_updated'] = now
        
        total_registros = 0
        registros_inseridos = 0
        registros_ignorados = 0
        
        for index, row in df.iterrows():
            total_registros += 1
            
            object_type = row.get('objectType', '')
            object_name = row.get('objectName', '')
            
            # Verificar se o tipo é permitido
            if object_type not in self.ALLOWED_OBJECT_TYPES:
                registros_ignorados += 1
                continue
            
            # Extrair códigos do nome
            cod_onda, cod_process = self.extract_codes_from_name(object_name, object_type)
            
            # Preparar registro
            record = {
                'id': row.get('id'),
                'cod_onda': cod_onda,
                'cod_process': cod_process,
                'object_path': row.get('objectPath', ''),
                'object_name': object_name,
                'object_type': object_type,
                'dt_inserted': row.get('Dt_Inserted', now),
                'dt_updated': row.get('Dt_updated', now)
            }
            
            # Inserir registro
            if self.insert_record(record):
                registros_inseridos += 1
            
            # Mostrar progresso a cada 100 registros
            if registros_inseridos % 100 == 0 and registros_inseridos > 0:
                print(f"      Registros inseridos até agora: {registros_inseridos}")
        
        return total_registros, registros_inseridos, registros_ignorados
    
    def process_directory(self, directory: str) -> Tuple[int, int, int, int]:
        """
        Processa todos os arquivos CSV de um diretório
        
        Args:
            directory: Diretório com os arquivos CSV
            
        Returns:
            Tuple (total_arquivos, total_registros, total_inseridos, total_ignorados)
        """
        # Normalizar o caminho do diretório
        directory = str(directory).replace('\\', '/')
        
        print(f"\n📂 Diretório de origem: {directory}")
        print("-" * 60)
        
        # Verificar se diretório existe
        if not os.path.exists(directory):
            print(f"❌ Erro: Diretório '{directory}' não encontrado.")
            return 0, 0, 0, 0
        
        # Listar arquivos CSV
        try:
            arquivos_csv = [f for f in os.listdir(directory) if f.endswith('.csv')]
        except PermissionError:
            print(f"❌ Erro: Sem permissão para ler o diretório '{directory}'.")
            return 0, 0, 0, 0
        
        if not arquivos_csv:
            print(f"⚠️  Atenção: Nenhum arquivo CSV encontrado no diretório '{directory}'.")
            return 0, 0, 0, 0
        
        print(f"\n📁 Processando {len(arquivos_csv)} arquivo(s) CSV...")
        print("-" * 60)
        
        total_arquivos = len(arquivos_csv)
        total_registros = 0
        total_inseridos = 0
        total_ignorados = 0
        
        for filename in sorted(arquivos_csv):
            file_path = os.path.join(directory, filename)
            print(f"\n📄 Processando: {filename}")
            
            registros, inseridos, ignorados = self.process_csv_file(file_path, filename)
            total_registros += registros
            total_inseridos += inseridos
            total_ignorados += ignorados
            
            print(f"   Registros no arquivo: {registros}")
            print(f"   Registros inseridos: {inseridos}")
            print(f"   Registros ignorados (tipo inválido): {ignorados}")
            
            # Commit após cada arquivo
            self.db_manager.commit()
        
        return total_arquivos, total_registros, total_inseridos, total_ignorados


class ExportPackageLoader:
    """Classe principal para carga do pacote de exportação"""
    
    def __init__(self):
        self.config = None
        self.db_manager = None
        self.processor = None
    
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
        
        self.processor = CSVProcessor(self.db_manager)
    
    def run(self):
        """Executa o fluxo principal de processamento"""
        print("\n" + "=" * 60)
        print("🚀 EXPORT PACKAGE LOADER - IICS")
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
            directory = os.path.join(self.config['json_file_path'], assunto)
            
            print(f"\n📌 Processando assunto: {assunto}")
            print(f"📂 Caminho completo: {directory}")
            
            # Processar arquivos CSV
            total_arquivos, total_registros, total_inseridos, total_ignorados = self.processor.process_directory(directory)
            
            # Relatório final
            print("\n" + "=" * 60)
            print("📊 RESUMO FINAL DO PROCESSAMENTO")
            print("=" * 60)
            print(f"📄 Total de arquivos CSV processados: {total_arquivos}")
            print(f"📝 Total de registros encontrados: {total_registros}")
            print(f"✅ Total de registros inseridos/atualizados: {total_inseridos}")
            print(f"⏭️  Total de registros ignorados: {total_ignorados}")
            print(f"📁 Tabela destino: fingerhard.ContentsofExportPackage")
            print("=" * 60)
            
            if total_inseridos > 0:
                print("\n✨ Processamento concluído com SUCESSO!")
            elif total_registros == 0:
                print("\n⚠️  Nenhum registro foi encontrado!")
                sys.exit(1)
            else:
                print(f"\n⚠️  Nenhum registro foi inserido!")
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
    loader = ExportPackageLoader()
    loader.run()


if __name__ == "__main__":
    main()