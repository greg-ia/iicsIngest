#!/usr/bin/env python3
"""
Script para processar arquivos fileRecord.json e inserir no banco de dados
Processa arquivos extraídos do IICS e carrega na tabela transformation_file_records
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

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
        config_path = os.getenv('CONFIG_PATH')
        
        if not config_path:
            print("❌ Error: Variável CONFIG_PATH não definida no .env")
            print("   Configure: CONFIG_PATH=/caminho/para/config.json")
            sys.exit(1)
        
        # Normalizar o caminho (converte barras invertidas para barras normais)
        config_path = str(config_path).replace('\\', '/')
        
        # Remove trailing slash se existir
        config_path = config_path.rstrip('/')
        
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


class TimeConverter:
    """Conversor de timezone"""
    
    @staticmethod
    def convert_to_brazil_time(epoch_time_ms: int) -> Optional[datetime]:
        """
        Converte timestamp em milissegundos para horário local brasileiro (UTC-3)
        
        Args:
            epoch_time_ms: Timestamp em milissegundos
            
        Returns:
            Datetime no horário brasileiro ou None em caso de erro
        """
        try:
            utc_time = datetime.fromtimestamp(epoch_time_ms / 1000, tz=timezone.utc)
            brazil_tz = timezone(timedelta(hours=-3))
            brazil_time = utc_time.astimezone(brazil_tz)
            return brazil_time
        except Exception as e:
            print(f"❌ Erro na conversão de horário: {e}")
            return None


class DatabaseManager:
    """Gerenciador de operações com banco de dados"""
    
    # Tabela fixa
    TABLE_NAME = "fingerhard.transformation_file_records"
    TABLE_HIST = "fingerhard.transformation_file_records_hist"
    
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
        
        # Validar credenciais
        missing = [k for k, v in self.config.items() if not v and k not in ['autocommit', 'charset', 'cursorclass']]
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
    
    def truncate_table(self) -> bool:
        """
        Trunca a tabela principal (remove todos os registros)
        
        Returns:
            bool: True se truncou com sucesso
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f'TRUNCATE TABLE {self.TABLE_NAME}')
            print(f"✓ Tabela {self.TABLE_NAME} truncada com sucesso.")
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao truncar tabela: {e}")
            return False
    
    def copy_historical_records(self, days_back: int = 3) -> bool:
        """
        Copia registros históricos para tabela de histórico
        
        Args:
            days_back: Dias para trás para copiar registros
            
        Returns:
            bool: True se copiou com sucesso
        """
        yesterday = datetime.today().date() - timedelta(days=days_back)
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO {self.TABLE_HIST}
                    SELECT DISTINCT
                        t.id, t.CodOnda, t.CodProcess, t.name, t.type, 
                        t.size, t.updated_time, t.updated_time_brazil
                    FROM {self.TABLE_NAME} t
                    WHERE DATE(t.updated_time_brazil) >= %s
                    AND t.type NOT IN ('IMAGE')
                    ON DUPLICATE KEY UPDATE 
                        name = VALUES(name),
                        type = VALUES(type),
                        size = VALUES(size),
                        updated_time = VALUES(updated_time),
                        updated_time_brazil = VALUES(updated_time_brazil);
                """, (yesterday,))
            print("✓ Registros históricos copiados com sucesso.")
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao copiar registros históricos: {e}")
            return False
    
    def insert_or_update_record(self, record: Dict) -> bool:
        """
        Insere ou atualiza um registro na tabela
        
        Args:
            record: Dicionário com os dados do registro
            
        Returns:
            bool: True se inseriu/atualizou com sucesso
        """
        query = f"""
            INSERT INTO {self.TABLE_NAME} (
                id, name, type, size, updated_time, updated_time_brazil, 
                CodOnda, CodProcess
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                name = VALUES(name),
                type = VALUES(type),
                size = VALUES(size),
                updated_time = VALUES(updated_time),
                updated_time_brazil = VALUES(updated_time_brazil);
        """
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (
                    record['id'],
                    record['name'],
                    record['type'],
                    record['size'],
                    record['updated_time'],
                    record['updated_time_brazil'],
                    record['cod_onda'],
                    record['cod_process']
                ))
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao inserir registro {record['id']}: {e}")
            return False
    
    def commit(self):
        """Confirma as alterações no banco"""
        if self.conn:
            self.conn.commit()
    
    def close(self):
        """Fecha a conexão com o banco"""
        if self.conn:
            self.conn.close()
            print("✓ Conexão com o banco de dados MySQL fechada.")


class FileRecordProcessor:
    """Processador de arquivos fileRecord.json"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Inicializa o processador
        
        Args:
            db_manager: Instância do gerenciador de banco de dados
        """
        self.db_manager = db_manager
        self.time_converter = TimeConverter()
        
    def extract_codes_from_name(self, name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extrai CodOnda e CodProcess do nome do arquivo
        
        Formato esperado: algo_CODONDA_CODPROCESS_...
        
        Args:
            name: Nome do arquivo
            
        Returns:
            Tuple (CodOnda, CodProcess)
        """
        parts = name.split('_')
        cod_onda = parts[3] if len(parts) > 3 else None
        cod_process = parts[4] if len(parts) > 4 else None
        return cod_onda, cod_process
    
    def parse_record(self, record: Dict, filename: str) -> Optional[Dict]:
        """
        Converte um registro JSON para o formato de banco de dados
        
        Args:
            record: Registro do JSON
            filename: Nome do arquivo de origem (para logging)
            
        Returns:
            Dict com dados processados ou None em caso de erro
        """
        # Verificar se é um fileRecord
        if record.get('@type') != 'fileRecord':
            return None
        
        try:
            # Extrair informações básicas
            record_id_str = record.get('id', '0')
            # Remove o '@' do início se existir
            if record_id_str.startswith('@'):
                record_id_str = record_id_str[1:]
            record_id = int(record_id_str)
            
            name = record.get('name', '')
            type_ = record.get('type', '')
            size = record.get('size', 0)
            
            # Processar timestamps
            attach_time = record.get('attachTime', 0)
            updated_time = datetime.utcfromtimestamp(attach_time / 1000)
            updated_time_brazil = self.time_converter.convert_to_brazil_time(attach_time)
            
            # Extrair códigos do nome
            cod_onda, cod_process = self.extract_codes_from_name(name)
            
            return {
                'id': record_id,
                'name': name,
                'type': type_,
                'size': size,
                'updated_time': updated_time,
                'updated_time_brazil': updated_time_brazil,
                'cod_onda': cod_onda,
                'cod_process': cod_process
            }
        except Exception as e:
            print(f"❌ Erro ao processar registro do arquivo '{filename}': {e}")
            return None
    
    def process_file(self, file_path: str, filename: str) -> Tuple[int, int, int]:
        """
        Processa um arquivo JSON e insere registros no banco
        
        Args:
            file_path: Caminho completo do arquivo
            filename: Nome do arquivo
            
        Returns:
            Tuple (total_registros_lidos, total_fileRecords_encontrados, registros_inseridos)
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        except json.JSONDecodeError as e:
            print(f"❌ Erro ao carregar o arquivo JSON '{filename}': {e}")
            return 0, 0, 0
        except Exception as e:
            print(f"❌ Erro ao ler o arquivo '{filename}': {e}")
            return 0, 0, 0
        
        # Verificar se o JSON é uma lista ou um objeto
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            # Se for um dicionário, pode ser um único registro ou ter uma chave 'records'
            if '@type' in data and data.get('@type') == 'fileRecord':
                records = [data]
            elif 'records' in data:
                records = data['records']
            else:
                records = [data]
        else:
            print(f"⚠️  Formato JSON não reconhecido no arquivo '{filename}'")
            return 0, 0, 0
        
        total_registros = len(records)
        file_records_encontrados = 0
        registros_inseridos = 0
        
        for record in records:
            parsed_record = self.parse_record(record, filename)
            
            if parsed_record:
                file_records_encontrados += 1
                if self.db_manager.insert_or_update_record(parsed_record):
                    registros_inseridos += 1
        
        return total_registros, file_records_encontrados, registros_inseridos
    
    def process_directory(self, directory: str) -> bool:
        """
        Processa todos os arquivos JSON de um diretório
        
        Args:
            directory: Diretório com os arquivos JSON
            
        Returns:
            bool: True se processou com sucesso
        """
        # Normalizar o caminho do diretório
        directory = str(directory).replace('\\', '/')
        
        print(f"\n📂 Diretório de origem: {directory}")
        print("-" * 60)
        
        # Listar arquivos JSON
        try:
            arquivos_json = [f for f in os.listdir(directory) if f.endswith('.json')]
        except FileNotFoundError:
            print(f"❌ Erro: Diretório '{directory}' não encontrado.")
            return False
        except PermissionError:
            print(f"❌ Erro: Sem permissão para ler o diretório '{directory}'.")
            return False
        
        if not arquivos_json:
            print(f"⚠️  Atenção: Nenhum arquivo JSON encontrado no diretório '{directory}'.")
            return True
        
        print(f"\n📁 Processando {len(arquivos_json)} arquivo(s) JSON...")
        print("-" * 60)
        
        total_registros_lidos = 0
        total_file_records = 0
        total_inseridos = 0
        
        for filename in sorted(arquivos_json):
            file_path = os.path.join(directory, filename)
            print(f"\n📄 Processando: {filename}")
            
            registros_lidos, file_records, inseridos = self.process_file(file_path, filename)
            total_registros_lidos += registros_lidos
            total_file_records += file_records
            total_inseridos += inseridos
            
            print(f"   Total de registros no JSON: {registros_lidos}")
            print(f"   Registros do tipo fileRecord: {file_records}")
            print(f"   Registros inseridos/atualizados: {inseridos}")
        
        print("\n" + "=" * 60)
        print("📊 RESUMO FINAL DO PROCESSAMENTO")
        print("=" * 60)
        print(f"📄 Total de arquivos processados: {len(arquivos_json)}")
        print(f"📝 Total de registros lidos: {total_registros_lidos}")
        print(f"✅ Total de fileRecords encontrados: {total_file_records}")
        print(f"💾 Total de registros inseridos/atualizados: {total_inseridos}")
        print(f"📁 Tabela destino: {DatabaseManager.TABLE_NAME}")
        print("=" * 60)
        
        return True


class FileRecordLoader:
    """Classe principal para carga de fileRecords"""
    
    def __init__(self):
        self.config = None
        self.db_manager = None
        self.processor = None
        
    def load_configuration(self):
        """Carrega as configurações do sistema"""
        self.config = ConfigLoader.load_config()
        
        # Validar chave obrigatória
        if 'PathFileRecordsGrava' not in self.config:
            print(f"❌ Erro: Chave 'PathFileRecordsGrava' ausente no arquivo de configuração")
            sys.exit(1)
        
        print(f"\n✓ Configuração carregada:")
        print(f"   Diretório de origem: {self.config['PathFileRecordsGrava']}")
    
    def setup_database(self):
        """Configura a conexão com o banco de dados"""
        self.db_manager = DatabaseManager()
        if not self.db_manager.connect():
            sys.exit(1)
        
        self.processor = FileRecordProcessor(self.db_manager)
    
    def run(self):
        """Executa o fluxo principal de processamento"""
        print("\n" + "=" * 60)
        print("🚀 FILE RECORD LOADER - IICS")
        print("=" * 60)
        print("\n📌 Processando arquivos para tabela transformation_file_records")
        
        # Carregar configurações
        self.load_configuration()
        
        # Configurar banco de dados
        self.setup_database()
        
        try:
            # Copiar registros históricos
            print("\n📋 Copiando registros históricos...")
            if not self.db_manager.copy_historical_records():
                print("⚠️  Continuando mesmo com erro na cópia histórica...")
            
            # Truncar tabela principal
            print("\n🗑️  Preparando tabela para nova carga...")
            if not self.db_manager.truncate_table():
                print("❌ Erro ao truncar tabela. Abortando...")
                sys.exit(1)
            
            # Processar arquivos JSON
            directory = self.config["PathFileRecordsGrava"]
            success = self.processor.process_directory(directory)
            
            # Commit final
            self.db_manager.commit()
            
            if success:
                print("\n✨ Processamento concluído com SUCESSO!")
            else:
                print("\n⚠️  Processamento concluído com falhas!")
                sys.exit(1)
            
        except Exception as e:
            print(f"\n❌ Erro inesperado: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            self.db_manager.close()


def main():
    """Função principal"""
    loader = FileRecordLoader()
    loader.run()

if __name__ == "__main__":
    main()