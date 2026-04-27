#!/usr/bin/env python3
"""
Script para processar transformations a partir da tabela content
Carrega dados na tabela fingerhard.transformation
"""

import json
import os
import sys
from datetime import datetime, date, timedelta
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


class TimestampValidator:
    """Validador e conversor de timestamps"""
    
    DEFAULT_TIMESTAMP = "1990-01-01 00:00:00"
    
    @staticmethod
    def validate(value: Optional[str]) -> str:
        """
        Valida e retorna um timestamp correto ou o valor padrão
        
        Args:
            value: String do timestamp a ser validado
            
        Returns:
            Timestamp validado ou valor padrão
        """
        if not value or str(value).strip() == "":
            return TimestampValidator.DEFAULT_TIMESTAMP
        
        try:
            # Remove o sufixo 'Z' se existir (ISO 8601 UTC)
            value_str = str(value)
            if value_str.endswith('Z'):
                value_str = value_str[:-1]
            
            parsed = datetime.fromisoformat(value_str)
            return parsed.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError) as e:
            print(f"⚠️  Erro ao validar timestamp '{value}': {e}")
            return TimestampValidator.DEFAULT_TIMESTAMP
    
    @staticmethod
    def convert_to_brazil_time(utc_time_str: Optional[str]) -> str:
        """
        Converte data UTC para timezone do Brasil (UTC-3)
        
        Args:
            utc_time_str: String do timestamp UTC
            
        Returns:
            Timestamp no horário brasileiro ou valor padrão
        """
        if not utc_time_str or str(utc_time_str).strip() == "":
            return TimestampValidator.DEFAULT_TIMESTAMP
        
        try:
            # Converte string para datetime
            value_str = str(utc_time_str).replace("Z", "+00:00")
            utc_time = datetime.fromisoformat(value_str)
            brazil_time = utc_time - timedelta(hours=3)
            return brazil_time.strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError) as e:
            print(f"⚠️  Erro ao converter timestamp para horário Brasil: {e}")
            return TimestampValidator.DEFAULT_TIMESTAMP


class TransformationProcessor:
    """Processador de transformations"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Inicializa o processador
        
        Args:
            db_manager: Instância do gerenciador de banco de dados
        """
        self.db_manager = db_manager
        self.timestamp_validator = TimestampValidator()
    
    def copy_historical_records(self, codigo_onda: str) -> bool:
        """
        Copia registros históricos para tabela transformation_hist
        
        Args:
            codigo_onda: Código da onda
            
        Returns:
            bool: True se copiou com sucesso
        """
        query = """
            INSERT INTO fingerhard.transformation_hist
            SELECT DISTINCT t.*
            FROM fingerhard.transformation t
            JOIN fingerhard.transformation_file_records tfr 
                ON t.CodOnda = tfr.CodOnda AND t.CodProcess = tfr.CodProcess
            WHERE DATE(tfr.updated_time_brazil) = (
                SELECT MAX(DATE(updated_time_brazil)) 
                FROM fingerhard.transformation_file_records
            ) AND tfr.TYPE NOT IN ('IMAGE')
              AND t.CodOnda = %s
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (codigo_onda,))
                rows_copied = cursor.rowcount
            print(f"✓ Registros históricos copiados: {rows_copied} registros")
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao copiar registros históricos: {e}")
            return False
    
    def delete_onda_records(self, codigo_onda: str) -> bool:
        """
        Remove registros da onda na tabela principal
        
        Args:
            codigo_onda: Código da onda
            
        Returns:
            bool: True se removeu com sucesso
        """
        query = "DELETE FROM fingerhard.transformation WHERE CodOnda = %s"
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (codigo_onda,))
                rows_deleted = cursor.rowcount
            print(f"✓ Registros removidos da tabela principal: {rows_deleted} registros")
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao remover registros: {e}")
            return False
    
    def get_contents(self, codigo_onda: str) -> List[Dict]:
        """
        Busca conteúdos da tabela content
        
        Args:
            codigo_onda: Código da onda
            
        Returns:
            Lista de dicionários com os conteúdos
        """
        today = date.today().strftime('%Y-%m-%d')
        query = """
            SELECT DISTINCT 
                a.CodOnda, 
                a.CodProcess, 
                a.name, 
                a.transformations
            FROM fingerhard.content a
            WHERE a.CodOnda = %s 
              AND DATE(a.dt_updated) = %s
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (codigo_onda, today))
                contents = cursor.fetchall()
            print(f"✓ Conteúdos encontrados: {len(contents)} registro(s)")
            return contents
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao buscar conteúdos: {e}")
            return []
    
    def parse_transformation(self, transformation: Dict, content_name: str, 
                            cod_onda: str, cod_process: str) -> Optional[Dict]:
        """
        Converte um transformation do JSON para o formato de banco de dados
        
        Args:
            transformation: Dicionário do transformation
            content_name: Nome do conteúdo pai
            cod_onda: Código da onda
            cod_process: Código do processo
            
        Returns:
            Dict com dados processados ou None em caso de erro
        """
        try:
            # Validar campos obrigatórios
            transformation_id = transformation.get('$$ID')
            if not transformation_id:
                print(f"⚠️  Transformation sem $$ID no conteúdo {content_name}")
                return None
            
            # Extrair e validar timestamps
            create_time = transformation.get('createTime', '')
            validated_time = self.timestamp_validator.validate(create_time)
            brazil_time = self.timestamp_validator.convert_to_brazil_time(validated_time)
            
            # Converter campos booleanos
            generate_filename_port = transformation.get('generateFilenamePort') == "true"
            use_labels = transformation.get('useLabels') == "true"
            use_sequence_fields = transformation.get('useSequenceFields') == "true"
            
            # Converter campos para JSON string
            annotations = json.dumps(transformation.get('annotations', {}))
            advanced_properties = json.dumps(transformation.get('advancedProperties', {}))
            groups = json.dumps(transformation.get('groups', []))
            session_properties = json.dumps(transformation.get('sessionProperties', {}))
            fields = json.dumps(transformation.get('fields', []))
            data_adapter = json.dumps(transformation.get('dataAdapter', {}))
            
            # Processar input_sorted
            input_sorted = transformation.get('inputSorted', '')
            if isinstance(input_sorted, bool):
                input_sorted = str(input_sorted).lower()
            elif isinstance(input_sorted, (dict, list)):
                input_sorted = json.dumps(input_sorted)
            else:
                input_sorted = str(input_sorted).strip('"')
            
            now = datetime.now()
            
            return {
                'id': transformation_id,
                'class': transformation.get('$$class', ''),
                'annotations': annotations,
                'create_time': validated_time,
                'create_time_brazil': brazil_time,
                'name': transformation.get('name', ''),
                'input_sorted': input_sorted,
                'advanced_properties': advanced_properties,
                'groups': groups,
                'session_properties': session_properties,
                'generate_filename_port': generate_filename_port,
                'use_labels': use_labels,
                'use_sequence_fields': use_sequence_fields,
                'fields': fields,
                'data_adapter': data_adapter,
                'content_name': content_name,
                'cod_onda': cod_onda,
                'cod_process': cod_process,
                'dt_inserted': now,
                'dt_updated': now
            }
        except Exception as e:
            print(f"❌ Erro ao processar transformation: {e}")
            return None
    
    def insert_transformation(self, record: Dict) -> bool:
        """
        Insere ou atualiza um transformation na tabela
        
        Args:
            record: Dicionário com os dados do transformation
            
        Returns:
            bool: True se inseriu/atualizou com sucesso
        """
        query = """
            INSERT INTO fingerhard.transformation (
                id, class, annotations, create_time, create_time_brazil, name, 
                inputSorted, advanced_properties, _groups, session_properties, 
                generate_filename_port, use_labels, use_sequence_fields, fields, 
                data_adapter, content_name, CodOnda, CodProcess, Dt_Inserted, Dt_updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                content_name = VALUES(content_name),
                class = VALUES(class),
                annotations = VALUES(annotations),
                create_time = VALUES(create_time),
                create_time_brazil = VALUES(create_time_brazil),
                name = VALUES(name),
                inputSorted = VALUES(inputSorted),
                advanced_properties = VALUES(advanced_properties),
                _groups = VALUES(_groups),
                session_properties = VALUES(session_properties),
                generate_filename_port = VALUES(generate_filename_port),
                use_labels = VALUES(use_labels),
                use_sequence_fields = VALUES(use_sequence_fields),
                fields = VALUES(fields),
                data_adapter = VALUES(data_adapter),
                Dt_updated = VALUES(Dt_updated)
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (
                    record['id'],
                    record['class'],
                    record['annotations'],
                    record['create_time'],
                    record['create_time_brazil'],
                    record['name'],
                    record['input_sorted'],
                    record['advanced_properties'],
                    record['groups'],
                    record['session_properties'],
                    record['generate_filename_port'],
                    record['use_labels'],
                    record['use_sequence_fields'],
                    record['fields'],
                    record['data_adapter'],
                    record['content_name'],
                    record['cod_onda'],
                    record['cod_process'],
                    record['dt_inserted'],
                    record['dt_updated']
                ))
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao inserir transformation '{record['name']}': {e}")
            return False
    
    def process_content(self, content: Dict) -> Tuple[int, int]:
        """
        Processa um conteúdo, extraindo e inserindo seus transformations
        
        Args:
            content: Dicionário com dados do conteúdo
            
        Returns:
            Tuple (total_transformations, transformations_inseridos)
        """
        cod_onda = content['CodOnda']
        cod_process = content['CodProcess']
        content_name = content['name']
        transformations_json = content['transformations']
        
        # Parse do JSON de transformations
        try:
            transformations_list = json.loads(transformations_json)
            if not isinstance(transformations_list, list):
                transformations_list = [transformations_list]
        except json.JSONDecodeError as e:
            print(f"❌ Erro ao decodificar JSON para '{content_name}': {e}")
            return 0, 0
        
        total = len(transformations_list)
        inseridos = 0
        
        for transformation in transformations_list:
            parsed = self.parse_transformation(
                transformation, content_name, cod_onda, cod_process
            )
            
            if parsed and self.insert_transformation(parsed):
                inseridos += 1
        
        if total > 0:
            print(f"   📊 {content_name}: {inseridos}/{total} transformations inseridos")
        
        return total, inseridos
    
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
        
        # Copiar registros históricos
        if not self.copy_historical_records(codigo_onda):
            print("⚠️  Continuando mesmo com erro na cópia histórica...")
        
        # Remover registros existentes da onda
        if not self.delete_onda_records(codigo_onda):
            print("❌ Erro ao remover registros existentes. Abortando...")
            return False
        
        # Buscar conteúdos
        contents = self.get_contents(codigo_onda)
        
        if not contents:
            print("⚠️  Nenhum conteúdo encontrado para processar")
            return True
        
        # Processar cada conteúdo
        total_transformations = 0
        total_inseridos = 0
        
        print("\n📦 Processando conteúdos...")
        for content in contents:
            total, inseridos = self.process_content(content)
            total_transformations += total
            total_inseridos += inseridos
        
        print("\n" + "-" * 60)
        print(f"📊 Resumo do processamento:")
        print(f"   Total de transformations processados: {total_transformations}")
        print(f"   Total de transformations inseridos/atualizados: {total_inseridos}")
        
        return total_inseridos > 0 or total_transformations == 0


class TransformationLoader:
    """Classe principal para carga de transformations"""
    
    def __init__(self):
        self.config = None
        self.db_manager = None
        self.processor = None
    
    def load_configuration(self):
        """Carrega as configurações do sistema"""
        self.config = ConfigLoader.load_config()
        
        # Validar chave obrigatória
        if 'PathMapsGrava' not in self.config:
            print(f"❌ Erro: Chave 'PathMapsGrava' ausente no arquivo de configuração")
            sys.exit(1)
        
        print(f"\n✓ Configuração carregada:")
        print(f"   PathMapsGrava: {self.config['PathMapsGrava']}")
    
    def setup_database(self):
        """Configura a conexão com o banco de dados"""
        self.db_manager = DatabaseManager()
        if not self.db_manager.connect():
            sys.exit(1)
        
        self.processor = TransformationProcessor(self.db_manager)
    
    def run(self):
        """Executa o fluxo principal de processamento"""
        print("\n" + "=" * 60)
        print("🚀 TRANSFORMATION LOADER - IICS")
        print("=" * 60)
        print("\n📌 Processando transformations para tabela fingerhard.transformation")
        
        # Validar argumentos
        if len(sys.argv) != 2:
            print("\n❌ Uso correto:")
            print(f"  python {sys.argv[0]} <codigo_onda>")
            print("\nExemplo:")
            print(f"  python {sys.argv[0]} 653")
            print("=" * 60)
            sys.exit(1)
        
        codigo_onda = sys.argv[1]
        
        # Carregar configurações
        self.load_configuration()
        
        # Configurar banco de dados
        self.setup_database()
        
        try:
            # Processar transformations
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
    loader = TransformationLoader()
    loader.run()


if __name__ == "__main__":
    main()