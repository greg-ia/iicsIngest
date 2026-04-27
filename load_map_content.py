#!/usr/bin/env python3
"""
Script para processar arquivos JSON de conteúdo (content) e inserir na tabela fingerhard.content
Processa arquivos extraídos do IICS e carrega informações de mapping/template
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, Optional, Tuple

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
    
    def insert_or_update_content(self, record: Dict) -> bool:
        """
        Insere ou atualiza um registro na tabela content
        
        Args:
            record: Dicionário com os dados do registro
            
        Returns:
            bool: True se inseriu/atualizou com sucesso
        """
        query = """
            INSERT INTO fingerhard.content (
                id, class, CodOnda, CodProcess, annotations, big_int_convert_type, 
                document_type, eco_system, name, template_origin, links, 
                transformations, variables, Dt_Inserted, Dt_updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                class = VALUES(class),
                annotations = VALUES(annotations),
                big_int_convert_type = VALUES(big_int_convert_type),
                document_type = VALUES(document_type),
                eco_system = VALUES(eco_system),
                template_origin = VALUES(template_origin),
                links = VALUES(links),
                transformations = VALUES(transformations),
                variables = VALUES(variables),
                Dt_updated = VALUES(Dt_updated);
        """
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, (
                    record['id'],
                    record['class'],
                    record['cod_onda'],
                    record['cod_process'],
                    record['annotations'],
                    record['big_int_convert_type'],
                    record['document_type'],
                    record['eco_system'],
                    record['name'],
                    record['template_origin'],
                    record['links'],
                    record['transformations'],
                    record['variables'],
                    record['dt_inserted'],
                    record['dt_updated']
                ))
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao inserir registro '{record['name']}': {e}")
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


class ContentProcessor:
    """Processador de arquivos JSON de conteúdo"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Inicializa o processador
        
        Args:
            db_manager: Instância do gerenciador de banco de dados
        """
        self.db_manager = db_manager
    
    def extract_codes_from_name(self, name: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extrai CodOnda e CodProcess do nome do arquivo
        
        Formato esperado: algo_CODONDA_CODPROCESS_...
        
        Args:
            name: Nome do arquivo
            
        Returns:
            Tuple (CodOnda, CodProcess)
        """
        if not name:
            return None, None
        
        parts = name.split('_')
        cod_onda = parts[3] if len(parts) > 3 else None
        cod_process = parts[4] if len(parts) > 4 else None
        return cod_onda, cod_process
    
    def parse_content(self, data: Dict, filename: str) -> Optional[Dict]:
        """
        Converte o conteúdo do JSON para o formato de banco de dados
        
        Args:
            data: Dados do JSON
            filename: Nome do arquivo de origem (para logging)
            
        Returns:
            Dict com dados processados ou None em caso de erro
        """
        try:
            # Verificar se tem a estrutura esperada
            if 'content' not in data:
                print(f"⚠️  Arquivo '{filename}' não possui a chave 'content'")
                return None
            
            content = data['content']
            
            # Extrair informações básicas
            content_id = content.get('$$IID')
            if not content_id:
                print(f"⚠️  Arquivo '{filename}' não possui $$IID")
                return None
            
            content_class = content.get('$$class', '')
            name = content.get('name', '')
            
            # Extrair códigos do nome
            cod_onda, cod_process = self.extract_codes_from_name(name)
            
            # Converter campos para JSON string
            annotations = json.dumps(content.get('annotations', {}))
            links = json.dumps(content.get('links', []))
            transformations = json.dumps(content.get('transformations', []))
            variables = json.dumps(content.get('variables', {}))
            
            # Data atual
            now = datetime.now()
            
            return {
                'id': content_id,
                'class': content_class,
                'cod_onda': cod_onda,
                'cod_process': cod_process,
                'annotations': annotations,
                'big_int_convert_type': content.get('bigIntConvertType'),
                'document_type': content.get('documentType'),
                'eco_system': content.get('ecoSystem'),
                'name': name,
                'template_origin': content.get('templateOrigin'),
                'links': links,
                'transformations': transformations,
                'variables': variables,
                'dt_inserted': now,
                'dt_updated': now
            }
        except Exception as e:
            print(f"❌ Erro ao processar arquivo '{filename}': {e}")
            return None
    
    def process_file(self, file_path: str, filename: str) -> bool:
        """
        Processa um arquivo JSON e insere no banco
        
        Args:
            file_path: Caminho completo do arquivo
            filename: Nome do arquivo
            
        Returns:
            bool: True se processou com sucesso
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
        except json.JSONDecodeError as e:
            print(f"❌ Erro ao carregar o arquivo JSON '{filename}': {e}")
            return False
        except Exception as e:
            print(f"❌ Erro ao ler o arquivo '{filename}': {e}")
            return False
        
        # Parse do conteúdo
        parsed_data = self.parse_content(data, filename)
        
        if not parsed_data:
            return False
        
        # Inserir no banco
        if self.db_manager.insert_or_update_content(parsed_data):
            print(f"   ✓ {filename} -> ID: {parsed_data['id']} | Nome: {parsed_data['name']}")
            return True
        else:
            return False
    
    def process_directory(self, directory: str) -> Tuple[int, int]:
        """
        Processa todos os arquivos JSON de um diretório
        
        Args:
            directory: Diretório com os arquivos JSON
            
        Returns:
            Tuple (total_arquivos, arquivos_processados)
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
            return 0, 0
        except PermissionError:
            print(f"❌ Erro: Sem permissão para ler o diretório '{directory}'.")
            return 0, 0
        
        if not arquivos_json:
            print(f"⚠️  Atenção: Nenhum arquivo JSON encontrado no diretório '{directory}'.")
            return 0, 0
        
        print(f"\n📁 Processando {len(arquivos_json)} arquivo(s) JSON...")
        print("-" * 60)
        
        total_arquivos = len(arquivos_json)
        arquivos_processados = 0
        
        for filename in sorted(arquivos_json):
            file_path = os.path.join(directory, filename)
            print(f"\n📄 Processando: {filename}")
            
            if self.process_file(file_path, filename):
                arquivos_processados += 1
        
        return total_arquivos, arquivos_processados


class ContentLoader:
    """Classe principal para carga de conteúdos"""
    
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
            print("   Chaves esperadas: PathMapsGrava")
            sys.exit(1)
        
        print(f"\n✓ Configuração carregada:")
        print(f"   Diretório de origem: {self.config['PathMapsGrava']}")
    
    def setup_database(self):
        """Configura a conexão com o banco de dados"""
        self.db_manager = DatabaseManager()
        if not self.db_manager.connect():
            sys.exit(1)
        
        self.processor = ContentProcessor(self.db_manager)
    
    def run(self):
        """Executa o fluxo principal de processamento"""
        print("\n" + "=" * 60)
        print("🚀 CONTENT LOADER - IICS")
        print("=" * 60)
        print("\n📌 Processando arquivos JSON para tabela fingerhard.content")
        
        # Carregar configurações
        self.load_configuration()
        
        # Configurar banco de dados
        self.setup_database()
        
        try:
            # Processar arquivos JSON
            directory = self.config["PathMapsGrava"]
            total_arquivos, processados = self.processor.process_directory(directory)
            
            # Commit final
            self.db_manager.commit()
            
            # Relatório final
            print("\n" + "=" * 60)
            print("📊 RESUMO FINAL DO PROCESSAMENTO")
            print("=" * 60)
            print(f"📄 Total de arquivos encontrados: {total_arquivos}")
            print(f"✅ Total de arquivos processados: {processados}")
            print(f"❌ Total de falhas: {total_arquivos - processados}")
            print(f"📁 Tabela destino: fingerhard.content")
            print("=" * 60)
            
            if processados == total_arquivos and total_arquivos > 0:
                print("\n✨ Processamento concluído com SUCESSO!")
            elif processados > 0:
                print(f"\n⚠️  Processamento concluído com {total_arquivos - processados} falha(s)!")
            else:
                print("\n⚠️  Nenhum arquivo foi processado!")
            
        except Exception as e:
            print(f"\n❌ Erro inesperado: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        finally:
            self.db_manager.close()


def main():
    """Função principal"""
    loader = ContentLoader()
    loader.run()


if __name__ == "__main__":
    main()