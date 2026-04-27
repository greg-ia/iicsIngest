#!/usr/bin/env python3
"""
Script para processar advanced_properties dos transformations
Carrega dados na tabela fingerhard.transformation_advanced_properties
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


class AdvancedPropertiesProcessor:
    """Processador de advanced_properties dos transformations"""
    
    def __init__(self, db_manager: DatabaseManager):
        """
        Inicializa o processador
        
        Args:
            db_manager: Instância do gerenciador de banco de dados
        """
        self.db_manager = db_manager
    
    def delete_old_records(self, codigo_onda: str) -> bool:
        """
        Remove registros antigos da tabela para o código da onda
        
        Args:
            codigo_onda: Código da onda
            
        Returns:
            bool: True se removeu com sucesso
        """
        query = "DELETE FROM fingerhard.transformation_advanced_properties WHERE CodOnda = %s"
        
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
                id, CodOnda, CodProcess, content_name, advanced_properties, create_time_brazil
            FROM 
                fingerhard.transformation
            WHERE 
                CodOnda = %s 
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
    
    def parse_advanced_properties(self, advanced_properties_json: str, content_name: str) -> Optional[List[Dict]]:
        """
        Parse do JSON de advanced_properties
        
        Args:
            advanced_properties_json: String JSON das advanced_properties
            content_name: Nome do conteúdo para logging
            
        Returns:
            Lista de advanced_properties ou None em caso de erro
        """
        if not advanced_properties_json or not advanced_properties_json.strip():
            return None
        
        try:
            advanced_properties = json.loads(advanced_properties_json)
            
            # Se for um dicionário, converte para lista
            if isinstance(advanced_properties, dict):
                # Pode ser um único objeto ou um dicionário com chave 'advancedProperties'
                if 'advancedProperties' in advanced_properties:
                    advanced_properties = advanced_properties['advancedProperties']
                else:
                    advanced_properties = [advanced_properties]
            
            # Se for uma lista, processa cada item
            if isinstance(advanced_properties, list):
                return advanced_properties
            else:
                print(f"⚠️  Formato inesperado de advanced_properties para {content_name}")
                return None
                
        except json.JSONDecodeError as e:
            print(f"❌ Erro ao decodificar JSON para {content_name}: {e}")
            return None
    
    def convert_value_to_string(self, value: Any) -> str:
        """
        Converte o valor para string adequada para o banco
        
        Args:
            value: Valor original (pode ser dict, list, bool, etc)
            
        Returns:
            str: Valor convertido para string
        """
        if value is None:
            return ''
        
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        
        if isinstance(value, bool):
            return 'true' if value else 'false'
        
        return str(value)
    
    def insert_advanced_property(self, property_data: Dict) -> bool:
        """
        Insere ou atualiza uma advanced_property na tabela
        
        Args:
            property_data: Dicionário com os dados da advanced_property
            
        Returns:
            bool: True se inseriu/atualizou com sucesso
        """
        query = """
            INSERT INTO fingerhard.transformation_advanced_properties (
                Id, id_transf, CodOnda, CodProcess, class, name, value, 
                content_name, Dt_Inserted, Dt_updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                class = VALUES(class),
                name = VALUES(name),
                value = VALUES(value),
                content_name = VALUES(content_name),
                Dt_updated = VALUES(Dt_updated)
        """
        
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (
                    property_data['id'],
                    property_data['id_transf'],
                    property_data['cod_onda'],
                    property_data['cod_process'],
                    property_data['class'],
                    property_data['name'],
                    property_data['value'],
                    property_data['content_name'],
                    property_data['dt_inserted'],
                    property_data['dt_updated']
                ))
            return True
        except pymysql.MySQLError as e:
            print(f"❌ Erro ao inserir advanced_property '{property_data['name']}': {e}")
            return False
    
    def process_transformation(self, transformation: Dict) -> Tuple[int, int]:
        """
        Processa um transformation, extraindo e inserindo suas advanced_properties
        
        Args:
            transformation: Dicionário com dados do transformation
            
        Returns:
            Tuple (total_properties, properties_inseridas)
        """
        id_transf = transformation['id']
        cod_onda = transformation['CodOnda']
        cod_process = transformation['CodProcess']
        content_name = transformation['content_name']
        advanced_properties_json = transformation['advanced_properties']
        create_time_brazil = transformation['create_time_brazil']
        
        # Parse das advanced_properties
        advanced_properties = self.parse_advanced_properties(advanced_properties_json, content_name)
        
        if not advanced_properties:
            return 0, 0
        
        total = len(advanced_properties)
        inseridas = 0
        
        for advanced_property in advanced_properties:
            # Extrair dados da advanced_property
            property_id = advanced_property.get('$$ID')
            if not property_id:
                print(f"⚠️  Advanced property sem $$ID no transformation {id_transf}")
                continue
            
            property_class = advanced_property.get('$$class', '')
            name = advanced_property.get('name', '')
            value = self.convert_value_to_string(advanced_property.get('value'))
            
            # Se o nome estiver vazio, usa um identificador padrão
            if not name:
                name = f"property_{property_id}"
            
            # Preparar dados para inserção
            property_data = {
                'id': property_id,
                'id_transf': id_transf,
                'cod_onda': cod_onda,
                'cod_process': cod_process,
                'class': property_class,
                'name': name,
                'value': value,
                'content_name': content_name,
                'dt_inserted': create_time_brazil if create_time_brazil else self.db_manager.date_ins,
                'dt_updated': self.db_manager.date_ins
            }
            
            if self.insert_advanced_property(property_data):
                inseridas += 1
        
        if total > 0:
            print(f"   📊 {content_name} (ID: {id_transf}): {inseridas}/{total} properties")
        
        return total, inseridas
    
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
        total_properties = 0
        total_inseridas = 0
        
        print("\n📦 Processando transformations...")
        for idx, transformation in enumerate(transformations, 1):
            print(f"\n   [{idx}/{len(transformations)}] Processando transformation ID: {transformation['id']}")
            total, inseridas = self.process_transformation(transformation)
            total_properties += total
            total_inseridas += inseridas
        
        print("\n" + "-" * 60)
        print(f"📊 Resumo do processamento:")
        print(f"   Total de transformations processados: {len(transformations)}")
        print(f"   Total de advanced_properties encontradas: {total_properties}")
        print(f"   Total de advanced_properties inseridas/atualizadas: {total_inseridas}")
        
        return total_inseridas > 0 or total_properties == 0


class AdvancedPropertiesLoader:
    """Classe principal para carga de advanced_properties"""
    
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
        
        self.processor = AdvancedPropertiesProcessor(self.db_manager)
    
    def run(self):
        """Executa o fluxo principal de processamento"""
        print("\n" + "=" * 60)
        print("🚀 ADVANCED PROPERTIES LOADER - IICS")
        print("=" * 60)
        print("\n📌 Processando advanced_properties para tabela fingerhard.transformation_advanced_properties")
        
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
            # Processar advanced_properties
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
    loader = AdvancedPropertiesLoader()
    loader.run()


if __name__ == "__main__":
    main()