#!/usr/bin/env python3
"""
Script para extrair arquivos connection.json de arquivos .Connection.zip
Uso exclusivo para extração de conexões do IICS
"""

import os
import zipfile
import sys
import json
import pymysql
from dotenv import load_dotenv
from pathlib import Path

# Carregar variáveis de ambiente
load_dotenv()


class MySQLConnection:
    """Gerenciador de conexão MySQL"""
    
    def __init__(self):
        self.config = {
            'host': os.getenv('MYSQL_RG_HOST', 'localhost'),
            'user': os.getenv('MYSQL_RG_USER'),
            'password': os.getenv('MYSQL_RG_PASSWORD'),
            'database': os.getenv('MYSQL_RG_DATABASE')
        }
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Estabelece conexão com MySQL"""
        # Verifica credenciais obrigatórias
        if not all([self.config['user'], self.config['password'], self.config['database']]):
            print("Erro: Variáveis de ambiente MySQL são obrigatórias:")
            print("  - MYSQL_RG_USER")
            print("  - MYSQL_RG_PASSWORD")
            print("  - MYSQL_RG_DATABASE")
            return False
        
        try:
            self.conn = pymysql.connect(**self.config)
            self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
            print("✓ Conexão com MySQL estabelecida com sucesso.")
            return True
        except Exception as e:
            print(f"✗ Erro ao conectar ao MySQL: {e}")
            return False
    
    def get_projeto_info(self, cod_projeto):
        """
        Busca informações do projeto no banco
        
        Retorna um dicionário com:
        - cod_projeto
        - nme_projeto
        - nme_projeto_iics (calculado)
        """
        query = """
            SELECT DISTINCT 
                p.cod_projeto,
                p.nme_projeto
            FROM ruleguardian.projetos p
            INNER JOIN ruleguardian.processos_familias pf 
                ON p.cod_projeto = pf.cod_projeto
            WHERE p.cod_projeto = %s
        """
        
        try:
            self.cursor.execute(query, (cod_projeto,))
            result = self.cursor.fetchone()
            
            if not result:
                print(f"✗ Projeto com código {cod_projeto} não encontrado no banco de dados.")
                return None
            
            # Adiciona nme_projeto_iics no formato: cod_projeto-nme_projeto
            result['nme_projeto_iics'] = f"{result['cod_projeto']}_{result['nme_projeto']}"
            
            print(f"✓ Projeto encontrado:")
            print(f"   Código: {result['cod_projeto']}")
            print(f"   Nome: {result['nme_projeto']}")
            print(f"   Nome IICS: {result['nme_projeto_iics']}")
            
            return result
        except Exception as e:
            print(f"✗ Erro ao consultar banco de dados: {e}")
            return None
    
    def close(self):
        """Fecha conexões"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("✓ Conexão MySQL fechada.")


class ConnectionExtractor:
    """Extrator de arquivos connection.json de arquivos .Connection.zip"""
    
    def __init__(self):
        self.config = None
        self.load_config()
    
    def load_config(self):
        """Carrega as configurações do arquivo JSON"""
        # Tenta primeiro a variável de ambiente específica
        config_path = os.getenv('CONFIG_PATH_ENGENHARIA')
        
        if not config_path:
            config_path = os.getenv('CONFIG_PATH')
        
        if not config_path:
            print("Error: Variável CONFIG_PATH_ENGENHARIA ou CONFIG_PATH não definida no .env")
            print("Configure: CONFIG_PATH_ENGENHARIA=/caminho/para/config.json")
            print("     ou: CONFIG_PATH=/caminho/para/config.json")
            sys.exit(1)
        
        # Remove trailing slash se existir
        config_path = config_path.rstrip('/')
        
        # Verifica se é um arquivo (não diretório)
        if os.path.isdir(config_path):
            # Tenta encontrar config.json dentro do diretório
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
                print(f"Error: CONFIG_PATH aponta para um diretório ({config_path})")
                print(f"       Mas nenhum dos arquivos foi encontrado: {', '.join(possible_files)}")
                sys.exit(1)
        
        # Verifica se o arquivo existe
        if not os.path.exists(config_path):
            print(f"Error: Arquivo de configuração não encontrado: {config_path}")
            sys.exit(1)
        
        # Verifica permissão de leitura
        if not os.access(config_path, os.R_OK):
            print(f"Error: Sem permissão de leitura para o arquivo: {config_path}")
            print(f"       Execute: chmod +r {config_path}")
            sys.exit(1)
        
        # Tenta ler o arquivo
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            print(f"✓ Configurações carregadas com sucesso de: {config_path}")
        except json.JSONDecodeError as e:
            print(f"Error: Arquivo de configuração com JSON inválido: {e}")
            print(f"       Verifique a sintaxe do arquivo: {config_path}")
            sys.exit(1)
        except Exception as e:
            print(f"Error: Não foi possível ler o arquivo de configuração: {e}")
            sys.exit(1)
        
        # Valida as configurações necessárias
        required_keys = ['json_file_path', 'PathfileConnecion']
        missing_keys = [key for key in required_keys if key not in self.config]
        
        if missing_keys:
            print(f"Error: Configuração incompleta. Chaves obrigatórias faltando: {missing_keys}")
            print(f"       Verifique o arquivo: {config_path}")
            print(f"       Chaves esperadas: json_file_path, PathfileConnecion")
            sys.exit(1)
    
    def criar_diretorio_seguro(self, caminho):
        """Cria diretório com permissões adequadas"""
        try:
            caminho_path = Path(caminho)
            caminho_path.mkdir(parents=True, exist_ok=True)
            
            # Tenta ajustar permissões (pode falhar no Windows)
            try:
                os.chmod(caminho, 0o755)
            except (PermissionError, OSError):
                pass
            
            return True
        except PermissionError:
            print(f"Error: Sem permissão para criar diretório: {caminho}")
            return False
        except Exception as e:
            print(f"Error: Não foi possível criar diretório {caminho}: {e}")
            return False
    
    def extrair_connection(self, arquivo_zip, caminho_zip, caminho_destino):
        """
        Extrai e renomeia o arquivo connection.json de um único arquivo .Connection.zip
        
        Args:
            arquivo_zip (str): Nome do arquivo ZIP
            caminho_zip (str): Caminho onde está o ZIP
            caminho_destino (str): Caminho destino para extração
            
        Returns:
            tuple: (sucesso, mensagem)
        """
        caminho_completo_zip = os.path.join(caminho_zip, arquivo_zip)
        arquivo_a_extrair = 'connection.json'
        
        try:
            with zipfile.ZipFile(caminho_completo_zip, 'r') as zip_ref:
                # Listar os arquivos contidos no ZIP
                arquivos_no_zip = zip_ref.namelist()
                
                # Procurar por connection.json
                arquivos_encontrados = [f for f in arquivos_no_zip if f.endswith(arquivo_a_extrair)]
                
                if not arquivos_encontrados:
                    return False, f"'{arquivo_a_extrair}' não encontrado"
                
                # Extrair o arquivo
                caminho_interno_arquivo = arquivos_encontrados[0]
                zip_ref.extract(caminho_interno_arquivo, caminho_destino)
                
                # Renomear o arquivo extraído
                nome_base = arquivo_zip.replace('.Connection.zip', '')
                novo_nome = f"{nome_base}.json"
                caminho_antigo = os.path.join(caminho_destino, caminho_interno_arquivo)
                caminho_novo = os.path.join(caminho_destino, novo_nome)
                
                # Se já existir, remove antes de renomear
                if os.path.exists(caminho_novo):
                    os.remove(caminho_novo)
                
                os.rename(caminho_antigo, caminho_novo)
                
                # Ajusta permissões (ignora erros no Windows)
                try:
                    os.chmod(caminho_novo, 0o644)
                except (PermissionError, OSError):
                    pass
                
                return True, f"'{arquivo_a_extrair}' extraído e renomeado para '{novo_nome}'"
                
        except zipfile.BadZipFile:
            return False, "Arquivo ZIP corrompido"
        except zipfile.LargeZipFile:
            return False, "Arquivo ZIP excede tamanho permitido"
        except Exception as e:
            return False, f"Erro: {str(e)}"
    
    def processar_conexoes(self, nme_projeto_iics):
        """
        Processa as conexões do projeto (pasta SYS fixa)
        
        Args:
            nme_projeto_iics (str): Nome do projeto no formato IICS (cod_projeto-nme_projeto)
        
        Returns:
            tuple: (sucessos, falhas)
        """
        print("\n" + "="*60)
        print(f"📁 PROCESSANDO CONEXÕES - PASTA: SYS")
        print("="*60)
        
        # Obter caminhos do config
        json_file_path = self.config.get("json_file_path")
        pathfile_connecion = self.config.get("PathfileConnecion")
        
        # Montar caminho de origem (sempre usa SYS, não tem Dimensional/Fatos/Geral)
        caminho_origem = os.path.join(json_file_path, nme_projeto_iics, 'SYS')
        caminho_origem = str(caminho_origem).replace('\\', '/')
        
        print(f"📂 Origem: {caminho_origem}")
        print(f"📂 Destino: {pathfile_connecion}")
        
        # Verificar se diretório de origem existe
        if not os.path.exists(caminho_origem):
            print(f"❌ Diretório de origem não encontrado: {caminho_origem}")
            return 0, 0
        
        # Verificar permissão de leitura
        if not os.access(caminho_origem, os.R_OK):
            print(f"❌ Sem permissão de leitura para o diretório: {caminho_origem}")
            return 0, 0
        
        # Listar arquivos .Connection.zip
        try:
            arquivos_zip = [f for f in os.listdir(caminho_origem) 
                           if f.endswith('.Connection.zip')]
        except PermissionError:
            print(f"❌ Sem permissão para ler o diretório: {caminho_origem}")
            return 0, 0
        except Exception as e:
            print(f"❌ Erro ao listar diretório: {e}")
            return 0, 0
        
        if not arquivos_zip:
            print(f"⚠️  Nenhum arquivo .Connection.zip encontrado")
            return 0, 0
        
        print(f"📦 Encontrados {len(arquivos_zip)} arquivos para processar")
        print("-"*60)
        
        # Processar cada arquivo
        sucessos = 0
        falhas = 0
        
        for idx, arquivo_zip in enumerate(sorted(arquivos_zip), 1):
            sucesso, mensagem = self.extrair_connection(
                arquivo_zip, 
                caminho_origem, 
                pathfile_connecion
            )
            
            if sucesso:
                print(f"   ✅ [{idx}/{len(arquivos_zip)}] {mensagem}")
                sucessos += 1
            else:
                print(f"   ❌ [{idx}/{len(arquivos_zip)}] {arquivo_zip}: {mensagem}")
                falhas += 1
        
        return sucessos, falhas
    
    def extrair_conexoes(self, projeto_info):
        """
        Função principal para extração de conexões
        
        Args:
            projeto_info (dict): Dicionário com informações do projeto
        """
        print("\n" + "="*60)
        print("🚀 EXTRATOR DE CONEXÕES IICS (connection.json)")
        print("="*60)
        
        # Garantir que o diretório de destino existe
        pathfile_connecion = self.config.get("PathfileConnecion")
        if not self.criar_diretorio_seguro(pathfile_connecion):
            print("❌ Não foi possível criar/acessar o diretório de destino")
            sys.exit(1)
        
        # Processar as conexões
        print(f"\n{'='*60}")
        print(f"📦 PROJETO")
        print(f"   Código: {projeto_info['cod_projeto']}")
        print(f"   Nome: {projeto_info['nme_projeto']}")
        print(f"   Nome IICS: {projeto_info['nme_projeto_iics']}")
        print(f"{'='*60}")
        
        sucessos, falhas = self.processar_conexoes(projeto_info['nme_projeto_iics'])
        
        # Relatório final
        print("\n" + "="*60)
        print("📊 RELATÓRIO FINAL")
        print("="*60)
        print(f"✅ Sucessos: {sucessos}")
        print(f"❌ Falhas: {falhas}")
        print(f"📁 Diretório destino: {pathfile_connecion}")
        
        if falhas == 0 and sucessos > 0:
            print("\n✨ Processo concluído com SUCESSO!")
            return 0
        elif sucessos == 0:
            print("\n⚠️  Nenhum arquivo foi processado!")
            return 1
        else:
            print(f"\n⚠️  Processo concluído com {falhas} falha(s)")
            return 1


def main():
    """Função principal com validação de argumentos"""
    if len(sys.argv) != 2:
        print("\n" + "="*60)
        print("Uso correto:")
        print(f"  python {sys.argv[0]} <cod_projeto>")
        print("\nExemplos:")
        print(f"  python {sys.argv[0]} 653")
        print(f"  python {sys.argv[0]} 684")
        print(f"  python {sys.argv[0]} 781")
        print("\nOnde:")
        print("  cod_projeto: Código numérico do projeto (ex: 653, 684, 781)")
        print("\nO script irá:")
        print("  1. Buscar o projeto no banco de dados")
        print("  2. Extrair connection.json de cada arquivo .Connection.zip")
        print("  3. Salvar na pasta SYS (conexões)")
        print("="*60)
        sys.exit(1)
    
    cod_projeto = sys.argv[1]
    
    # Validar se é número
    if not cod_projeto.isdigit():
        print(f"\n❌ Error: cod_projeto deve ser um número inteiro")
        print(f"   Recebido: '{cod_projeto}'")
        sys.exit(1)
    
    # Conectar ao MySQL e buscar informações
    db = MySQLConnection()
    if not db.connect():
        sys.exit(1)
    
    projeto_info = db.get_projeto_info(cod_projeto)
    db.close()
    
    if not projeto_info:
        print(f"\n❌ Nenhuma informação encontrada para o código {cod_projeto}")
        sys.exit(1)
    
    # Executar extração
    extractor = ConnectionExtractor()
    exit_code = extractor.extrair_conexoes(projeto_info)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()