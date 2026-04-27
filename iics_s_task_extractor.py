#!/usr/bin/env python3
"""
Script para extrair arquivos mtTask.json de arquivos .MTT.zip
Uso exclusivo para extração de MTT Tasks do IICS
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
        
        Retorna uma lista de dicionários, cada um contendo:
        - cod_projeto
        - nme_projeto
        - pasta
        - nme_projeto_iics (calculado)
        """
        query = """
            SELECT DISTINCT 
                p.cod_projeto,
                p.nme_projeto,
                pf.pasta
            FROM ruleguardian.projetos p
            INNER JOIN ruleguardian.processos_familias pf 
                ON p.cod_projeto = pf.cod_projeto
            WHERE p.cod_projeto = %s
        """
        
        try:
            self.cursor.execute(query, (cod_projeto,))
            results = self.cursor.fetchall()
            
            if not results:
                print(f"✗ Projeto com código {cod_projeto} não encontrado no banco de dados.")
                return []
            
            # Adiciona nme_projeto_iics em cada resultado
            for result in results:
                result['nme_projeto_iics'] = f"{result['cod_projeto']}_{result['nme_projeto']}"
            
            print(f"✓ Encontradas {len(results)} pasta(s) para o projeto {cod_projeto}:")
            for result in results:
                print(f"  - {result['pasta']}")
            
            return results
        except Exception as e:
            print(f"✗ Erro ao consultar banco de dados: {e}")
            return []
    
    def close(self):
        """Fecha conexões"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("✓ Conexão MySQL fechada.")


class MTTTaskExtractor:
    """Extrator de arquivos mtTask.json de arquivos .MTT.zip"""
    
    def __init__(self):
        self.config = None
        self.load_config()
    
    def load_config(self):
        """Carrega as configurações do arquivo JSON"""
        # Tenta primeiro a variável de ambiente específica
        
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
        required_keys = ['json_file_path', 'PathfileTask']
        missing_keys = [key for key in required_keys if key not in self.config]
        
        if missing_keys:
            print(f"Error: Configuração incompleta. Chaves obrigatórias faltando: {missing_keys}")
            print(f"       Verifique o arquivo: {config_path}")
            print(f"       Chaves esperadas: json_file_path, PathfileTask")
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
    
    def extrair_mtt_task(self, arquivo_zip, caminho_zip, caminho_destino):
        """
        Extrai e renomeia o arquivo mtTask.json de um único arquivo .MTT.zip
        
        Args:
            arquivo_zip (str): Nome do arquivo ZIP
            caminho_zip (str): Caminho onde está o ZIP
            caminho_destino (str): Caminho destino para extração
            
        Returns:
            tuple: (sucesso, mensagem)
        """
        caminho_completo_zip = os.path.join(caminho_zip, arquivo_zip)
        arquivo_a_extrair = 'mtTask.json'
        
        try:
            with zipfile.ZipFile(caminho_completo_zip, 'r') as zip_ref:
                # Listar os arquivos contidos no ZIP
                arquivos_no_zip = zip_ref.namelist()
                
                # Procurar por mtTask.json
                arquivos_encontrados = [f for f in arquivos_no_zip if f.endswith(arquivo_a_extrair)]
                
                if not arquivos_encontrados:
                    return False, f"'{arquivo_a_extrair}' não encontrado"
                
                # Extrair o arquivo
                caminho_interno_arquivo = arquivos_encontrados[0]
                zip_ref.extract(caminho_interno_arquivo, caminho_destino)
                
                # Renomear o arquivo extraído
                nome_base = arquivo_zip.replace('.MTT.zip', '')
                novo_nome = f"{nome_base}_mtt.json"
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
    
    def processar_pasta(self, nme_projeto_iics, pasta):
        """
        Processa uma pasta específica do projeto
        
        Args:
            nme_projeto_iics (str): Nome do projeto no formato IICS (cod_projeto-nme_projeto)
            pasta (str): Tipo de pasta (Dimensional, Fatos, Geral)
        
        Returns:
            tuple: (sucessos, falhas)
        """
        print("\n" + "="*60)
        print(f"📁 PROCESSANDO PASTA: {pasta}")
        print("="*60)
        
        # Obter caminhos do config
        json_file_path = self.config.get("json_file_path")
        pathfile_task = self.config.get("PathfileTask")
        
        # Montar caminho de origem
        caminho_origem = os.path.join(json_file_path, nme_projeto_iics, 'Explore', nme_projeto_iics, pasta)
        caminho_origem = str(caminho_origem).replace('\\', '/')
        
        print(f"📂 Origem: {caminho_origem}")
        print(f"📂 Destino: {pathfile_task}")
        
        # Verificar se diretório de origem existe
        if not os.path.exists(caminho_origem):
            print(f"⚠️  Diretório de origem não encontrado, pulando...")
            return 0, 0
        
        # Verificar permissão de leitura
        if not os.access(caminho_origem, os.R_OK):
            print(f"⚠️  Sem permissão de leitura para o diretório, pulando...")
            return 0, 0
        
        # Listar arquivos .MTT.zip
        try:
            arquivos_zip = [f for f in os.listdir(caminho_origem) 
                           if f.endswith('.MTT.zip')]
        except PermissionError:
            print(f"❌ Sem permissão para ler o diretório: {caminho_origem}")
            return 0, 0
        except Exception as e:
            print(f"❌ Erro ao listar diretório: {e}")
            return 0, 0
        
        if not arquivos_zip:
            print(f"⚠️  Nenhum arquivo .MTT.zip encontrado")
            return 0, 0
        
        print(f"📦 Encontrados {len(arquivos_zip)} arquivos para processar")
        print("-"*60)
        
        # Processar cada arquivo
        sucessos = 0
        falhas = 0
        
        for idx, arquivo_zip in enumerate(sorted(arquivos_zip), 1):
            sucesso, mensagem = self.extrair_mtt_task(
                arquivo_zip, 
                caminho_origem, 
                pathfile_task
            )
            
            if sucesso:
                print(f"   ✅ [{idx}/{len(arquivos_zip)}] {mensagem}")
                sucessos += 1
            else:
                print(f"   ❌ [{idx}/{len(arquivos_zip)}] {arquivo_zip}: {mensagem}")
                falhas += 1
        
        return sucessos, falhas
    
    def extrair_mtt_tasks(self, projetos_info):
        """
        Função principal para extração de MTT Tasks
        
        Args:
            projetos_info (list): Lista de dicionários com informações dos projetos
        """
        print("\n" + "="*60)
        print("🚀 EXTRATOR DE MTT TASKS IICS (mtTask.json)")
        print("="*60)
        
        # Garantir que o diretório de destino existe
        pathfile_task = self.config.get("PathfileTask")
        if not self.criar_diretorio_seguro(pathfile_task):
            print("❌ Não foi possível criar/acessar o diretório de destino")
            sys.exit(1)
        
        # Processar cada projeto/pasta
        total_sucessos = 0
        total_falhas = 0
        total_projetos = len(projetos_info)
        
        for idx, projeto in enumerate(projetos_info, 1):
            print(f"\n{'='*60}")
            print(f"📦 PROJETO {idx}/{total_projetos}")
            print(f"   Código: {projeto['cod_projeto']}")
            print(f"   Nome: {projeto['nme_projeto']}")
            print(f"   Pasta: {projeto['pasta']}")
            print(f"   Nome IICS: {projeto['nme_projeto_iics']}")
            print(f"{'='*60}")
            
            sucessos, falhas = self.processar_pasta(
                projeto['nme_projeto_iics'],
                projeto['pasta']
            )
            
            total_sucessos += sucessos
            total_falhas += falhas
        
        # Relatório final consolidado
        print("\n" + "="*60)
        print("📊 RELATÓRIO FINAL CONSOLIDADO")
        print("="*60)
        print(f"✅ Total de sucessos: {total_sucessos}")
        print(f"❌ Total de falhas: {total_falhas}")
        print(f"📁 Diretório destino: {pathfile_task}")
        print(f"📋 Total de pastas processadas: {total_projetos}")
        
        if total_falhas == 0 and total_sucessos > 0:
            print("\n✨ Processo concluído com SUCESSO!")
            return 0
        elif total_sucessos == 0:
            print("\n⚠️  Nenhum arquivo foi processado!")
            return 1
        else:
            print(f"\n⚠️  Processo concluído com {total_falhas} falha(s)")
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
        print("  1. Buscar todas as pastas (Dimensional, Fatos, Geral) no banco")
        print("  2. Extrair mtTask.json de cada arquivo .MTT.zip")
        print("  3. Renomear para <nome>_mtt.json")
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
    
    projetos_info = db.get_projeto_info(cod_projeto)
    db.close()
    
    if not projetos_info:
        print(f"\n❌ Nenhuma informação encontrada para o código {cod_projeto}")
        sys.exit(1)
    
    # Executar extração
    extractor = MTTTaskExtractor()
    exit_code = extractor.extrair_mtt_tasks(projetos_info)
    
    sys.exit(exit_code)


if __name__ == "__main__":
    main()