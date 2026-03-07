import os
import zipfile
import sys
import json
from dotenv import load_dotenv
# Carregar variáveis de ambiente
load_dotenv()

config_path = os.getenv('CONFIG_PATH')

# Função para carregar as configurações do arquivo JSON
def load_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        return config
    except FileNotFoundError:
        print(f"Error: Arquivo de configuração '{config_path}' não encontrado.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Erro ao interpretar JSON do arquivo de configuração. Detalhes: {e}")
        sys.exit(1)

# Função principal para extrair os arquivos do tipo fileRecord.json
def ExtrairDimensional_bin(onda, pasta, deploy, deployiics):
    # Caminho para carregar o arquivo de configuração
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA')

    # Tentar carregar as configurações
    config = load_config(config_path)
    
    try:
        # Chaves esperadas no arquivo de configuração
        dirDoubleCheck = config["dirDoubleCheck"]
        PathFileRecordsGrava = config["PathFileRecordsGrava"]
        PathFileRecordsDoubleCheckGrava = config["PathFileRecordsDoubleCheckGrava"]        
        json_file_path = config["json_file_path"]
    except KeyError as e:
        print(f"Error: Chave faltando no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)
    
    # Definir o caminho do ZIP e o caminho de extração com base no deployment
    if deploy.upper() == "DD":  # "DD" é para o caso Dedo-Duro
        caminho_zip = fr'{json_file_path}/{onda}/Explore/{onda}/{pasta}'
        CaminhoExtracao = PathFileRecordsGrava
    else:  # Outros casos regulares para deploy específico
        caminho_zip = fr'{dirDoubleCheck}/{deploy}/{deploy}/{deployiics}/Explore/{onda}/{pasta}'
        CaminhoExtracao = PathFileRecordsDoubleCheckGrava

    # Garantir que o diretório de extração existe
    os.makedirs(CaminhoExtracao, exist_ok=True)
    
    # Identificar os arquivos ZIP para extração
    try:
        arquivos_zip = [f for f in os.listdir(caminho_zip) if f.endswith('.DTEMPLATE.zip')]
        if not arquivos_zip:
            print(f"Warning: Nenhum arquivo '.DTEMPLATE.zip' encontrado no diretório '{caminho_zip}'.")
            sys.exit(0)
    except FileNotFoundError:
        print(f"Error: Diretório '{caminho_zip}' não encontrado.")
        sys.exit(1)

    # Processar cada arquivo ZIP e extrair o "fileRecord.json"
    for indice, arquivo_zip in enumerate(arquivos_zip, start=1):
        caminho_completo_zip = os.path.join(caminho_zip, arquivo_zip)
        
        try:
            with zipfile.ZipFile(caminho_completo_zip, 'r') as zip_ref:
                arquivos_no_zip = zip_ref.namelist()
                arquivo_a_extrair = 'fileRecord.json'
                arquivos_encontrados = [arquivo for arquivo in arquivos_no_zip if arquivo.endswith(arquivo_a_extrair)]

                if arquivos_encontrados:
                    caminho_interno_arquivo = arquivos_encontrados[0]
                    
                    # Extração do arquivo "fileRecord.json"
                    zip_ref.extract(caminho_interno_arquivo, CaminhoExtracao)
                    
                    # Renomear o arquivo extraído com base no ZIP original
                    novo_nome = f"{arquivo_zip.replace('.DTEMPLATE.zip', '')}_fileRecords.json"
                    caminho_completo_destino = os.path.join(CaminhoExtracao, novo_nome)
                    
                    os.rename(os.path.join(CaminhoExtracao, caminho_interno_arquivo), caminho_completo_destino)
                    print(f"Sucesso: Extraído e renomeado para '{novo_nome}' (Arquivo {indice} de {len(arquivos_zip)})")
                else:
                    print(f"Erro: Arquivo '{arquivo_a_extrair}' não encontrado no ZIP '{arquivo_zip}'")
        except (zipfile.BadZipFile, zipfile.LargeZipFile) as e:
            print(f"Erro: O arquivo '{arquivo_zip}' não é um ZIP válido ou excede o tamanho permitido. Detalhes: {e}")
        except Exception as e:
            print(f"Erro desconhecido ao processar '{arquivo_zip}'. Detalhes: {e}")

if __name__ == "__main__":
    # Verificar se os argumentos foram passados corretamente
    if len(sys.argv) != 5:
        print("Uso: python map_01_ExtrairFileRecord.py 684_UME_TRAFEGO Dimensional DD N/A")
        print("Uso: python map_01_ExtrairFileRecord.py 684_UME_TRAFEGO Fatos DD N/A")
        print("Uso: python map_01_ExtrairFileRecord.py 684_UME_TRAFEGO Geral DD N/A")
        print("Onde deploy = DD (Dedo Duro) ou nome do pacote de deployment.")
        sys.exit(1)

    # Coletar os argumentos fornecidos
    onda = sys.argv[1]
    pasta = sys.argv[2]
    deploy = sys.argv[3]
    deployiics = sys.argv[4]
    
    # Executar função principal
    ExtrairDimensional_bin(onda, pasta, deploy, deployiics)