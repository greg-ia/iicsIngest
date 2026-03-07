import os
import zipfile
import sys
import json

# Função para carregar o arquivo de configuração JSON
def load_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        return config
    except FileNotFoundError:
        print(f"Erro: Arquivo de configuração '{config_path}' não encontrado.")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Erro: Estrutura inválida no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)

# Função principal para extração
def ExtrairDimensional_bin(onda, pasta):
    # Carregar o arquivo de configuração
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E://engenharia//config//config.json')
    config = load_config(config_path)
    
    # Validar a presença das chaves esperadas no arquivo de configuração
    try:
        directory = config["directory"]
        dirDoubleCheck = config["dirDoubleCheck"]
        PathMapsGrava = config["PathMapsGrava"]
        json_file_path = config["json_file_path"]
    except KeyError as e:
        print(f"Erro: Chave ausente no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)
    
    # Montar o caminho para os zips com base no JSON
    caminho_zip = rf'{json_file_path}/{onda}/Explore/{onda}/{pasta}'

    # Garantir que o diretório de destino existe
    os.makedirs(PathMapsGrava, exist_ok=True)
    
    # Listar todos os arquivos `.DTEMPLATE.zip` no diretório origem
    try:
        arquivos_zip = [f for f in os.listdir(caminho_zip) if f.endswith('.DTEMPLATE.zip')]
        if not arquivos_zip:
            print(f"Atenção: Nenhum arquivo '.DTEMPLATE.zip' encontrado em: {caminho_zip}")
            sys.exit(0)
    except FileNotFoundError:
        print(f"Erro: Diretório '{caminho_zip}' não encontrado.")
        sys.exit(1)

    # Processar cada arquivo ZIP
    for indice, arquivo_zip in enumerate(arquivos_zip, start=1):
        caminho_completo_zip = os.path.join(caminho_zip, arquivo_zip)
        print(f"Processando arquivo ZIP {indice}/{len(arquivos_zip)}: {arquivo_zip}")

        try:
            with zipfile.ZipFile(caminho_completo_zip, 'r') as zip_ref:
                # Listar os arquivos contidos no ZIP
                arquivos_no_zip = zip_ref.namelist()

                # Arquivo que desejamos extrair
                arquivo_a_extrair = '@3.bin'
                arquivos_encontrados = [arquivo for arquivo in arquivos_no_zip if arquivo.endswith(arquivo_a_extrair)]

                # Verificar se o arquivo existe no ZIP
                if arquivos_encontrados:
                    caminho_interno_arquivo = arquivos_encontrados[0]

                    try:
                        # Extrair o arquivo para o diretório de destino
                        zip_ref.extract(caminho_interno_arquivo, PathMapsGrava)
                        
                        # Renomeia o arquivo extraído para incluir o prefixo do arquivo ZIP sem a extensão
                        novo_nome = f"{arquivo_zip.replace('.DTEMPLATE.zip', '')}.json"
                        caminho_completo_destino = os.path.join(PathMapsGrava, novo_nome)
                        os.rename(os.path.join(PathMapsGrava, caminho_interno_arquivo), caminho_completo_destino)
                        
                        print(f"Sucesso: '{arquivo_a_extrair}' extraído e renomeado para '{novo_nome}'")
                    except Exception as e:
                        print(f"Erro ao renomear ou mover '{arquivo_a_extrair}' do ZIP '{arquivo_zip}'. Detalhes: {e}")
                else:
                    print(f"Aviso: '{arquivo_a_extrair}' não encontrado no arquivo ZIP '{arquivo_zip}'")
        except (zipfile.BadZipFile, zipfile.LargeZipFile) as e:
            print(f"Erro: Arquivo ZIP '{arquivo_zip}' corrompido ou excede tamanho permitido. Detalhes: {e}")
        except Exception as e:
            print(f"Erro desconhecido ao processar '{arquivo_zip}'. Detalhes: {e}")

if __name__ == "__main__":
    # Validar se os argumentos foram passados corretamente
    if len(sys.argv) != 3:
        print("Uso: python Emap_02_ExtrairBinario.py 684_UME_TRAFEGO Dimensional")
        sys.exit(1)

    # Capturar os parâmetros da linha de comando
    onda = sys.argv[1]
    pasta = sys.argv[2]

    # Executar a função principal
    ExtrairDimensional_bin(onda, pasta)