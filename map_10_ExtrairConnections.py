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
        print(f"Erro: Configuração JSON inválida. Detalhes: {e}")
        sys.exit(1)

# Função principal para extrair arquivos connection.json de arquivos ZIP
def ExtrairConnection_zip(onda):
    # Carregar o arquivo de configuração
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E://engenharia//config//config.json')
    config = load_config(config_path)

    try:
        PathfileConnecion = config["PathfileConnecion"] + '/'  # Diretório para salvar os arquivos extraídos
        json_file_path = config["json_file_path"]  # Diretório onde os arquivos ZIP estão localizados
    except KeyError as e:
        print(f"Erro: Chave ausente no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)

    # Construir o caminho para o diretório da onda
    PathConnecion = os.path.join(json_file_path, onda, 'SYS')

    # Certifica-se de que o diretório de destino exista; se não existir, cria-o
    os.makedirs(PathfileConnecion, exist_ok=True)

    # Listar todos os arquivos .Connection.zip no diretório
    try:
        arquivos_zip = [f for f in os.listdir(PathConnecion) if f.endswith('.Connection.zip')]
    except FileNotFoundError:
        print(f"Erro: Diretório '{PathConnecion}' não encontrado.")
        sys.exit(1)

    if not arquivos_zip:
        print(f"Nenhum arquivo '.Connection.zip' encontrado no diretório '{PathConnecion}'.")
        return

    # Para cada arquivo ZIP encontrado, extrair o connection.json
    for indice, arquivo_zip in enumerate(arquivos_zip, start=1):
        caminho_completo_zip = os.path.join(PathConnecion, arquivo_zip)

        try:
            with zipfile.ZipFile(caminho_completo_zip, 'r') as zip_ref:
                # Lista todos os arquivos dentro do zip
                arquivos_no_zip = zip_ref.namelist()

                # Nome do arquivo dentro do ZIP que queremos extrair
                arquivo_a_extrair = 'connection.json'

                # Verificar se o arquivo a ser extraído existe no ZIP
                arquivos_encontrados = [arquivo for arquivo in arquivos_no_zip if arquivo.endswith(arquivo_a_extrair)]

                if arquivos_encontrados:
                    caminho_interno_arquivo = arquivos_encontrados[0]

                    try:
                        # Extrair o arquivo connection.json para o diretório de destino
                        zip_ref.extract(caminho_interno_arquivo, PathfileConnecion)

                        # Renomear o arquivo com base no nome do arquivo ZIP
                        novo_nome = f"{arquivo_zip.replace('.Connection.zip', '')}.json"
                        caminho_completo_destino = os.path.join(PathfileConnecion, novo_nome)

                        # Renomear o arquivo extraído
                        os.rename(os.path.join(PathfileConnecion, caminho_interno_arquivo), caminho_completo_destino)
                        print(f"'{arquivo_a_extrair}' extraído e renomeado para '{novo_nome}' ({arquivo_zip})")

                    except (FileNotFoundError, PermissionError) as e:
                        print(f"Erro ao renomear '{arquivo_a_extrair}' no arquivo ZIP {arquivo_zip}. Detalhes: {e}")
                else:
                    print(f"'{arquivo_a_extrair}' não encontrado no arquivo ZIP '{arquivo_zip}'.")

        except zipfile.BadZipFile:
            print(f"Erro: O arquivo '{arquivo_zip}' não é um arquivo ZIP válido.")
        except Exception as e:
            print(f"Erro inesperado ao processar o arquivo ZIP '{arquivo_zip}'. Detalhes: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python ExtrairConnection_zip.py <onda>")
        sys.exit(1)

    onda = sys.argv[1]

    ExtrairConnection_zip(onda)