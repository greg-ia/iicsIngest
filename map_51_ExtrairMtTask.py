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

# Função principal para extrair arquivos mtTask.json de arquivos MTT.zip
def ExtrairDimensional_bin(onda, pasta):
    # Carregar o arquivo de configuração
    config_path = os.getenv('CONFIG_PATH_ENGENHARIA', 'E://engenharia//config//config.json')
    config = load_config(config_path)

    try:  
        PathfileTask = config["PathfileTask"] + '/'  # Diretório para salvar arquivos extraídos
        json_file_path = config["json_file_path"]  # Diretório base para os arquivos ZIP
    except KeyError as e:
        print(f"Error: Chave ausente no arquivo de configuração. Detalhes: {e}")
        sys.exit(1)  

    # Construir o caminho completo para os arquivos do projeto
    PathProject = os.path.join(json_file_path, onda, 'Explore', onda, pasta)

    # Certificar-se de que o diretório de destino exista
    os.makedirs(PathfileTask, exist_ok=True)

    # Listar todos os arquivos .MTT.zip no diretório
    try:
        arquivos_zip = [f for f in os.listdir(PathProject) if f.endswith('.MTT.zip')]
    except FileNotFoundError:
        print(f"Erro: Diretório '{PathProject}' não encontrado.")
        sys.exit(1)

    if not arquivos_zip:
        print(f"Nenhum arquivo '.MTT.zip' encontrado no diretório '{PathProject}'.")
        return

    # Para cada arquivo zip encontrado, extrair o mtTask.json
    for indice, arquivo_zip in enumerate(arquivos_zip, start=1):
        caminho_completo_zip = os.path.join(PathProject, arquivo_zip)

        try:
            with zipfile.ZipFile(caminho_completo_zip, 'r') as zip_ref:
                # Lista todos os arquivos dentro do ZIP
                arquivos_no_zip = zip_ref.namelist()

                # Nome do arquivo que queremos extrair
                arquivo_a_extrair = 'mtTask.json'

                # Verificar se o arquivo a ser extraído existe no ZIP
                arquivos_encontrados = [arquivo for arquivo in arquivos_no_zip if arquivo.endswith(arquivo_a_extrair)]

                if arquivos_encontrados:
                    caminho_interno_arquivo = arquivos_encontrados[0]

                    try:
                        # Extrair o arquivo desejado para o diretório de destino
                        zip_ref.extract(caminho_interno_arquivo, PathfileTask)

                        # Renomear o arquivo extraído com base no nome do arquivo ZIP
                        novo_nome = f"{arquivo_zip.replace('.MTT.zip', '')}_mtt.json"
                        caminho_completo_destino = os.path.join(PathfileTask, novo_nome)

                        # Renomear o arquivo extraído
                        os.rename(os.path.join(PathfileTask, caminho_interno_arquivo), caminho_completo_destino)

                        print(f"'{arquivo_a_extrair}' extraído e renomeado para '{novo_nome}' ({arquivo_zip})")
                    except (FileNotFoundError, PermissionError) as e:
                        print(f"Erro ao renomear '{arquivo_a_extrair}' no arquivo ZIP '{arquivo_zip}'. Detalhes: {e}")

                else:
                    print(f"'{arquivo_a_extrair}' não encontrado no arquivo ZIP '{arquivo_zip}'.")

        except zipfile.BadZipFile:
            print(f"Erro: O arquivo '{arquivo_zip}' não é um arquivo ZIP válido.")
        except Exception as e:
            print(f"Erro inesperado ao processar o arquivo ZIP '{arquivo_zip}'. Detalhes: {e}")

if __name__ == "__main__":
    # Validar argumentos de linha de comando
    if len(sys.argv) != 3:
        print("Uso: python map_51_ExtrairMtTask.py 684_UME_TRAFEGO Dimensional")
        print("     python map_51_ExtrairMtTask.py 684_UME_TRAFEGO Fatos")
        print("     python map_51_ExtrairMtTask.py 684_UME_TRAFEGO Gerl")
        sys.exit(1)

    onda = sys.argv[1]
    pasta = sys.argv[2]

    # Executar a extração dos arquivos
    ExtrairDimensional_bin(onda, pasta)