# IICS Ingest - Pipeline de Extração e Carga de Dados do Informatica IICS
[![license](https://img.shields.io/badge/license-%20ePanelinha-orange)](http://www.e-panelinha.com.br/)
[![python](https://img.shields.io/badge/python-3v-blue)]() 
[![ci/cd](https://img.shields.io/badge/ci/cd-github-white)]()    
[![airflow](https://img.shields.io/badge/AirFlow-Hetzner-Yellow)](https://airflow.apache.org/docs/apache-airflow/stable/)
[![cloud build](https://img.shields.io/badge/Build-Hetzner-Orange)]()
![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![MySQL](https://img.shields.io/badge/mysql-%2300f.svg?style=for-the-badge&logo=mysql&logoColor=white)
![Shell Script](https://img.shields.io/badge/shell_script-%23121011.svg?style=for-the-badge&logo=gnu-bash&logoColor=white)
![MIT License](https://img.shields.io/badge/license-MIT-green?style=for-the-badge)

## 📋 Visão Geral

Este repositório contém um pipeline robusto de scripts Python e Shell projetado para automatizar a **extração, processamento e carga (ETL)** de metadados e artefatos do **Informatica Intelligent Cloud Services (IICS)** para um banco de dados MySQL.

O sistema é capaz de processar diversos componentes do IICS, tais como:
*   **FileRecords** e **Transformations**
*   **Conexões** e **Tasks** (MTT)
*   **Taskflows** (via processamento de XML)
*   **Propriedades de Sessão** e **Advanced Properties**

---

## 🏗️ Arquitetura do Pipeline

O fluxo de dados segue uma estrutura linear de quatro etapas principais:

1.  **Extração**: Scripts Shell interagem com a API/CLI do IICS para exportar pacotes.
2.  **Descompressão**: Arquivos `.zip` exportados são processados para extração de JSONs e XMLs.
3.  **Processamento**: Scripts Python realizam o parsing, limpeza e transformação dos dados brutos.
4.  **Carga**: Os dados estruturados são inseridos no schema `fingerhard` do MySQL.

> **Fluxo:** IICS → Extração (Shell) → Processamento (Python) → MySQL (`fingerhard.*`)

---

## 📁 Estrutura do Projeto

```text
iicsIngest/
├── extract_iics.sh                 # Script principal de extração (Shell)
├── extract_iics_file_records.py    # Extrator de fileRecords
├── iics_s_task_extractor.py        # Extrator de s_task (sessions)
├── iics_maps_extractor.py          # Extrator de Mapas
├── extract_iics_connections.py     # Extrator de conexões
├── file_record_loader.py           # Loader de fileRecords para o MySQL
├── load_transformation.py          # Loader de transformations
├── load_session_properties.py      # Loader de session properties
├── load_advanced_properties.py     # Loader de advanced properties
├── load_data_adapter.py            # Loader de data adapters
├── load_data_adapter_objects.py    # Loader de objetos de data adapters
├── load_connections.py             # Loader de conexões
├── load_export_package.py          # Loader de pacotes de exportação
├── load_tasks.py                   # Loader de tasks
├── load_task_session_properties.py # Loader de session properties de tasks
├── load_task_parameters.py         # Loader de parâmetros de tasks
├── load_xml_taskflow.py            # Loader de taskflows (Parsing XML)
├── requirements.txt                # Dependências do projeto
├── .env.example                    # Modelo de variáveis de ambiente
└── README.md                       # Documentação do projeto
```

---

## 🚀 Componentes Detalhados

### 1. Scripts de Extração (Shell)
| Script | Função | Saída |
| :--- | :--- | :--- |
| `extract_iics.sh` | Exporta múltiplos códigos do IICS | Arquivos `.zip` no diretório de origem |

### 2. Extratores (Python)
| Script | Entrada | Saída |
| :--- | :--- | :--- |
| `extract_iics_file_records.py` | `.DTEMPLATE.zip` | `_fileRecords.json` |
| `extract_iics_binary.py` | `.DTEMPLATE.zip` | `.json` |
| `extract_iics_mtt_tasks.py` | `.MTT.zip` | `_mtt.json` |
| `extract_iics_connections.py` | `.Connection.zip` | `.json` |

### 3. Loaders (Python)
| Script | Origem | Tabela Destino (MySQL) |
| :--- | :--- | :--- |
| `file_record_loader.py` | `_fileRecords.json` | `transformation_file_records` |
| `load_transformation.py` | Tabela `content` | `transformation` |
| `load_connections.py` | Arquivos JSON | `connections` |
| `load_tasks.py` | Arquivos JSON | `s_task` |
| `load_xml_taskflow.py` | Arquivos XML | `Item`, `Entry`, `Flow`, `Service`, etc. |

---

## 📦 Instalação e Configuração

### Pré-requisitos
*   **Python 3.8+**
*   **MySQL 5.7+** ou **MariaDB 10.2+**
*   Acesso e credenciais ao **Informatica IICS**

### Passo a Passo

1.  **Clonar o repositório:**
    ```bash
    git clone https://github.com/seu-usuario/iics-ingest.git
    cd iics-ingest
    ```

2.  **Instalar dependências:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configurar Variáveis de Ambiente:**
    Copie o arquivo de exemplo e preencha com suas credenciais:
    ```bash
    cp .env.example .env
    ```
    Edite o `.env`:
    ```env
    USUARIO_IICS=seu_usuario
    SENHA_IICS=sua_senha
    MYSQL_RG_HOST=localhost
    MYSQL_RG_USER=root
    MYSQL_RG_PASSWORD=senha
    MYSQL_RG_DATABASE=fingerhard
    ```

4.  **Configurar Caminhos (JSON):**
    Crie um arquivo `config.json` conforme definido no seu `.env` (ex: `/opt/engenharia/config/config.json`):
    ```json
    {
        "json_file_path": "/opt/projetos/origem",
        "PathFileRecordsGrava": "/opt/projetos/destino/file_records",
        "PathMapsGrava": "/opt/projetos/destino/maps"
    }
    ```

---

## 🔧 Como Usar

### Execução do Pipeline Completo
Você pode executar o fluxo manualmente seguindo esta ordem:

1.  **Extração Inicial:**
    ```bash
    ./extract_iics.sh
    ```

2.  **Processamento de Metadados:**
    ```bash
    python extract_iics_file_records.py 653
    python file_record_loader.py
    ```

3.  **Carga de Transformações e Tasks:**
    ```bash
    python load_transformation.py 653
    python load_tasks.py
    ```

4.  **Processamento de Taskflows (XML):**
    ```bash
    python load_xml_taskflow.py 653
    ```

---

## 📊 Estrutura do Banco de Dados (Schema: `fingerhard`)

| Tabela | Descrição |
| :--- | :--- |
| `transformation` | Dados principais das transformações IICS |
| `connections` | Detalhes de conexões configuradas |
| `s_task` | Metadados de Tasks do IICS |
| `Item` / `Flow` | Estrutura lógica dos Taskflows extraídos do XML |

---

## 🐛 Troubleshooting

*   **Erro de Permissão**: Certifique-se de dar permissão de execução ao script shell: `chmod +x extract_iics.sh`.
*   **Conexão MySQL**: Valide se o host e porta estão acessíveis a partir do servidor de execução.
*   **Logs**: Os scripts imprimem logs no console. Use `python script.py 2>&1 | tee log.txt` para salvar a saída.

---

## 📝 Contribuição

1.  Faça um **Fork** do projeto.
2.  Crie uma **Branch** para sua feature (`git checkout -b feature/nova-feature`).
3.  Dê um **Commit** em suas mudanças (`git commit -m 'Adiciona nova feature'`).
4.  Dê um **Push** na Branch (`git push origin feature/nova-feature`).
5.  Abra um **Pull Request**.

---

## 📄 Licença

Este projeto está sob a licença **MIT**. Consulte o arquivo `LICENSE` para mais informações.

---
**Desenvolvido por:** - [Greg Oak](https://github.com/greg.ia)