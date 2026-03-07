<!-- PROJECT SHIELDS -->
[![license](https://img.shields.io/badge/license-%20ePanelinha-orange)](http://www.e-panelinha.com.br/)
[![python](https://img.shields.io/badge/python-3v-blue)]() 
[![ci/cd](https://img.shields.io/badge/ci/cd-github-white)]()    
[![airflow](https://img.shields.io/badge/AirFlow-Hetzner-Yellow)](https://airflow.apache.org/docs/apache-airflow/stable/)
[![cloud build](https://img.shields.io/badge/Build-Hetzner-Orange)]()
# RuleGuardian - Sistema de Gerenciamento de Processos
## 📋 Sobre o Projeto

O **RuleGuardian** é um sistema web desenvolvido para gerenciar projetos, famílias de processos e seus respectivos processos em ambientes de Data Warehouse/ETL. A ferramenta permite o controle completo do ciclo de vida dos processos, desde o planejamento até a implementação, com recursos avançados de versionamento e logs de alterações.

### 🎯 Principais Funcionalidades

- **Gestão de Projetos**: Cadastro e acompanhamento de projetos ETL/DW
- **Famílias de Processos**: Organização hierárquica de processos por famílias
- **Controle de Processos**: Gerenciamento detalhado com informações técnicas
- **Automação de Nomenclatura**: Geração automática de nomes técnicos para processos
- **Sources/Targets**: Configuração de fontes e destinos de dados
- **Cálculo Automático**: Estimativa de horas por processo baseada em função e nível
- **Logs de Alterações**: Rastreamento completo de mudanças em funções VAL
- **Autenticação e Perfis**: Controle de acesso com níveis admin, líder e dev

## 🚀 Tecnologias Utilizadas

- **Backend**: Python 3, Flask
- **Frontend**: HTML5, CSS3, JavaScript
- **Banco de Dados**: MySQL
- **Infraestrutura**: Hetzner Cloud
- **Orquestração**: Apache Airflow
- **CI/CD**: GitHub Actions

## 📁 Estrutura do Projeto
```RuleGuardian/
├── app/
│ ├── RuleGuard.py # Aplicação principal Flask
│ └── templates/ # Templates HTML
│ ├── base.html # Template base
│ ├── dashboard.html # Página inicial
│ ├── login.html # Autenticação
│ ├── projetos/ # Gestão de projetos
│ │ ├── list.html
│ │ ├── form.html
│ │ └── detalhes.html
│ ├── familias/ # Gestão de famílias
│ │ ├── list.html
│ │ ├── form.html
│ │ └── detalhes.html
│ ├── processos/ # Gestão de processos
│ │ ├── list.html
│ │ ├── form.html
│ │ ├── detalhes.html
│ │ └── nomes_tecnicos.html
│ ├── source_target/ # Sources/Targets
│ │ └── form.html
│ └── usuarios/ # Gestão de usuários
│ ├── list.html
│ └── form.html
├── static/
│ ├── css/
│ │ └── style.css # Estilos da aplicação
│ └── js/
│ └── main.js # Scripts JavaScript
└── config/
└── config.json # Configurações da aplicação
```

## ⚙️ Instalação e Configuração

### Pré-requisitos

- Python 3.8+
- MySQL 5.7+
- Git

### Passos para Instalação

1. **Clone o repositório**

```bash
git clone https://github.com/seu-usuario/RuleGuardian.git
cd RuleGuardian
```

2. **Crie um ambiente virtual**

```bash
python -m venv venv
```
3. **Ative o ambiente virtual**

```bash
# Linux/Mac
source venv/bin/activate

# Windows
venv\Scripts\activate
4. **Instale as dependências
```
```bash
pip install -r requirements.txt
```
5. **Configure o banco de dados**

´´´
sql
CREATE DATABASE RuleGuard;
CREATE USER 'ruleguard_app'@'localhost' IDENTIFIED BY 'RuleGuard2024!@#';
GRANT ALL PRIVILEGES ON RuleGuard.* TO 'ruleguard_app'@'localhost';
FLUSH PRIVILEGES;
´´´
6. **Execute as migrations**

```bash
mysql -u ruleguard_app -p RuleGuard < database/schema.sql
```

7. **Configure as variáveis de ambiente**

```bash
export SECRET_KEY="sua-chave-secreta"
export MYSQL_RG_HOST="localhost"
export MYSQL_RG_USER="ruleguard_app"
export MYSQL_RG_PASSWORD="RuleGuard2024!@#"
export MYSQL_RG_DATABASE="RuleGuard"
```
8. **Inicie a aplicação**

```bash
python app/RuleGuard.py
Acesse: http://localhost:9000
```
**🔧 Configuração de Email (para recuperação de senha)**
```bash
export GMAIL_APP_SMTP_SERVER="smtp.gmail.com"
export GMAIL_APP_SMTP_PORT="587"
export GMAIL_APP_SMTP_USERNAME="seu-email@gmail.com"
export GMAIL_APP_PASSWORD="sua-senha-app"
export GMAIL_APP_EMAIL_FROM="noreply@ruleguardian.com"
```

**🗄️ Modelo de Dados**
- **Principais Tabelas**
- **Tabela	Descrição**
- **usuarios	Usuários do sistema**
- **projetos	Projetos ETL/DW**
- **familias_projetos	Famílias de processos**
- **processos_familias	Processos individuais**
- **processos_source_target	Fontes e destinos de dados**
- **processos_familias_log	Logs de alterações**
- **ponto_de_funcao	Tabela para cálculo de horas**


**Relacionamentos**
- **Um Projeto pode ter várias Famílias**

- **Uma Família pode ter vários Processos**

- **m Processo pode ter vários Sources/Targets**

- **Um Processo pode ter vários Logs de alter**




