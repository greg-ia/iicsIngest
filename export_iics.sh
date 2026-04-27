#!/bin/bash

# Script para exportar múltiplos códigos do IICS
# Uso: ./export_iics.sh

# Configurações
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TIMESTAMP=$(date +'%Y%m%d%H%M%S')
LOG_FILE="/opt/projetos/logExecucaoJobs/iics_export_${TIMESTAMP}.log"

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Função para carregar variáveis do arquivo .env
load_env_file() {
    local env_file="${SCRIPT_DIR}/.env"
    
    if [ -f "$env_file" ]; then
        echo -e "${YELLOW}📄 Carregando variáveis do arquivo .env${NC}"
        
        # Carrega o arquivo .env linha por linha
        while IFS='=' read -r key value; do
            # Ignora linhas vazias e comentários
            if [[ -z "$key" ]] || [[ "$key" =~ ^# ]]; then
                continue
            fi
            
            # Remove espaços e aspas
            key=$(echo "$key" | xargs)
            value=$(echo "$value" | xargs | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")
            
            # Exporta a variável
            export "$key=$value"
        done < "$env_file"
        
        return 0
    else
        echo -e "${RED}❌ Arquivo .env não encontrado: ${env_file}${NC}"
        return 1
    fi
}

# Função para verificar se uma variável está configurada
check_var() {
    local var_name=$1
    local var_value=${!var_name}
    
    if [ -z "$var_value" ]; then
        echo -e "${RED}❌ Variável ${var_name} não configurada${NC}"
        return 1
    fi
    return 0
}

# Carregar variáveis do arquivo .env
load_env_file

# Verificar se as variáveis de ambiente estão configuradas
if [ -z "$USUARIO_IICS" ] || [ -z "$SENHA_IICS" ]; then
    echo -e "${RED}ERRO: Variáveis USUARIO_IICS e SENHA_IICS não configuradas${NC}"
    echo ""
    echo "Por favor, configure o arquivo .env com:"
    echo "  USUARIO_IICS='seu_usuario'"
    echo "  SENHA_IICS='sua_senha'"
    echo ""
    echo "Ou exporte as variáveis manualmente:"
    echo "  export USUARIO_IICS='seu_usuario'"
    echo "  export SENHA_IICS='sua_senha'"
    exit 1
fi

# Mostrar status das variáveis (ocultando senha)
echo -e "${GREEN}✅ Variáveis carregadas:${NC}"
echo "   USUARIO_IICS: ${USUARIO_IICS}"
echo "   SENHA_IICS: ********"

# Lista de códigos dos jobs
CODIGOS_JOB=(
    "684_UME_TRAFEGO"
    "702_UME_Disponibilidades"
    "781_CONEXAO_TORPEDO_NOVA_PLATAFORMA"
)

# Função para log
log() {
    echo -e "$1"
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $1" >> "$LOG_FILE"
}

# Função para verificar se houve erro
check_error() {
    local output="$1"
    local codigo="$2"
    
    if [[ "$output" == *"401 Unauthorized"* ]] || \
       [[ "$output" == *"ERRO"* ]] || \
       [[ "$(echo "$output" | tr '[:upper:]' '[:lower:]')" == *"error"* ]]; then
        log "${RED}✗ Falha detectada no código ${codigo}${NC}"
        log "${RED}Detalhes do erro:${NC}"
        log "$output"
        exit 1
    fi
}

# Cabeçalho
log "${GREEN}=== Iniciando exportação de dados IICS ===${NC}"
log "Timestamp: $TIMESTAMP"
log "Total de jobs: ${#CODIGOS_JOB[@]}"
log "Log salvando em: $LOG_FILE"
echo ""

# Limpar arquivos temporários antigos (opcional)
rm -rf /opt/projetos/origem/*.* 2>/dev/null || true

# Loop principal
for codigo in "${CODIGOS_JOB[@]}"; do
    log "${YELLOW}>>> Extraindo JSON da onda código ${codigo} Dimensional${NC}"
    
    # Executa o comando de exportação
    OUTPUT=$(/opt/api_iics/iics export \
        -n "${codigo}_${TIMESTAMP}" \
        -u "$USUARIO_IICS" \
        -p "$SENHA_IICS" \
        -r us \
        -a "Explore/${codigo}.Project" \
        -z "/opt/projetos/origem/${codigo}-${TIMESTAMP}.zip" \
        2>&1)
    
    EXIT_CODE=$?
    
    # Mostra saída do comando
    log "Saída do comando:"
    echo "$OUTPUT"
    echo ""
    
    # Verifica se houve erro no output
    check_error "$OUTPUT" "$codigo"
    
    # Verifica código de saída
    if [ $EXIT_CODE -ne 0 ]; then
        log "${RED}✗ Comando falhou para o código ${codigo} (exit code: $EXIT_CODE)${NC}"
        exit 1
    fi
    
    log "${GREEN}✓ Exportação do código ${codigo} concluída com sucesso${NC}"
    
    # Extrair o arquivo zip
    ZIP_FILE="/opt/projetos/origem/${codigo}-${TIMESTAMP}.zip"
    EXTRACT_DIR="/opt/projetos/origem/${codigo}"
    
    if [ -f "$ZIP_FILE" ]; then
        log "📦 Extraindo arquivo: ${ZIP_FILE}"
        unzip -q "$ZIP_FILE" -d "$EXTRACT_DIR"
        log "${GREEN}✓ Extração concluída para: ${EXTRACT_DIR}${NC}"
    else
        log "${RED}✗ Arquivo ZIP não encontrado: ${ZIP_FILE}${NC}"
        exit 1
    fi

    # Aguarda 10 segundos para evitar colisão e rate limit
    if [ "$codigo" != "${CODIGOS_JOB[-1]}" ]; then
        log "${YELLOW}⚠ Aguardando 10 segundos antes do próximo job...${NC}"
        sleep 10
    fi
    echo ""
done

# Sucesso final
log "${GREEN}========================================${NC}"
log "${GREEN}✓ Todos os ${#CODIGOS_JOB[@]} jobs foram executados com sucesso!${NC}"
log "${GREEN}========================================${NC}"
log "Log completo disponível em: $LOG_FILE"

exit 0