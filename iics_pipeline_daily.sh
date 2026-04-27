#!/bin/bash
################################################################################
# Script: iics_pipeline_daily.sh
# Descrição: Pipeline diário de ETL do IICS
# Execução: Diariamente às 2h da manhã
# Códigos: 684, 702, 781
################################################################################

# Configurações
set -e  # Sai se algum comando falhar
set -u  # Sai se variável não definida

# Diretórios
SCRIPT_DIR="/opt/engenharia/iicsIngest"
LOG_DIR="/opt/projetos/logExecucaoJobs"
DATA_DIR="/opt/projetos/origem"
TIMESTAMP=$(date +'%Y%m%d_%H%M%S')
LOG_FILE="${LOG_DIR}/iics_pipeline_${TIMESTAMP}.log"

# Códigos dos projetos
PROJECT_CODES=("684" "702" "781")

# Cores para output (se for executado interativamente)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m' # No Color
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
fi

# Criar diretório de logs se não existir
mkdir -p "${LOG_DIR}"

# Função para log
log() {
    local level=$1
    shift
    local message="$*"
    local timestamp=$(date +'%Y-%m-%d %H:%M:%S')
    
    case $level in
        "INFO")
            echo -e "${GREEN}[INFO]${NC} ${timestamp} - ${message}" | tee -a "${LOG_FILE}"
            ;;
        "WARN")
            echo -e "${YELLOW}[WARN]${NC} ${timestamp} - ${message}" | tee -a "${LOG_FILE}"
            ;;
        "ERROR")
            echo -e "${RED}[ERROR]${NC} ${timestamp} - ${message}" | tee -a "${LOG_FILE}"
            ;;
        "STEP")
            echo -e "${BLUE}[STEP]${NC} ${timestamp} - ${message}" | tee -a "${LOG_FILE}"
            ;;
        *)
            echo -e "${timestamp} - ${message}" | tee -a "${LOG_FILE}"
            ;;
    esac
}

# Função para executar script Python com verificação de erro
run_python_script() {
    local script=$1
    local description=$2
    local args=${3:-}
    local dependencies=${4:-}
    
    log "INFO" "▶️ Executando: ${description} (${script})"
    
    # Verificar dependências
    if [ -n "${dependencies}" ]; then
        for dep in "${dependencies[@]}"; do
            if [ ! -f "${SCRIPT_DIR}/${dep}" ]; then
                log "ERROR" "Dependência não encontrada: ${dep}"
                return 1
            fi
        done
    fi
    
    # Executar script
    cd "${SCRIPT_DIR}"
    
    if [ -n "${args}" ]; then
        python "${script}" ${args} 2>&1 | tee -a "${LOG_FILE}"
    else
        python "${script}" 2>&1 | tee -a "${LOG_FILE}"
    fi
    
    local exit_code=${PIPESTATUS[0]}
    
    if [ ${exit_code} -eq 0 ]; then
        log "INFO" "✅ ${description} concluído com sucesso"
        return 0
    else
        log "ERROR" "❌ ${description} falhou com código ${exit_code}"
        return 1
    fi
}

# Função para executar scripts em paralelo
run_parallel() {
    local scripts=("$@")
    local pids=()
    
    for script_info in "${scripts[@]}"; do
        IFS='|' read -r script description args <<< "${script_info}"
        log "INFO" "🚀 Iniciando em paralelo: ${description}"
        
        cd "${SCRIPT_DIR}"
        if [ -n "${args}" ]; then
            python "${script}" ${args} 2>&1 | tee -a "${LOG_FILE}" &
        else
            python "${script}" 2>&1 | tee -a "${LOG_FILE}" &
        fi
        pids+=($!)
    done
    
    # Aguardar todos os processos
    local failed=0
    for i in "${!pids[@]}"; do
        wait ${pids[$i]}
        if [ $? -ne 0 ]; then
            log "ERROR" "❌ Processo ${scripts[$i]} falhou"
            failed=1
        fi
    done
    
    return ${failed}
}

# Função para executar para múltiplos projetos
run_for_projects() {
    local script=$1
    local description=$2
    
    for code in "${PROJECT_CODES[@]}"; do
        log "INFO" "   Processando projeto ${code}"
        if ! run_python_script "${script}" "${description} - ${code}" "${code}"; then
            log "ERROR" "   Falha no projeto ${code}"
            return 1
        fi
    done
    return 0
}

# Função para limpar arquivos temporários antigos
cleanup_old_files() {
    log "STEP" "🧹 Limpando arquivos temporários antigos (mais de 7 dias)"
    find "${DATA_DIR}" -name "*.tmp" -type f -mtime +7 -delete 2>/dev/null || true
    find "${LOG_DIR}" -name "*.log" -type f -mtime +30 -delete 2>/dev/null || true
    log "INFO" "Limpeza concluída"
}

# Função para verificar espaço em disco
check_disk_space() {
    local required_gb=$1
    local available_gb=$(df -BG "${DATA_DIR}" | awk 'NR==2 {print $4}' | sed 's/G//')
    
    if [ ${available_gb} -lt ${required_gb} ]; then
        log "ERROR" "Espaço em disco insuficiente. Disponível: ${available_gb}GB, Necessário: ${required_gb}GB"
        return 1
    fi
    log "INFO" "Espaço em disco OK: ${available_gb}GB disponível"
    return 0
}

################################################################################
# PIPELINE PRINCIPAL
################################################################################

main() {
    local start_time=$(date +%s)
    
    log "STEP" "=========================================="
    log "STEP" "🚀 INICIANDO PIPELINE IICS - ${TIMESTAMP}"
    log "STEP" "=========================================="
    
    # Verificar espaço em disco (mínimo 10GB)
    if ! check_disk_space 10; then
        log "ERROR" "Espaço em disco insuficiente. Abortando pipeline."
        exit 1
    fi
    
    # Verificar diretórios
    if [ ! -d "${SCRIPT_DIR}" ]; then
        log "ERROR" "Diretório de scripts não encontrado: ${SCRIPT_DIR}"
        exit 1
    fi
    
    cd "${SCRIPT_DIR}"
    
    ############################################################################
    # ETAPA 1: EXTRATORES (SEM DEPENDÊNCIAS - PARALELO)
    ############################################################################
    log "STEP" ""
    log "STEP" "📦 ETAPA 1: EXTRATORES (Executando em paralelo)"
    log "STEP" "=========================================="
    
    # Lista de scripts para executar em paralelo (formato: script|descrição|args)
    local extractors=(
        "iics_maps_extractor.py|Extrator de Maps|"
        "iics_connection_extractor.py|Extrator de Conexões|"
        "iics_s_task_extractor.py|Extrator de Tasks|"
        "iics_file_record_extractor.py|Extrator de FileRecords|"
        "iics_ContentsofExportPackage.py|Extrator de ExportPackage|"
        "iics_wkf_item_entry_flow.py|Extrator de Workflow|"
    )
    
    # Executar para cada projeto
    for code in "${PROJECT_CODES[@]}"; do
        log "STEP" ""
        log "STEP" "📌 Processando projeto: ${code}"
        
        for extractor in "${extractors[@]}"; do
            IFS='|' read -r script desc args <<< "${extractor}"
            log "INFO" "   Executando: ${desc} para ${code}"
            if ! run_python_script "${script}" "${desc} - ${code}" "${code}"; then
                log "ERROR" "   Falha no extrator ${desc} para ${code}"
                # Continuar com outros extratores mesmo se um falhar
            fi
        done
    done
    
    ############################################################################
    # ETAPA 2: LOADERS (EXECUTAM APÓS EXTRATORES)
    ############################################################################
    log "STEP" ""
    log "STEP" "📦 ETAPA 2: LOADERS (Dependem dos extratores)"
    log "STEP" "=========================================="
    
    # 2.1 Load File Records (depende do iics_file_record_extractor.py)
    log "STEP" "2.1 - Load File Records"
    if ! run_python_script "load_file_records.py" "Load File Records"; then
        log "ERROR" "Load File Records falhou"
        exit 1
    fi
    
    # 2.2 Load Connections (depende do iics_connection_extractor.py)
    log "STEP" "2.2 - Load Connections"
    if ! run_python_script "load_connections.py" "Load Connections"; then
        log "ERROR" "Load Connections falhou"
        exit 1
    fi
    
    # 2.3 Load Map Content (depende do iics_maps_extractor.py)
    log "STEP" "2.3 - Load Map Content para projetos"
    for code in "${PROJECT_CODES[@]}"; do
        if ! run_python_script "load_map_content.py" "Load Map Content - ${code}" "${code}"; then
            log "ERROR" "Load Map Content falhou para ${code}"
            exit 1
        fi
    done
    
    ############################################################################
    # ETAPA 3: TRANSFORMATIONS (DEPENDEM DO LOAD_MAP_CONTENT)
    ############################################################################
    log "STEP" ""
    log "STEP" "📦 ETAPA 3: TRANSFORMATIONS"
    log "STEP" "=========================================="
    
    for code in "${PROJECT_CODES[@]}"; do
        current_code=${code}
        
        # 3.1 Load Map Transformation
        log "STEP" "3.1 - Load Map Transformation - ${current_code}"
        if ! run_python_script "load_map_transformation.py" "Load Map Transformation - ${current_code}" "${current_code}"; then
            log "ERROR" "Load Map Transformation falhou para ${current_code}"
            exit 1
        fi
        
        # 3.2 Load Transformation Session Properties
        log "STEP" "3.2 - Load Transformation Session Properties - ${current_code}"
        if ! run_python_script "load_map_transformation_session_properties.py" \
            "Load Transformation Session Properties - ${current_code}" "${current_code}"; then
            log "ERROR" "Load Transformation Session Properties falhou para ${current_code}"
            exit 1
        fi
        
        # 3.3 Load Transformation Advanced Properties
        log "STEP" "3.3 - Load Transformation Advanced Properties - ${current_code}"
        if ! run_python_script "load_map_transformation_advanced_properties.py" \
            "Load Transformation Advanced Properties - ${current_code}" "${current_code}"; then
            log "ERROR" "Load Transformation Advanced Properties falhou para ${current_code}"
            exit 1
        fi
        
        # 3.4 Load Transformation Data Adapter
        log "STEP" "3.4 - Load Transformation Data Adapter - ${current_code}"
        if ! run_python_script "load_map_transformation_data_adpter.py" \
            "Load Transformation Data Adapter - ${current_code}" "${current_code}"; then
            log "ERROR" "Load Transformation Data Adapter falhou para ${current_code}"
            exit 1
        fi
        
        # 3.5 Load Transformation Data Adapter Objects (depende do anterior)
        log "STEP" "3.5 - Load Transformation Data Adapter Objects - ${current_code}"
        if ! run_python_script "load_map_transformation_data_adpter_objects.py" \
            "Load Transformation Data Adapter Objects - ${current_code}" "${current_code}"; then
            log "ERROR" "Load Transformation Data Adapter Objects falhou para ${current_code}"
            exit 1
        fi
    done
    
    ############################################################################
    # ETAPA 4: TASKS (DEPENDEM DO IICS_S_TASK_EXTRACTOR)
    ############################################################################
    log "STEP" ""
    log "STEP" "📦 ETAPA 4: TASKS"
    log "STEP" "=========================================="
    
    # 4.1 Load S Task
    log "STEP" "4.1 - Load S Task"
    if ! run_python_script "load_s_task.py" "Load S Task"; then
        log "ERROR" "Load S Task falhou"
        exit 1
    fi
    
    # 4.2 Load S Task Session Properties List
    log "STEP" "4.2 - Load S Task Session Properties List"
    if ! run_python_script "load_s_task_sessionPropertiesList.py" \
        "Load S Task Session Properties List"; then
        log "ERROR" "Load S Task Session Properties List falhou"
        exit 1
    fi
    
    # 4.3 Load S Task Parameters
    log "STEP" "4.3 - Load S Task Parameters"
    if ! run_python_script "load_s_task_parameters.py" "Load S Task Parameters"; then
        log "ERROR" "Load S Task Parameters falhou"
        exit 1
    fi
    
    ############################################################################
    # LIMPEZA E FINALIZAÇÃO
    ############################################################################
    log "STEP" ""
    log "STEP" "🧹 ETAPA 5: LIMPEZA"
    log "STEP" "=========================================="
    
    cleanup_old_files
    
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    local hours=$((duration / 3600))
    local minutes=$(((duration % 3600) / 60))
    local seconds=$((duration % 60))
    
    log "STEP" "=========================================="
    log "STEP" "✨ PIPELINE CONCLUÍDO COM SUCESSO!"
    log "STEP" "⏱️  Tempo total: ${hours}h ${minutes}m ${seconds}s"
    log "STEP" "📁 Log salvo em: ${LOG_FILE}"
    log "STEP" "=========================================="
}

# Executar pipeline
main "$@"

# Código de saída
exit $?