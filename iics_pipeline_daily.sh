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
BASE_DIR="/opt/engenharia/iicsIngest/hml"
LOG_DIR="/opt/projetos/logExecucaoJobs"
TIMESTAMP=$(date +'%Y%m%d_%H%M%S')
LOG_FILE="${LOG_DIR}/iics_pipeline.log"

# Python do ambiente virtual
PYTHON_BIN="${BASE_DIR}/venv/bin/python"

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
    RED=''; GREEN=''; YELLOW=''; BLUE=''; NC=''
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

# Verificar se ambiente virtual existe
check_venv() {
    if [ ! -f "${PYTHON_BIN}" ]; then
        log "ERROR" "Ambiente virtual não encontrado em: ${PYTHON_BIN}"
        log "ERROR" "Execute: cd ${BASE_DIR} && python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
        exit 1
    fi
    log "INFO" "✅ Ambiente virtual encontrado: ${PYTHON_BIN}"
}

# Função para executar script Python
run_python_script() {
    local script=$1
    local description=$2
    local args=${3:-}
    
    log "INFO" "▶️ Executando: ${description}"
    
    cd "${BASE_DIR}"
    
    if [ -n "${args}" ]; then
        ${PYTHON_BIN} "${script}" ${args} 2>&1 | tee -a "${LOG_FILE}"
    else
        ${PYTHON_BIN} "${script}" 2>&1 | tee -a "${LOG_FILE}"
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

# Função para executar script para múltiplos projetos
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

# Função para executar scripts que não recebem parâmetros
run_no_args() {
    local script=$1
    local description=$2
    
    if ! run_python_script "${script}" "${description}"; then
        log "ERROR" "❌ ${description} falhou"
        return 1
    fi
    return 0
}

# Verificar espaço em disco (opcional)
check_disk_space() {
    local required_gb=$1
    local data_dir="/opt/projetos/origem"
    local available_gb=$(df -BG "${data_dir}" 2>/dev/null | awk 'NR==2 {print $4}' | sed 's/G//' || echo "0")
    
    if [ -z "${available_gb}" ] || [ ${available_gb} -lt ${required_gb} ]; then
        log "WARN" "Espaço em disco: ${available_gb}GB disponível (mínimo recomendado: ${required_gb}GB)"
        return 0  # Não falha por espaço insuficiente, apenas avisa
    fi
    log "INFO" "Espaço em disco OK: ${available_gb}GB disponível"
    return 0
}

# Limpar arquivos temporários antigos
cleanup_old_files() {
    log "STEP" "🧹 Limpando arquivos temporários antigos (mais de 7 dias)"
    find "/opt/projetos/origem" -name "*.tmp" -type f -mtime +7 -delete 2>/dev/null || true
    find "${LOG_DIR}" -name "*.log" -type f -mtime +30 -delete 2>/dev/null || true
    log "INFO" "Limpeza concluída"
}

################################################################################
# PIPELINE PRINCIPAL
################################################################################

main() {
    local start_time=$(date +%s)
    
    log "STEP" "=========================================="
    log "STEP" "🚀 INICIANDO PIPELINE IICS - ${TIMESTAMP}"
    log "STEP" "=========================================="
    
    # Verificar ambiente virtual
    check_venv
    
    # Verificar espaço em disco (mínimo 10GB)
    check_disk_space 10
    
    # Verificar diretório base
    if [ ! -d "${BASE_DIR}" ]; then
        log "ERROR" "Diretório base não encontrado: ${BASE_DIR}"
        exit 1
    fi
    
    cd "${BASE_DIR}"
    
    ############################################################################
    # ETAPA 1: EXTRATORES (SEM DEPENDÊNCIAS)
    ############################################################################
    log "STEP" ""
    log "STEP" "📦 ETAPA 1: EXTRATORES"
    log "STEP" "=========================================="
    
    for code in "${PROJECT_CODES[@]}"; do
        log "STEP" ""
        log "STEP" "📌 Processando projeto: ${code}"
        
        # Executar extratores para o projeto
        log "INFO" "   Executando iics_maps_extractor.py"
        run_python_script "iics_maps_extractor.py" "Extrator de Maps" "${code}" || true
        
        log "INFO" "   Executando iics_connection_extractor.py"
        run_python_script "iics_connection_extractor.py" "Extrator de Conexões" "${code}" || true
        
        log "INFO" "   Executando iics_s_task_extractor.py"
        run_python_script "iics_s_task_extractor.py" "Extrator de Tasks" "${code}" || true
        
        log "INFO" "   Executando iics_file_record_extractor.py"
        run_python_script "iics_file_record_extractor.py" "Extrator de FileRecords" "${code}" || true
        
        log "INFO" "   Executando iics_ContentsofExportPackage.py"
        run_python_script "iics_ContentsofExportPackage.py" "Extrator de ExportPackage" "${code}" || true
        
        log "INFO" "   Executando iics_wkf_item_entry_flow.py"
        run_python_script "iics_wkf_item_entry_flow.py" "Extrator de Workflow" "${code}" || true
    done
    
    ############################################################################
    # ETAPA 2: LOADERS (DEPENDEM DOS EXTRATORES)
    ############################################################################
    log "STEP" ""
    log "STEP" "📦 ETAPA 2: LOADERS"
    log "STEP" "=========================================="
    
    # 2.1 Load File Records (depende do iics_file_record_extractor.py)
    log "STEP" "2.1 - Load File Records"
    run_no_args "load_file_records.py" "Load File Records" || exit 1
    
    # 2.2 Load Connections (depende do iics_connection_extractor.py)
    log "STEP" "2.2 - Load Connections"
    run_no_args "load_connections.py" "Load Connections" || exit 1
    
    # 2.3 Load Map Content (depende do iics_maps_extractor.py)
    log "STEP" "2.3 - Load Map Content"
    for code in "${PROJECT_CODES[@]}"; do
        run_python_script "load_map_content.py" "Load Map Content - ${code}" "${code}" || exit 1
    done
    
    ############################################################################
    # ETAPA 3: TRANSFORMATIONS (DEPENDEM DO LOAD_MAP_CONTENT)
    ############################################################################
    log "STEP" ""
    log "STEP" "📦 ETAPA 3: TRANSFORMATIONS"
    log "STEP" "=========================================="
    
    for code in "${PROJECT_CODES[@]}"; do
        log "STEP" ""
        log "STEP" "📌 Processando projeto: ${code}"
        
        # 3.1 Load Map Transformation
        run_python_script "load_map_transformation.py" "Load Map Transformation" "${code}" || exit 1
        
        # 3.2 Load Transformation Session Properties
        run_python_script "load_map_transformation_session_properties.py" "Load Session Properties" "${code}" || exit 1
        
        # 3.3 Load Transformation Advanced Properties
        run_python_script "load_map_transformation_advanced_properties.py" "Load Advanced Properties" "${code}" || abortar 1
        
        # 3.4 Load Transformation Data Adapter
        run_python_script "load_map_transformation_data_adpter.py" "Load Data Adapter" "${code}" || exit 1
        
        # 3.5 Load Transformation Data Adapter Objects
        run_python_script "load_map_transformation_data_adpter_objects.py" "Load Data Adapter Objects" "${code}" || exit 1
    done
    
    ############################################################################
    # ETAPA 4: TASKS (DEPENDEM DO IICS_S_TASK_EXTRACTOR)
    ############################################################################
    log "STEP" ""
    log "STEP" "📦 ETAPA 4: TASKS"
    log "STEP" "=========================================="
    
    # 4.1 Load S Task
    run_no_args "load_s_task.py" "Load S Task" || exit 1
    
    # 4.2 Load S Task Session Properties List
    run_no_args "load_s_task_sessionPropertiesList.py" "Load S Task Session Properties List" || exit 1
    
    # 4.3 Load S Task Parameters
    run_no_args "load_s_task_parameters.py" "Load S Task Parameters" || exit 1
    
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

exit 0