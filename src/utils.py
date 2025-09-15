"""
Utilitários
===========

Funções utilitárias para o pipeline MONAN/MPAS
"""

import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional


def setup_logging(level: int = logging.INFO, 
                 log_file: Optional[str] = None,
                 format_str: Optional[str] = None) -> None:
    """
    Configura o sistema de logging
    
    Args:
        level: Nível de logging
        log_file: Arquivo de log (opcional)
        format_str: Formato das mensagens
    """
    if format_str is None:
        format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Configuração básica
    logging.basicConfig(
        level=level,
        format=format_str,
        handlers=[]
    )
    
    # Handler para console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(format_str)
    console_handler.setFormatter(console_formatter)
    
    # Handler para arquivo (se especificado)
    handlers = [console_handler]
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_formatter = logging.Formatter(format_str)
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)
    
    # Configurar root logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    for handler in handlers:
        root_logger.addHandler(handler)


def create_directory_structure(base_dir: Path, run_date: str) -> Dict[str, Path]:
    """
    Cria a estrutura de diretórios necessária
    
    Args:
        base_dir: Diretório base
        run_date: Data da rodada (YYYYMMDD)
        
    Returns:
        Dicionário com os caminhos criados
    """
    logger = logging.getLogger(__name__)
    
    # Diretório principal da rodada
    run_dir = base_dir / run_date
    
    # Subdiretórios
    directories = {
        'main': run_dir,
        'gfs': run_dir / 'gfs',
        'init': run_dir / 'init',
        'boundary': run_dir / 'bound',
        'run': run_dir / 'run'
    }
    
    # Criar diretórios
    for name, path in directories.items():
        path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"[DEBUG] Directory created/verified: {path}")
    
    logger.info(f"[INFO] Directory structure created at: {run_dir}")
    return directories


def create_symbolic_link(source: Path, target: Path, force: bool = True) -> bool:
    """
    Cria um link simbólico
    
    Args:
        source: Arquivo/diretório origem
        target: Local do link
        force: Remover link existente se necessário
        
    Returns:
        True se sucesso, False caso contrário
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Remover link existente se necessário
        if target.exists() or target.is_symlink():
            if force:
                target.unlink()
                logger.debug(f"Link existente removido: {target}")
            else:
                logger.warning(f"WARNING: Link already exists: {target}")
                return False
        
        # Criar link simbólico
        target.symlink_to(source)
        logger.debug(f"Link criado: {target} -> {source}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar link {target} -> {source}: {e}")
        return False


def run_command(command: str, cwd: Optional[Path] = None, 
               timeout: Optional[int] = None) -> tuple:
    """
    Executa um comando do sistema
    
    Args:
        command: Comando a ser executado
        cwd: Diretório de trabalho
        timeout: Timeout em segundos
        
    Returns:
        Tupla (return_code, stdout, stderr)
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Executando comando: {command}")
    if cwd:
        logger.debug(f"[DEBUG] Working directory: {cwd}")
    
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        
        if result.returncode == 0:
            logger.debug(f"[DEBUG] Command executed successfully (code: {result.returncode})")
        else:
            logger.error(f"ERROR: Command failed (code: {result.returncode})")
            logger.error(f"STDERR: {result.stderr}")
        
        return result.returncode, result.stdout, result.stderr
        
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout ({timeout}s) executando comando: {command}")
        return -1, "", "Timeout"
    
    except Exception as e:
        logger.error(f"Erro executando comando: {e}")
        return -1, "", str(e)


def write_namelist(filepath: Path, namelist_dict: Dict) -> bool:
    """
    Escreve um arquivo namelist do MPAS/WRF
    
    Args:
        filepath: Caminho do arquivo
        namelist_dict: Dicionário com as configurações
        
    Returns:
        True se sucesso, False caso contrário
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Ensure parent directory exists
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w') as f:
            for section, params in namelist_dict.items():
                f.write(f"&{section}\n")
                for key, value in params.items():
                    if isinstance(value, str):
                        f.write(f"    {key} = '{value}'\n")
                    elif isinstance(value, bool):
                        f.write(f"    {key} = .{str(value).lower()}.\n")
                    else:
                        f.write(f"    {key} = {value}\n")
                f.write("/\n\n")
        
        logger.debug(f"Namelist escrito: {filepath}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao escrever namelist {filepath}: {e}")
        logger.exception("Detalhes do erro:")
        return False


def write_streams_file(filepath: Path, streams_content: str) -> bool:
    """
    Escreve um arquivo de streams do MPAS
    
    Args:
        filepath: Caminho do arquivo
        streams_content: Conteúdo XML dos streams
        
    Returns:
        True se sucesso, False caso contrário
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Ensure parent directory exists
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w') as f:
            f.write(streams_content)
        
        logger.debug(f"Arquivo de streams escrito: {filepath}")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao escrever arquivo de streams {filepath}: {e}")
        logger.exception("Detalhes do erro:")
        return False


def calculate_end_time(start_time: str, forecast_days: int) -> str:
    """
    Calcula o tempo final baseado no tempo inicial e dias de previsão
    
    Args:
        start_time: Tempo inicial no formato 'YYYY-MM-DD_HH:MM:SS'
        forecast_days: Número de dias de previsão
        
    Returns:
        Tempo final no mesmo formato
    """
    # Parse do tempo inicial
    dt = datetime.strptime(start_time, '%Y-%m-%d_%H:%M:%S')
    
    # Adicionar dias de previsão
    end_dt = dt + timedelta(days=forecast_days)
    
    # Retornar no formato original
    return end_dt.strftime('%Y-%m-%d_%H:%M:%S')


def validate_file_exists(filepath: Path, min_size: int = 0) -> bool:
    """
    Valida se um arquivo existe e tem tamanho mínimo
    
    Args:
        filepath: Caminho do arquivo
        min_size: Tamanho mínimo em bytes
        
    Returns:
        True se válido, False caso contrário
    """
    if not filepath.exists():
        return False
    
    if filepath.stat().st_size < min_size:
        return False
    
    return True


def format_duration(seconds: int) -> str:
    """
    Formata duração em segundos para formato legível
    
    Args:
        seconds: Duração em segundos
        
    Returns:
        String formatada (ex: "2h 30m 45s")
    """
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    
    return " ".join(parts)


def get_file_size_mb(filepath: Path) -> float:
    """
    Retorna o tamanho do arquivo em MB
    
    Args:
        filepath: Caminho do arquivo
        
    Returns:
        Tamanho em MB
    """
    if not filepath.exists():
        return 0.0
    
    return filepath.stat().st_size / (1024 * 1024)


def copy_template_files(template_dir: Path, target_dir: Path, 
                       file_patterns: list) -> None:
    """
    Copia arquivos template para diretório alvo
    
    Args:
        template_dir: Diretório com templates
        target_dir: Diretório de destino
        file_patterns: Lista de padrões de arquivos
    """
    logger = logging.getLogger(__name__)
    
    for pattern in file_patterns:
        files = list(template_dir.glob(pattern))
        for file_path in files:
            target_path = target_dir / file_path.name
            target_path.write_text(file_path.read_text())
            logger.debug(f"Template copiado: {file_path.name}")


def check_executable_exists(executable_path: Path) -> bool:
    """
    Verifica se um executável existe e tem permissões
    
    Args:
        executable_path: Caminho do executável
        
    Returns:
        True se válido, False caso contrário
    """
    if not executable_path.exists():
        return False
    
    if not os.access(executable_path, os.X_OK):
        return False
    
    return True
