#!/usr/bin/env python3
"""
Script de Verificação do Setup MONAN/MPAS
==========================================

Verifica se todos os caminhos e executáveis estão configurados corretamente
"""

import logging
from pathlib import Path
import sys

from src.config_loader import ConfigLoader
from src.utils import setup_logging, check_executable_exists, validate_file_exists

try:
    import xarray as xr
    import numpy as np
    import sklearn
    CONVERSION_AVAILABLE = True
except ImportError:
    CONVERSION_AVAILABLE = False


def verify_executables(config: ConfigLoader) -> bool:
    """Verifica se todos os executáveis existem e são executáveis"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando executáveis...")
    
    executables = {
        'ungrib.exe': config.get('paths.ungrib_exe'),
        'init_atmosphere_model': config.get('paths.mpas_init_exe'),
        'atmosphere_model': config.get('paths.monan_exe'),
        'link_grib.csh': config.get('paths.link_grib')
    }
    
    all_ok = True
    for name, path in executables.items():
        if path is None:
            logger.error(f"ERROR: Caminho não configurado: {name}")
            all_ok = False
            continue
            
        exe_path = Path(path)
        if check_executable_exists(exe_path):
            logger.info(f"SUCCESS: {name}: {path}")
        else:
            logger.error(f"ERROR: {name}: Não encontrado ou sem permissão de execução - {path}")
            all_ok = False
    
    return all_ok


def verify_data_files(config: ConfigLoader) -> bool:
    """Verifica se arquivos de dados essenciais existem"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando arquivos de dados...")
    
    data_files = {
        'Vtable.GFS': config.get('paths.vtable_gfs'),
        'brasil_circle.static.nc': config.get('paths.static_file'),
        'Diretório geográfico WPS': config.get('paths.wps_geog_path'),
        'Diretório geográfico MPAS': config.get('paths.geog_data_path')
    }
    
    all_ok = True
    for name, path in data_files.items():
        if path is None:
            logger.error(f"ERROR: Caminho não configurado: {name}")
            all_ok = False
            continue
            
        file_path = Path(path)
        if file_path.exists():
            if file_path.is_file():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                logger.info(f"SUCCESS: {name}: {path} ({size_mb:.1f} MB)")
            else:
                logger.info(f"SUCCESS: {name}: {path} (diretório)")
        else:
            logger.error(f"ERROR: {name}: Não encontrado - {path}")
            all_ok = False
    
    return all_ok


def verify_directories(config: ConfigLoader) -> bool:
    """Verifica se diretórios essenciais existem"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando diretórios...")
    
    directories = {
        'Base de trabalho': config.get('general.base_dir'),
        'WPS': config.get('paths.wps_dir'),
        'MONAN': config.get('paths.monan_dir')
    }
    
    all_ok = True
    for name, path in directories.items():
        if path is None:
            logger.error(f"ERROR: Caminho não configurado: {name}")
            all_ok = False
            continue
            
        dir_path = Path(path)
        if dir_path.exists() and dir_path.is_dir():
            logger.info(f"SUCCESS: {name}: {path}")
        else:
            logger.error(f"ERROR: {name}: Diretório não encontrado - {path}")
            all_ok = False
    
    return all_ok


def verify_monan_files(config: ConfigLoader) -> bool:
    """Verifica se arquivos do MONAN (tabelas, etc.) existem"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando arquivos do MONAN...")
    
    monan_dir = Path(config.get('paths.monan_dir', ''))
    if not monan_dir.exists():
        logger.error(f"ERROR: Diretório MONAN não encontrado: {monan_dir}")
        return False
    
    # Verificar padrões de arquivos esperados
    required_patterns = ['*DBL', '*TBL', 'RRTMG_*']
    
    all_ok = True
    for pattern in required_patterns:
        files = list(monan_dir.glob(pattern))
        if files:
            logger.info(f"SUCCESS: Arquivos {pattern}: {len(files)} encontrados")
        else:
            logger.warning(f"WARNING:  Nenhum arquivo {pattern} encontrado em {monan_dir}")
            # Não marca como erro fatal pois podem estar em subdiretórios
    
    return all_ok


def verify_stream_files(config: ConfigLoader) -> bool:
    """Verifica se arquivos de streams existem"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando arquivos de streams...")
    
    stream_files = {
        'stream_list.atmosphere.diagnostics': config.get('paths.stream_diagnostics'),
        'stream_list.atmosphere.output': config.get('paths.stream_output'),
        'stream_list.atmosphere.surface': config.get('paths.stream_surface'),
        'streams.atmosphere': config.get('paths.streams_atmosphere')
    }
    
    all_ok = True
    found_count = 0
    
    for name, path in stream_files.items():
        if path is None:
            logger.warning(f"WARNING:  Caminho não configurado: {name}")
            continue
            
        file_path = Path(path)
        if file_path.exists():
            logger.info(f"SUCCESS: {name}: {path}")
            found_count += 1
        else:
            logger.warning(f"WARNING:  {name}: Não encontrado - {path}")
    
    if found_count == 0:
        logger.error("ERROR: Nenhum arquivo de streams encontrado")
        all_ok = False
    else:
        logger.info(f"INFO:  {found_count}/{len(stream_files)} arquivos de streams encontrados")
    
    return all_ok


def verify_conversion_dependencies() -> bool:
    """Verifica se dependências para conversão estão disponíveis"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando dependências para conversão...")
    
    if not CONVERSION_AVAILABLE:
        logger.error("ERROR: Dependências para conversão não encontradas")
        logger.error("   Instale: pip install xarray numpy scikit-learn netCDF4")
        return False
    
    logger.info("SUCCESS: Dependências para conversão disponíveis")
    return True


def verify_config_consistency(config: ConfigLoader) -> bool:
    """Verifica consistência da configuração"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando consistência da configuração...")
    
    # Verificar se datas estão consistentes
    start_time = config.get('dates.start_time')
    end_time = config.get('dates.end_time')
    run_date = config.get('dates.run_date')
    
    if not all([start_time, end_time, run_date]):
        logger.error("ERROR: Configurações de data incompletas")
        return False
    
    # Verificar se run_date está consistente com start_time
    if run_date not in start_time:
        logger.warning(f"WARNING:  run_date ({run_date}) pode não estar consistente com start_time ({start_time})")
    
    # Verificar configurações de domínio
    domain_config = config.get_domain_config()
    required_domain_keys = ['dx', 'dy', 'ref_lat', 'ref_lon', 'e_we', 'e_sn']
    
    missing_domain = [key for key in required_domain_keys if key not in domain_config]
    if missing_domain:
        logger.error(f"ERROR: Configurações de domínio faltantes: {missing_domain}")
        return False
    
    # Verificar configurações de física
    physics_config = config.get_physics_config()
    required_physics_keys = ['nvertlevels', 'dt', 'physics_suite']
    
    missing_physics = [key for key in required_physics_keys if key not in physics_config]
    if missing_physics:
        logger.error(f"ERROR: Configurações de física faltantes: {missing_physics}")
        return False
    
    logger.info("SUCCESS: Configuração consistente")
    return True


def main():
    """Função principal de verificação"""
    
    # Configurar logging
    setup_logging(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("VERIFICAÇÃO DO SETUP MONAN/MPAS")
    logger.info("="*60)
    
    try:
        # Carregar configuração
        config = ConfigLoader('config.yml')
        logger.info(f"SUCCESS: Arquivo de configuração carregado: config.yml")
        
        # Executar verificações
        checks = [
            ("Configuração", verify_config_consistency),
            ("Diretórios", verify_directories),
            ("Executáveis", verify_executables),
            ("Arquivos de dados", verify_data_files),
            ("Arquivos MONAN", verify_monan_files),
            ("Arquivos de streams", verify_stream_files),
            ("Dependências conversão", verify_conversion_dependencies)
        ]
        
        results = {}
        for check_name, check_func in checks:
            logger.info(f"\n--- {check_name.upper()} ---")
            results[check_name] = check_func(config)
        
        # Resumo final
        logger.info("\n" + "="*60)
        logger.info("RESUMO DA VERIFICAÇÃO")
        logger.info("="*60)
        
        all_passed = True
        for check_name, passed in results.items():
            status = "SUCCESS: PASSOU" if passed else "ERROR: FALHOU"
            logger.info(f"{check_name}: {status}")
            if not passed:
                all_passed = False
        
        logger.info("="*60)
        
        if all_passed:
            logger.info("SUCCESS: TODAS AS VERIFICAÇÕES PASSARAM!")
            logger.info("Sistema pronto para executar o MONAN/MPAS")
            return 0
        else:
            logger.error("ERROR: ALGUMAS VERIFICAÇÕES FALHARAM")
            logger.error("Corrija os problemas antes de executar o pipeline")
            return 1
            
    except FileNotFoundError:
        logger.error("ERROR: Arquivo config.yml não encontrado")
        logger.info("Execute: python setup.py para criar a configuração inicial")
        return 1
    except Exception as e:
        logger.error(f"ERROR: Erro durante verificação: {e}")
        logger.exception("Detalhes do erro:")
        return 1


if __name__ == "__main__":
    sys.exit(main())
