#!/usr/bin/env python3
"""
Script de Verificacao do Setup MONAN/MPAS
==========================================

Verifica se todos os caminhos e executaveis estao configurados corretamente
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
    """Verifica se todos os executaveis existem e sao executaveis"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando executaveis...")
    
    executables = {
        'ungrib.exe': config.get('paths.ungrib_exe'),
        'init_atmosphere_model': config.get('paths.mpas_init_exe'),
        'atmosphere_model': config.get('paths.monan_exe'),
        'link_grib.csh': config.get('paths.link_grib')
    }
    
    all_ok = True
    for name, path in executables.items():
        if path is None:
            logger.error(f"ERROR: Caminho nao configurado: {name}")
            all_ok = False
            continue
            
        exe_path = Path(path)
        if check_executable_exists(exe_path):
            logger.info(f"SUCCESS: {name}: {path}")
        else:
            logger.error(f"ERROR: {name}: Nao encontrado ou sem permissao de execucao - {path}")
            all_ok = False
    
    return all_ok


def verify_data_files(config: ConfigLoader) -> bool:
    """Verifica se arquivos de dados essenciais existem"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando arquivos de dados...")
    
    data_files = {
        'Vtable.GFS': config.get('paths.vtable_gfs'),
        'Vtable.ECMWF': config.get('paths.vtable_ecmwf'),  # NEW: ERA5 support
        'brasil_circle.static.nc': config.get('paths.static_file'),
        'Diretorio geografico WPS': config.get('paths.wps_geog_path'),
        'Diretorio geografico MPAS': config.get('paths.geog_data_path')
    }
    
    all_ok = True
    for name, path in data_files.items():
        if path is None:
            logger.error(f"ERROR: Caminho nao configurado: {name}")
            all_ok = False
            continue
            
        file_path = Path(path)
        if file_path.exists():
            if file_path.is_file():
                size_mb = file_path.stat().st_size / (1024 * 1024)
                logger.info(f"SUCCESS: {name}: {path} ({size_mb:.1f} MB)")
            else:
                logger.info(f"SUCCESS: {name}: {path} (diretorio)")
        else:
            logger.error(f"ERROR: {name}: Nao encontrado - {path}")
            all_ok = False
    
    return all_ok


def verify_directories(config: ConfigLoader) -> bool:
    """Verifica se diretorios essenciais existem"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando diretorios...")
    
    directories = {
        'Base de trabalho': config.get('general.base_dir'),
        'WPS': config.get('paths.wps_dir'),
        'MONAN': config.get('paths.monan_dir')
    }
    
    all_ok = True
    for name, path in directories.items():
        if path is None:
            logger.error(f"ERROR: Caminho nao configurado: {name}")
            all_ok = False
            continue
            
        dir_path = Path(path)
        if dir_path.exists() and dir_path.is_dir():
            logger.info(f"SUCCESS: {name}: {path}")
        else:
            logger.error(f"ERROR: {name}: Diretorio nao encontrado - {path}")
            all_ok = False
    
    return all_ok


def verify_monan_files(config: ConfigLoader) -> bool:
    """Verifica se arquivos do MONAN (tabelas, etc.) existem"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando arquivos do MONAN...")
    
    monan_dir = Path(config.get('paths.monan_dir', ''))
    if not monan_dir.exists():
        logger.error(f"ERROR: Diretorio MONAN nao encontrado: {monan_dir}")
        return False
    
    # Verificar padroes de arquivos esperados
    required_patterns = ['*DBL', '*TBL', 'RRTMG_*']
    
    all_ok = True
    for pattern in required_patterns:
        files = list(monan_dir.glob(pattern))
        if files:
            logger.info(f"SUCCESS: Arquivos {pattern}: {len(files)} encontrados")
        else:
            logger.warning(f"WARNING:  Nenhum arquivo {pattern} encontrado em {monan_dir}")
            # Nao marca como erro fatal pois podem estar em subdiretorios
    
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
            logger.warning(f"WARNING:  Caminho nao configurado: {name}")
            continue
            
        file_path = Path(path)
        if file_path.exists():
            logger.info(f"SUCCESS: {name}: {path}")
            found_count += 1
        else:
            logger.warning(f"WARNING:  {name}: Nao encontrado - {path}")
    
    if found_count == 0:
        logger.error("ERROR: Nenhum arquivo de streams encontrado")
        all_ok = False
    else:
        logger.info(f"INFO:  {found_count}/{len(stream_files)} arquivos de streams encontrados")
    
    return all_ok


def verify_conversion_dependencies() -> bool:
    """Verifica se dependencias para conversao estao disponiveis"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando dependencias para conversao...")
    
    if not CONVERSION_AVAILABLE:
        logger.error("ERROR: Dependencias para conversao nao encontradas")
        logger.error("   Instale: pip install xarray numpy scikit-learn netCDF4")
        return False
    
    logger.info("SUCCESS: Dependencias para conversao disponiveis")
    return True


def verify_era5_dependencies(config: ConfigLoader) -> bool:
    """Verifica se dependencias para ERA5 estao disponiveis quando necessario"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando dependencias para ERA5...")
    
    data_source = config.get_data_source_type()
    
    if data_source != 'era5':
        logger.info("INFO: ERA5 nao esta configurado como fonte de dados")
        return True
    
    # Verificar se cdsapi esta disponivel
    try:
        import cdsapi
        logger.info("SUCCESS: cdsapi disponivel")
    except ImportError:
        logger.error("ERROR: cdsapi nao encontrado")
        logger.error("   Instale: pip install 'cdsapi>=0.7.4'")
        return False
    
    # Verificar se arquivo .cdsapirc existe
    import os
    cdsapirc_path = os.path.expanduser("~/.cdsapirc")
    if not Path(cdsapirc_path).exists():
        logger.error("ERROR: Arquivo ~/.cdsapirc nao encontrado")
        logger.error("   Configure sua conta CDS em: https://cds.climate.copernicus.eu/how-to-api")
        return False
    else:
        logger.info(f"SUCCESS: Arquivo ~/.cdsapirc encontrado: {cdsapirc_path}")
    
    # Verificar se Vtable.ECMWF esta configurado e existe
    vtable_ecmwf = config.get('paths.vtable_ecmwf')
    if not vtable_ecmwf:
        logger.error("ERROR: paths.vtable_ecmwf nao configurado para ERA5")
        return False
    
    if not Path(vtable_ecmwf).exists():
        logger.error(f"ERROR: Vtable.ECMWF nao encontrado: {vtable_ecmwf}")
        logger.error("   Certifique-se de que WPS esta instalado com Vtable.ECMWF")
        return False
    
    logger.info("SUCCESS: Dependencias ERA5 verificadas")
    return True


def verify_config_consistency(config: ConfigLoader) -> bool:
    """Verifica consistencia da configuracao"""
    
    logger = logging.getLogger(__name__)
    logger.info("Verificando consistencia da configuracao...")
    
    # Verificar se datas estao consistentes
    start_time = config.get('dates.start_time')
    end_time = config.get('dates.end_time')
    run_date = config.get('dates.run_date')
    
    if not all([start_time, end_time, run_date]):
        logger.error("ERROR: Configuracoes de data incompletas")
        return False
    
    # Verificar se run_date esta consistente com start_time
    if run_date not in start_time:
        logger.warning(f"WARNING:  run_date ({run_date}) pode nao estar consistente com start_time ({start_time})")
    
    # Verificar configuracoes de dominio MONAN
    domain_config = config.get_domain_config()
    # MONAN usa malha nao-estruturada, sem necessidade de configuracoes WRF (dx, dy, etc)
    if 'config_len_disp' not in domain_config:
        logger.warning("WARNING: config_len_disp nao configurado, usando padrao")
    else:
        logger.info(f"SUCCESS: config_len_disp configurado: {domain_config['config_len_disp']}m")
    
    # Verificar configuracoes de fisica
    physics_config = config.get_physics_config()
    required_physics_keys = ['nvertlevels', 'dt', 'physics_suite']
    
    missing_physics = [key for key in required_physics_keys if key not in physics_config]
    if missing_physics:
        logger.error(f"ERROR: Configuracoes de fisica faltantes: {missing_physics}")
        return False
    
    logger.info("SUCCESS: Configuracao consistente")
    return True


def main():
    """Funcao principal de verificacao"""
    
    # Configurar logging
    setup_logging(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    logger.info("="*60)
    logger.info("VERIFICACAO DO SETUP MONAN/MPAS")
    logger.info("="*60)
    
    try:
        # Carregar configuracao
        config = ConfigLoader('config.yml')
        logger.info(f"SUCCESS: Arquivo de configuracao carregado: config.yml")
        
        # Executar verificacoes
        checks = [
            ("Configuracao", verify_config_consistency),
            ("Diretorios", verify_directories),
            ("Executaveis", verify_executables),
            ("Arquivos de dados", verify_data_files),
            ("Arquivos MONAN", verify_monan_files),
            ("Arquivos de streams", verify_stream_files),
            ("Dependencias conversao", verify_conversion_dependencies),
            ("Dependencias ERA5", verify_era5_dependencies)  # NEW
        ]
        
        results = {}
        for check_name, check_func in checks:
            logger.info(f"\n--- {check_name.upper()} ---")
            results[check_name] = check_func(config)
        
        # Resumo final
        logger.info("\n" + "="*60)
        logger.info("RESUMO DA VERIFICACAO")
        logger.info("="*60)
        
        all_passed = True
        for check_name, passed in results.items():
            status = "SUCCESS: PASSOU" if passed else "ERROR: FALHOU"
            logger.info(f"{check_name}: {status}")
            if not passed:
                all_passed = False
        
        logger.info("="*60)
        
        if all_passed:
            logger.info("SUCCESS: TODAS AS VERIFICACOES PASSARAM!")
            logger.info("Sistema pronto para executar o MONAN/MPAS")
            return 0
        else:
            logger.error("ERROR: ALGUMAS VERIFICACOES FALHARAM")
            logger.error("Corrija os problemas antes de executar o pipeline")
            return 1
            
    except FileNotFoundError:
        logger.error("ERROR: Arquivo config.yml nao encontrado")
        logger.info("Execute: python setup.py para criar a configuracao inicial")
        return 1
    except Exception as e:
        logger.error(f"ERROR: Erro durante verificacao: {e}")
        logger.exception("Detalhes do erro:")
        return 1


if __name__ == "__main__":
    sys.exit(main())
