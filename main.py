#!/usr/bin/env python3
"""
MONAN/MPAS Runner - Script Principal
====================================

Este script executa o pipeline completo do MONAN/MPAS:
1. Download dos dados GFS
2. Processamento WPS (ungrib)
3. Geração das condições iniciais
4. Geração das condições de fronteira
5. Execução do modelo

Autor: Otavio Feitosa
Data: 2025
"""

import argparse
import logging
import sys
from pathlib import Path

from src.config_loader import ConfigLoader
from src.data_downloader import GFSDownloader
from src.wps_processor import WPSProcessor
from src.initial_conditions import InitialConditionsGenerator
from src.boundary_conditions import BoundaryConditionsGenerator
from src.model_runner import ModelRunner
from src.data_converter import MPASDataConverter
from src.utils import setup_logging, create_directory_structure


def main():
    """Função principal do pipeline MONAN/MPAS"""
    
    # Argumentos da linha de comando
    parser = argparse.ArgumentParser(description='MONAN/MPAS Pipeline Runner')
    parser.add_argument('--config', '-c', default='config.yml', 
                       help='Arquivo de configuração (default: config.yml)')
    parser.add_argument('--step', '-s', 
                       choices=['download', 'wps', 'init', 'boundary', 'run', 'convert', 'all'],
                       default='all',
                       help='Etapa específica para executar (default: all)')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Modo verboso')
    
    args = parser.parse_args()
    
    try:
        # Carregar configuração
        config = ConfigLoader(args.config)
        
        # Configurar logging
        log_level = logging.DEBUG if args.verbose else getattr(logging, config.get('logging.level', 'INFO'))
        setup_logging(
            level=log_level,
            log_file=config.get('logging.file', 'monan_execution.log'),
            format_str=config.get('logging.format')
        )
        
        logger = logging.getLogger(__name__)
        logger.info("="*60)
        logger.info("INICIANDO PIPELINE MONAN/MPAS")
        logger.info("="*60)
        
        # Criar estrutura de diretórios
        base_dir = Path(config.get('general.base_dir'))
        run_date = config.get('dates.run_date')
        
        dirs = create_directory_structure(base_dir, run_date)
        logger.info(f"[INFO] Directory structure created at: {base_dir}/{run_date}")
        
        # Executar etapas
        if args.step in ['download', 'all']:
            logger.info("ETAPA 1: Download dos dados GFS")
            downloader = GFSDownloader(config)
            if not downloader.download_gfs_data(dirs['gfs']):
                logger.error("FAILED: GFS download failed")
                sys.exit(1)
        
        if args.step in ['wps', 'all']:
            logger.info("ETAPA 2: Processamento WPS (ungrib)")
            wps = WPSProcessor(config)
            if not wps.process(dirs['gfs']):
                logger.error("FAILED: WPS processing failed")
                sys.exit(1)
        
        if args.step in ['init', 'all']:
            logger.info("STEP 3: Initial conditions generation")
            init_gen = InitialConditionsGenerator(config)
            if not init_gen.generate(dirs['init'], dirs['gfs']):
                logger.error("FAILED: Initial conditions generation failed")
                sys.exit(1)
        
        if args.step in ['boundary', 'all']:
            logger.info("STEP 4: Boundary conditions generation")
            boundary_gen = BoundaryConditionsGenerator(config)
            if not boundary_gen.generate(dirs['boundary'], dirs['init'], dirs['gfs']):
                logger.error("FAILED: Boundary conditions generation failed")
                sys.exit(1)
        
        if args.step in ['run', 'all']:
            logger.info("STEP 5: MONAN model execution")
            runner = ModelRunner(config)
            if not runner.run_model(dirs['run'], dirs['init'], dirs['boundary']):
                logger.error("FAILED: Model execution failed")
                sys.exit(1)
        
        if args.step in ['convert', 'all']:
            # Verificar se conversão está habilitada
            conversion_enabled = config.get('conversion.enabled', True)
            if conversion_enabled:
                logger.info("STEP 6: Conversion to regular grid")
                converter = MPASDataConverter(config)
                static_file = Path(config.get('paths.static_file'))
                converter.convert_all_diag_files(dirs['run'], static_file)
            else:
                logger.info("STEP 6: Conversion disabled in configuration")
        
        logger.info("="*60)
        logger.info("SUCCESS: MONAN/MPAS PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info("="*60)
        
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"ERRO FATAL: {e}")
        logger.exception("Detalhes do erro:")
        sys.exit(1)


if __name__ == "__main__":
    main()
