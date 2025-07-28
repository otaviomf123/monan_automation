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
from src.utils import setup_logging, create_directory_structure


def main():
    """Função principal do pipeline MONAN/MPAS"""
    
    # Argumentos da linha de comando
    parser = argparse.ArgumentParser(description='MONAN/MPAS Pipeline Runner')
    parser.add_argument('--config', '-c', default='config.yml', 
                       help='Arquivo de configuração (default: config.yml)')
    parser.add_argument('--step', '-s', 
                       choices=['download', 'wps', 'init', 'boundary', 'run', 'all'],
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
        logger.info(f"Estrutura de diretórios criada em: {base_dir}/{run_date}")
        
        # Executar etapas
        if args.step in ['download', 'all']:
            logger.info("ETAPA 1: Download dos dados GFS")
            downloader = GFSDownloader(config)
            downloader.download_gfs_data(dirs['gfs'])
        
        if args.step in ['wps', 'all']:
            logger.info("ETAPA 2: Processamento WPS (ungrib)")
            wps = WPSProcessor(config)
            wps.process(dirs['gfs'])
        
        if args.step in ['init', 'all']:
            logger.info("ETAPA 3: Geração das condições iniciais")
            init_gen = InitialConditionsGenerator(config)
            init_gen.generate(dirs['init'], dirs['gfs'])
        
        if args.step in ['boundary', 'all']:
            logger.info("ETAPA 4: Geração das condições de fronteira")
            boundary_gen = BoundaryConditionsGenerator(config)
            boundary_gen.generate(dirs['boundary'], dirs['init'], dirs['gfs'])
        
        if args.step in ['run', 'all']:
            logger.info("ETAPA 5: Execução do modelo MONAN")
            runner = ModelRunner(config)
            runner.run_model(dirs['run'], dirs['init'], dirs['boundary'])
        
        logger.info("="*60)
        logger.info("PIPELINE MONAN/MPAS CONCLUÍDO COM SUCESSO!")
        logger.info("="*60)
        
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.error(f"ERRO FATAL: {e}")
        logger.exception("Detalhes do erro:")
        sys.exit(1)


if __name__ == "__main__":
    main()
