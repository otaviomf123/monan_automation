"""
MONAN/MPAS Runner
=================

Sistema automatizado para execução do modelo MONAN/MPAS no ambiente CEMPA.

Módulos:
    config_loader: Carregamento e gerenciamento de configurações
    data_downloader: Download de dados GFS do NOAA
    wps_processor: Processamento WPS (ungrib)
    initial_conditions: Geração de condições iniciais
    boundary_conditions: Geração de condições de fronteira
    model_runner: Execução do modelo MONAN/MPAS
    utils: Utilitários gerais

Autor: Otavio Feitosa
Data: 2025
"""

__version__ = "1.0.0"
__author__ = "Otavio Feitosa"
__email__ = "otavio.feitosa@cempa.br"

from .config_loader import ConfigLoader
from .data_downloader import GFSDownloader
from .wps_processor import WPSProcessor
from .initial_conditions import InitialConditionsGenerator
from .boundary_conditions import BoundaryConditionsGenerator
from .model_runner import ModelRunner
from .data_converter import MPASDataConverter

__all__ = [
    'ConfigLoader',
    'GFSDownloader', 
    'WPSProcessor',
    'InitialConditionsGenerator',
    'BoundaryConditionsGenerator',
    'ModelRunner',
    'MPASDataConverter'
]
