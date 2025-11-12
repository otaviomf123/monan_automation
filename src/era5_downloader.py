"""
ERA5 Downloader
===============

Modulo para download de dados do ECMWF ERA5 para o pipeline MONAN/MPAS
"""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

try:
    import cdsapi
except ImportError:
    cdsapi = None

from .config_loader import ConfigLoader


class ERA5Downloader:
    """Classe para download de dados ERA5 do ECMWF"""
    
    def __init__(self, config: ConfigLoader):
        """
        Inicializa o downloader ERA5
        
        Args:
            config: Objeto de configuracao
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Verificar se cdsapi esta disponivel
        if cdsapi is None:
            raise ImportError(
                "cdsapi nao esta instalado. Instale com: pip install 'cdsapi>=0.7.4'"
            )
        
        # Carregar configuracoes
        self.era5_config = config.get_era5_config()
        self.dates = config.get_dates()
        
        # Configuracoes padrao do ERA5
        self.pressure_levels = self.era5_config.get('pressure_levels', [
            '10', '30', '50', '70', '100', '150', '200', '250', '300', '350',
            '400', '500', '600', '650', '700', '750', '775', '800', '825',
            '850', '875', '900', '925', '950', '975', '1000'
        ])
        
        self.grid_resolution = self.era5_config.get('grid_resolution', '0.25/0.25')
        self.download_interval_hours = self.era5_config.get('download_interval_hours', 3)
        
        # Variaveis para niveis de pressao
        self.pl_variables = [
            'geopotential', 'relative_humidity', 'temperature',
            'u_component_of_wind', 'v_component_of_wind'
        ]
        
        # Variaveis para superficie (mantem todas as 20 variaveis)
        self.sl_variables = [
            '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_dewpoint_temperature',
            '2m_temperature', 'land_sea_mask', 'mean_sea_level_pressure',
            'sea_ice_cover', 'sea_surface_temperature', 'skin_temperature',
            'snow_density', 'snow_depth', 'soil_temperature_level_1',
            'soil_temperature_level_2', 'soil_temperature_level_3', 'soil_temperature_level_4',
            'surface_pressure', 'volumetric_soil_water_layer_1', 'volumetric_soil_water_layer_2',
            'volumetric_soil_water_layer_3', 'volumetric_soil_water_layer_4'
        ]
        
        # Inicializar cliente CDS
        self._init_cds_client()
    
    def _init_cds_client(self) -> None:
        """
        Inicializa o cliente CDS API
        
        Raises:
            RuntimeError: Se nao conseguir inicializar o cliente
        """
        try:
            self.client = cdsapi.Client()
            self.logger.info("Cliente CDS API inicializado com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao inicializar cliente CDS API: {e}")
            self.logger.error("Verifique se o arquivo $HOME/.cdsapirc esta configurado corretamente")
            self.logger.error("Instrucoes: https://cds.climate.copernicus.eu/how-to-api")
            raise RuntimeError(f"Falha ao inicializar cliente CDS API: {e}")
    
    def _generate_hourly_timestamps(self) -> List[datetime]:
        """
        Gera lista de timestamps para download baseado nas configuracoes
        
        Returns:
            Lista de timestamps para download
        """
        start_time = datetime.strptime(self.dates['start_time'], '%Y-%m-%d_%H:%M:%S')
        end_time = datetime.strptime(self.dates['end_time'], '%Y-%m-%d_%H:%M:%S')
        
        timestamps = []
        current_time = start_time
        
        while current_time <= end_time:
            timestamps.append(current_time)
            current_time += timedelta(hours=self.download_interval_hours)
        
        self.logger.info(f"Gerados {len(timestamps)} timestamps para download")
        self.logger.debug(f"Intervalo: {self.download_interval_hours} horas")
        return timestamps
    
    def _download_pressure_levels(self, dt: datetime, output_dir: Path) -> Path:
        """
        Baixa dados dos niveis de pressao para uma hora especifica
        
        Args:
            dt: Timestamp dos dados
            output_dir: Diretorio de saida
            
        Returns:
            Caminho do arquivo baixado
        """
        filename = f"era5_pl_{dt.strftime('%Y%m%d_%H')}.grib"
        file_path = output_dir / filename
        
        self.logger.info(f"Baixando niveis de pressao: {filename}")
        
        try:
            self.client.retrieve(
                'reanalysis-era5-pressure-levels',
                {
                    'product_type': 'reanalysis',
                    'format': 'grib',
                    'grid': self.grid_resolution,
                    'variable': self.pl_variables,
                    'pressure_level': self.pressure_levels,
                    'year': f'{dt.year:04d}',
                    'month': f'{dt.month:02d}',
                    'day': f'{dt.day:02d}',
                    'time': f'{dt.hour:02d}:00',
                },
                str(file_path)
            )
            
            self.logger.info(f"SUCCESS: Baixado {filename}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"FAILED: Erro ao baixar {filename}: {e}")
            raise
    
    def _download_single_levels(self, dt: datetime, output_dir: Path) -> Path:
        """
        Baixa dados de superficie para uma hora especifica
        
        Args:
            dt: Timestamp dos dados
            output_dir: Diretorio de saida
            
        Returns:
            Caminho do arquivo baixado
        """
        filename = f"era5_sfc_{dt.strftime('%Y%m%d_%H')}.grib"
        file_path = output_dir / filename
        
        self.logger.info(f"Baixando dados de superficie: {filename}")
        
        try:
            self.client.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'format': 'grib',
                    'grid': self.grid_resolution,
                    'variable': self.sl_variables,
                    'year': f'{dt.year:04d}',
                    'month': f'{dt.month:02d}',
                    'day': f'{dt.day:02d}',
                    'time': f'{dt.hour:02d}:00',
                },
                str(file_path)
            )
            
            self.logger.info(f"SUCCESS: Baixado {filename}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"FAILED: Erro ao baixar {filename}: {e}")
            raise
    
    def download_era5_data(self, output_dir: Path) -> bool:
        """
        Baixa todos os dados ERA5 necessarios
        
        Args:
            output_dir: Diretorio de saida (equivalente ao ic/ directory)
            
        Returns:
            True se todos os downloads foram bem-sucedidos, False caso contrario
        """
        self.logger.info("="*50)
        self.logger.info("INICIANDO DOWNLOAD DE DADOS ERA5")
        self.logger.info("="*50)
        
        # Criar diretorio de saida
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Gerar timestamps
        timestamps = self._generate_hourly_timestamps()
        
        if not timestamps:
            self.logger.error("FAILED: Nenhum timestamp gerado para download")
            return False
        
        self.logger.info(f"Baixando dados ERA5 para {len(timestamps)} horarios")
        self.logger.info(f"Intervalo: {self.download_interval_hours} horas")
        self.logger.info(f"Grid: {self.grid_resolution}")
        self.logger.info(f"Niveis de pressao: {len(self.pressure_levels)}")
        
        success_count = 0
        total_files = len(timestamps) * 2  # pressure levels + surface para cada timestamp
        
        for i, dt in enumerate(timestamps, 1):
            self.logger.info(f"Processando timestamp {i}/{len(timestamps)}: {dt}")
            
            try:
                # Download pressure levels
                pl_file = self._download_pressure_levels(dt, output_dir)
                success_count += 1
                
                # Download surface data
                sfc_file = self._download_single_levels(dt, output_dir)
                success_count += 1
                
                self.logger.info(f"Concluido {i}/{len(timestamps)} timestamps")
                
            except Exception as e:
                self.logger.error(f"FAILED: Erro no timestamp {dt}: {e}")
                # Continuar com proximo timestamp
                continue
        
        # Relatorio final
        self.logger.info("="*50)
        self.logger.info("RESUMO DO DOWNLOAD ERA5")
        self.logger.info("="*50)
        self.logger.info(f"Arquivos baixados: {success_count}/{total_files}")
        self.logger.info(f"Taxa de sucesso: {(success_count/total_files)*100:.1f}%")
        
        if success_count == total_files:
            self.logger.info("SUCCESS: Todos os dados ERA5 foram baixados")
            return True
        elif success_count > 0:
            self.logger.warning(f"WARNING: Apenas {success_count}/{total_files} arquivos baixados")
            return False
        else:
            self.logger.error("FAILED: Nenhum arquivo foi baixado")
            return False
    
    def verify_downloads(self, data_dir: Path) -> List[str]:
        """
        Verifica se todos os arquivos esperados foram baixados
        
        Args:
            data_dir: Diretorio com os dados baixados
            
        Returns:
            Lista de arquivos faltando (vazia se todos estao presentes)
        """
        missing_files = []
        timestamps = self._generate_hourly_timestamps()
        
        for dt in timestamps:
            # Verificar arquivo de niveis de pressao
            pl_file = data_dir / f"era5_pl_{dt.strftime('%Y%m%d_%H')}.grib"
            if not pl_file.exists():
                missing_files.append(str(pl_file))
            
            # Verificar arquivo de superficie
            sfc_file = data_dir / f"era5_sfc_{dt.strftime('%Y%m%d_%H')}.grib"
            if not sfc_file.exists():
                missing_files.append(str(sfc_file))
        
        if missing_files:
            self.logger.warning(f"Arquivos faltando: {len(missing_files)}")
            for file in missing_files[:10]:  # Mostrar apenas os primeiros 10
                self.logger.warning(f"  - {file}")
            if len(missing_files) > 10:
                self.logger.warning(f"  ... e mais {len(missing_files) - 10} arquivos")
        else:
            self.logger.info("Todos os arquivos ERA5 estao presentes")
        
        return missing_files