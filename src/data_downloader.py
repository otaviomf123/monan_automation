"""
Download de Dados GFS
=====================

Módulo para download automático dos dados GFS do NOAA
"""

import logging
import requests
from pathlib import Path
from typing import List
from tqdm import tqdm

from .config_loader import ConfigLoader


class GFSDownloader:
    """Classe para download dos dados GFS"""
    
    def __init__(self, config: ConfigLoader):
        """
        Inicializa o downloader
        
        Args:
            config: Objeto de configuração
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Configurações do GFS
        self.base_url = config.get('data_sources.gfs_base_url')
        self.run_date = config.get('dates.run_date')
        self.cycle = config.get('dates.cycle')
        
        # Horas de previsão
        forecast_config = config.get('data_sources.forecast_hours')
        self.forecast_hours = range(
            forecast_config['start'],
            forecast_config['end'] + 1,
            forecast_config['step']
        )
    
    def _generate_file_urls(self) -> List[tuple]:
        """
        Gera lista de URLs e nomes de arquivos para download
        
        Returns:
            Lista de tuplas (url, filename)
        """
        urls_and_files = []
        
        for fh in self.forecast_hours:
            fh_str = f"{fh:03d}"
            filename = f"gfs.t{self.cycle}z.pgrb2.0p25.f{fh_str}"
            url = f"{self.base_url}/gfs.{self.run_date}/{self.cycle}/atmos/{filename}"
            
            urls_and_files.append((url, filename))
        
        return urls_and_files
    
    def _download_file(self, url: str, filepath: Path) -> bool:
        """
        Download de um arquivo individual
        
        Args:
            url: URL do arquivo
            filepath: Caminho local para salvar
            
        Returns:
            True se sucesso, False se erro
        """
        try:
            self.logger.info(f"Baixando: {url}")
            
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            # Obter tamanho do arquivo para barra de progresso
            total_size = int(response.headers.get('content-length', 0))
            
            with open(filepath, 'wb') as f:
                with tqdm(
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    desc=filepath.name,
                    leave=False
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            
<<<<<<< HEAD
            self.logger.info(f"✓ Baixado com sucesso: {filepath.name}")
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"✗ Erro ao baixar {url}: {e}")
=======
            self.logger.info(f" Baixado com sucesso: {filepath.name}")
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f" Erro ao baixar {url}: {e}")
>>>>>>> Melhoramento da descricao, e add a parte de conversao para grade regular e escrita em arquivo netcdf
            # Remove arquivo parcial se existir
            if filepath.exists():
                filepath.unlink()
            return False
        
        except Exception as e:
<<<<<<< HEAD
            self.logger.error(f"✗ Erro inesperado ao baixar {url}: {e}")
=======
            self.logger.error(f" Erro inesperado ao baixar {url}: {e}")
>>>>>>> Melhoramento da descricao, e add a parte de conversao para grade regular e escrita em arquivo netcdf
            if filepath.exists():
                filepath.unlink()
            return False
    
    def download_gfs_data(self, output_dir: Path) -> bool:
        """
        Download de todos os arquivos GFS necessários
        
        Args:
            output_dir: Diretório de saída
            
        Returns:
            True se todos os downloads foram bem-sucedidos
        """
        self.logger.info(f"Iniciando download dos dados GFS para {self.run_date}/{self.cycle}")
        self.logger.info(f"Diretório de saída: {output_dir}")
        
        # Garantir que o diretório existe
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Gerar lista de arquivos
        urls_and_files = self._generate_file_urls()
        total_files = len(urls_and_files)
        
        self.logger.info(f"Total de arquivos para download: {total_files}")
        self.logger.info(f"Horas de previsão: {min(self.forecast_hours)}h a {max(self.forecast_hours)}h "
                        f"(intervalo de {self.forecast_hours.step}h)")
        
        # Contador de sucessos e falhas
        success_count = 0
        failed_files = []
        
        # Download dos arquivos
        for i, (url, filename) in enumerate(urls_and_files, 1):
            filepath = output_dir / filename
            
            # Pular se arquivo já existe e tem tamanho > 0
            if filepath.exists() and filepath.stat().st_size > 0:
                self.logger.info(f"[{i}/{total_files}] Arquivo já existe: {filename}")
                success_count += 1
                continue
            
            self.logger.info(f"[{i}/{total_files}] Baixando arquivo: {filename}")
            
            if self._download_file(url, filepath):
                success_count += 1
            else:
                failed_files.append(filename)
        
        # Relatório final
        self.logger.info("="*50)
        self.logger.info("RELATÓRIO DE DOWNLOAD")
        self.logger.info("="*50)
        self.logger.info(f"Total de arquivos: {total_files}")
        self.logger.info(f"Downloads bem-sucedidos: {success_count}")
        self.logger.info(f"Downloads com falha: {len(failed_files)}")
        
        if failed_files:
            self.logger.warning("Arquivos que falharam no download:")
            for filename in failed_files:
                self.logger.warning(f"  - {filename}")
        
        success = len(failed_files) == 0
        
        if success:
            self.logger.info("✓ Todos os arquivos foram baixados com sucesso!")
        else:
            self.logger.error("✗ Alguns arquivos falharam no download")
        
        return success
    
    def verify_downloads(self, data_dir: Path) -> List[str]:
        """
        Verifica se todos os arquivos necessários foram baixados
        
        Args:
            data_dir: Diretório com os dados
            
        Returns:
            Lista de arquivos faltantes
        """
        urls_and_files = self._generate_file_urls()
        missing_files = []
        
        for url, filename in urls_and_files:
            filepath = data_dir / filename
            
            if not filepath.exists():
                missing_files.append(filename)
                continue
            
            # Verificar se arquivo não está vazio
            if filepath.stat().st_size == 0:
                missing_files.append(filename)
                continue
        
        if missing_files:
            self.logger.warning(f"Arquivos faltantes ou corrompidos: {len(missing_files)}")
            for filename in missing_files:
                self.logger.warning(f"  - {filename}")
        else:
<<<<<<< HEAD
            self.logger.info("✓ Todos os arquivos GFS estão presentes e válidos")
=======
            self.logger.info("Todos os arquivos GFS estão presentes e válidos")
>>>>>>> Melhoramento da descricao, e add a parte de conversao para grade regular e escrita em arquivo netcdf
        
        return missing_files
    
    def get_file_list(self) -> List[str]:
        """
        Retorna lista de nomes de arquivos esperados
        
        Returns:
            Lista de nomes de arquivos
        """
        urls_and_files = self._generate_file_urls()
        return [filename for url, filename in urls_and_files]
