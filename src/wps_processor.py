"""
Processador WPS
===============

Módulo para processar dados meteorológicos usando WPS (ungrib)
"""

import logging
from pathlib import Path

from .config_loader import ConfigLoader
from .utils import create_symbolic_link, run_command, write_namelist


class WPSProcessor:
    """Classe para processamento WPS"""
    
    def __init__(self, config: ConfigLoader):
        """
        Inicializa o processador WPS
        
        Args:
            config: Objeto de configuração
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Caminhos dos executáveis WPS
        self.paths = config.get_paths()
        self.dates = config.get_dates()
        self.domain = config.get_domain_config()
    
    def _create_wps_links(self, work_dir: Path) -> bool:
        """
        Cria links simbólicos necessários para o WPS
        
        Args:
            work_dir: Diretório de trabalho
            
        Returns:
            True se sucesso, False caso contrário
        """
        self.logger.info("Criando links simbólicos do WPS...")
        
        links = [
            (self.paths['link_grib'], work_dir / 'link_grib.csh'),
            (self.paths['ungrib_exe'], work_dir / 'ungrib.exe'),
            (self.paths['vtable_gfs'], work_dir / 'Vtable')
        ]
        
        success = True
        for source, target in links:
            source_path = Path(source)
            if not create_symbolic_link(source_path, target):
                self.logger.error(f"Falha ao criar link: {target}")
                success = False
        
        return success
    
    def _generate_wps_namelist(self) -> dict:
        """
        Gera namelist para o WPS
        
        Returns:
            Dicionário com configurações do namelist
        """
        return {
            'share': {
                'wrf_core': 'ARW',
                'max_dom': 1,
                'start_date': self.dates['start_time'],
                'end_date': self.dates['end_time'],
                'interval_seconds': 10800
            },
            'geogrid': {
                'parent_id': 1,
                'parent_grid_ratio': 1,
                'i_parent_start': 1,
                'j_parent_start': 1,
                'e_we': self.domain['e_we'],
                'e_sn': self.domain['e_sn'],
                'geog_data_res': 'default',
                'dx': self.domain['dx'],
                'dy': self.domain['dy'],
                'map_proj': 'lambert',
                'ref_lat': self.domain['ref_lat'],
                'ref_lon': self.domain['ref_lon'],
                'truelat1': self.domain['truelat1'],
                'truelat2': self.domain['truelat2'],
                'stand_lon': self.domain['stand_lon'],
                'geog_data_path': self.paths['wps_geog_path']
            },
            'ungrib': {
                'out_format': 'WPS',
                'prefix': 'FILE'
            },
            'metgrid': {
                'fg_name': 'FILE'
            }
        }
    
    def _link_grib_files(self, work_dir: Path, gfs_dir: Path) -> bool:
        """
        Executa link_grib.csh para preparar arquivos GRIB
        
        Args:
            work_dir: Diretório de trabalho
            gfs_dir: Diretório com arquivos GFS
            
        Returns:
            True se sucesso, False caso contrário
        """
        self.logger.info("Executando link_grib.csh...")
        
        # Comando para linkar arquivos GRIB
        grib_files = list(gfs_dir.glob("gfs.*.pgrb2.*"))
        
        if not grib_files:
            self.logger.error("Nenhum arquivo GRIB encontrado no diretório GFS")
            return False
        
        self.logger.info(f"Encontrados {len(grib_files)} arquivos GRIB")
        
        # Executar link_grib.csh
        command = f"./link_grib.csh {gfs_dir}/gfs.*.pgrb2.*"
        return_code, stdout, stderr = run_command(command, cwd=work_dir)
        
        if return_code != 0:
            self.logger.error(f"Erro no link_grib.csh: {stderr}")
            return False
        
        # Verificar se arquivos GRIBFILE foram criados
        gribfiles = list(work_dir.glob("GRIBFILE.*"))
        self.logger.info(f"Criados {len(gribfiles)} arquivos GRIBFILE")
        
        return len(gribfiles) > 0
    
    def _run_ungrib(self, work_dir: Path) -> bool:
        """
        Executa ungrib.exe
        
        Args:
            work_dir: Diretório de trabalho
            
        Returns:
            True se sucesso, False caso contrário
        """
        self.logger.info("Executando ungrib.exe...")
        
        command = "./ungrib.exe"
        return_code, stdout, stderr = run_command(command, cwd=work_dir, timeout=1800)
        
        if return_code != 0:
            self.logger.error(f"Erro no ungrib.exe: {stderr}")
            return False
        
        # Verificar se arquivos FILE foram criados
        file_outputs = list(work_dir.glob("FILE:*"))
        self.logger.info(f"Ungrib gerou {len(file_outputs)} arquivos FILE")
        
        if len(file_outputs) == 0:
            self.logger.error("Ungrib não gerou nenhum arquivo FILE")
            return False
        
        return True
    
    def process(self, gfs_dir: Path) -> bool:
        """
        Processa dados GFS usando WPS
        
        Args:
            gfs_dir: Diretório com dados GFS
            
        Returns:
            True se sucesso, False caso contrário
        """
        self.logger.info("="*50)
        self.logger.info("INICIANDO PROCESSAMENTO WPS")
        self.logger.info("="*50)
        
        work_dir = gfs_dir  # Usar o mesmo diretório dos dados GFS
        
        try:
            # 1. Criar links simbólicos
            if not self._create_wps_links(work_dir):
                return False
            
            # 2. Gerar namelist.wps
            namelist_data = self._generate_wps_namelist()
            namelist_path = work_dir / 'namelist.wps'
            write_namelist(namelist_path, namelist_data)
            self.logger.info(f"Namelist WPS criado: {namelist_path}")
            
            # 3. Executar link_grib.csh
            if not self._link_grib_files(work_dir, gfs_dir):
                return False
            
            # 4. Executar ungrib.exe
            if not self._run_ungrib(work_dir):
                return False
            
<<<<<<< HEAD
            self.logger.info("✓ Processamento WPS concluído com sucesso!")
=======
            self.logger.info(" Processamento WPS concluído com sucesso!")
>>>>>>> Melhoramento da descricao, e add a parte de conversao para grade regular e escrita em arquivo netcdf
            return True
            
        except Exception as e:
            self.logger.error(f"Erro durante processamento WPS: {e}")
            self.logger.exception("Detalhes do erro:")
            return False
    
    def verify_wps_output(self, work_dir: Path) -> bool:
        """
        Verifica se a saída do WPS está correta
        
        Args:
            work_dir: Diretório de trabalho
            
        Returns:
            True se válido, False caso contrário
        """
        # Verificar arquivos FILE
        file_outputs = list(work_dir.glob("FILE:*"))
        
        if not file_outputs:
            self.logger.error("Nenhum arquivo FILE encontrado")
            return False
        
        # Verificar se arquivos não estão vazios
        empty_files = []
        for file_path in file_outputs:
            if file_path.stat().st_size == 0:
                empty_files.append(file_path.name)
        
        if empty_files:
            self.logger.error(f"Arquivos FILE vazios encontrados: {empty_files}")
            return False
        
<<<<<<< HEAD
        self.logger.info(f"✓ Verificação WPS: {len(file_outputs)} arquivos FILE válidos")
=======
        self.logger.info(f" Verificação WPS: {len(file_outputs)} arquivos FILE válidos")
>>>>>>> Melhoramento da descricao, e add a parte de conversao para grade regular e escrita em arquivo netcdf
        return True
    
    def cleanup_wps_files(self, work_dir: Path) -> None:
        """
        Remove arquivos temporários do WPS
        
        Args:
            work_dir: Diretório de trabalho
        """
        self.logger.info("Limpando arquivos temporários do WPS...")
        
        # Arquivos para remover
        cleanup_patterns = [
            "GRIBFILE.*",
            "ungrib.log",
            "namelist.wps"
        ]
        
        removed_count = 0
        for pattern in cleanup_patterns:
            files = list(work_dir.glob(pattern))
            for file_path in files:
                try:
                    file_path.unlink()
                    removed_count += 1
                except Exception as e:
                    self.logger.warning(f"Erro ao remover {file_path}: {e}")
        
        self.logger.info(f"Removidos {removed_count} arquivos temporários")
    
    def get_file_outputs(self, work_dir: Path) -> list:
        """
        Retorna lista de arquivos FILE gerados
        
        Args:
            work_dir: Diretório de trabalho
            
        Returns:
            Lista de caminhos dos arquivos FILE
        """
        return sorted(work_dir.glob("FILE:*"))
