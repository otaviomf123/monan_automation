"""
Gerador de Condicoes de Fronteira
==================================

Modulo para gerar condicoes de fronteira do MPAS
"""

import logging
from pathlib import Path

from .config_loader import ConfigLoader
from .utils import create_symbolic_link, run_command, write_namelist, write_streams_file


class BoundaryConditionsGenerator:
    """Classe para geracao de condicoes de fronteira"""
    
    def __init__(self, config: ConfigLoader):
        """
        Inicializa o gerador de condicoes de fronteira
        
        Args:
            config: Objeto de configuracao
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        self.paths = config.get_paths()
        self.dates = config.get_dates()
        self.physics = config.get_physics_config()
    
    def _create_file_links(self, boundary_dir: Path, ic_dir: Path) -> bool:
        """
        Cria links para arquivos FILE do WPS
        
        Args:
            boundary_dir: Diretorio de condicoes de fronteira
            ic_dir: Diretorio com dados IC processados (GFS ou ERA5)
            
        Returns:
            True se sucesso, False caso contrario
        """
        self.logger.info("Criando links para arquivos FILE...")
        
        # Buscar arquivos FILE no diretorio IC
        file_pattern = f"FILE:{self.dates['run_date'][:4]}-*"  # Ex: FILE:2025-*
        file_list = list(ic_dir.glob(file_pattern))
        
        if not file_list:
            self.logger.error(f"Nenhum arquivo FILE encontrado com padrao: {file_pattern}")
            return False
        
        self.logger.info(f"Encontrados {len(file_list)} arquivos FILE")
        
        # Criar links simbolicos
        success_count = 0
        for file_path in file_list:
            target_path = boundary_dir / file_path.name
            if create_symbolic_link(file_path, target_path):
                success_count += 1
        
        self.logger.info(f"Criados {success_count} links para arquivos FILE")
        return success_count == len(file_list)
    
    def _generate_boundary_namelist(self, init_dir: Path) -> dict:
        """
        Gera namelist para condicoes de fronteira
        
        Args:
            init_dir: Diretorio com condicoes iniciais
            
        Returns:
            Dicionario com configuracoes do namelist
        """
        return {
            'nhyd_model': {
                'config_init_case': 9,
                'config_start_time': self.dates['start_time'],
                'config_stop_time': self.dates['end_time']
            },
            'dimensions': {
                'config_nfglevels': 35
            },
            'data_sources': {
                'config_fg_interval': 10800,  # 3 horas
                'config_met_prefix': 'FILE',
                'config_sfc_prefix': 'SST'
            },
            'interpolation_control': {
                'config_extrap_airtemp': 'linear'
            },
            'decomposition': {
                'config_block_decomp_file_prefix': '/home/otavio.feitosa/limited_area/MPAS-Limited-Area/goias.graph.info.part.'
            }
        }
    
    def _generate_boundary_streams(self, init_dir: Path) -> str:
        """
        Gera arquivo de streams para condicoes de fronteira
        
        Args:
            init_dir: Diretorio com condicoes iniciais
            
        Returns:
            Conteudo XML do arquivo streams
        """
        init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
        init_file = init_dir / init_filename
        
        return f'''<streams>
<immutable_stream name="input"
                 type="input"
                 precision="single"
                 io_type="pnetcdf,cdf5"
                 filename_template="{init_file.absolute()}"
                 input_interval="initial_only" />
<immutable_stream name="lbc"
                 type="output"
                 filename_template="lbc.$Y-$M-$D_$h.00.00.nc"
                 filename_interval="output_interval"
                 packages="lbcs"
                 io_type="pnetcdf,cdf5"
                 output_interval="3:00:00" />
</streams>'''
    
    def _link_executable(self, boundary_dir: Path) -> bool:
        """
        Cria link para executavel init_atmosphere_model
        
        Args:
            boundary_dir: Diretorio de condicoes de fronteira
            
        Returns:
            True se sucesso, False caso contrario
        """
        exe_source = Path(self.paths['mpas_init_exe'])
        exe_target = boundary_dir / 'init_atmosphere_model'
        
        return create_symbolic_link(exe_source, exe_target)
    
    def _run_boundary_generation(self, boundary_dir: Path) -> bool:
        """
        Executa geracao das condicoes de fronteira
        
        Args:
            boundary_dir: Diretorio de condicoes de fronteira
            
        Returns:
            True se sucesso, False caso contrario
        """
        self.logger.info("Executando geracao de condicoes de fronteira...")
        
        command = "./init_atmosphere_model"
        return_code, stdout, stderr = run_command(command, cwd=boundary_dir, timeout=3600)
        
        if return_code != 0:
            self.logger.error(f"Erro na geracao de condicoes de fronteira: {stderr}")
            return False
        
        # Verificar se arquivos LBC foram criados
        lbc_files = list(boundary_dir.glob("lbc.*.nc"))
        if not lbc_files:
            self.logger.error("Nenhum arquivo LBC foi gerado")
            return False
        
        total_size_mb = sum(f.stat().st_size for f in lbc_files) / (1024 * 1024)
        self.logger.info(f"SUCCESS: Gerados {len(lbc_files)} arquivos LBC ({total_size_mb:.1f} MB total)")
        
        return True
    
    def generate(self, boundary_dir: Path, init_dir: Path, ic_dir: Path) -> bool:
        """
        Gera condicoes de fronteira do MPAS
        
        Args:
            boundary_dir: Diretorio de condicoes de fronteira
            init_dir: Diretorio com condicoes iniciais
            ic_dir: Diretorio com dados IC processados (GFS ou ERA5)
            
        Returns:
            True se sucesso, False caso contrario
        """
        self.logger.info("="*50)
        self.logger.info("GERANDO CONDICOES DE FRONTEIRA")
        self.logger.info("="*50)
        
        try:
            # 1. Verificar se condicoes iniciais existem
            init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
            init_file = init_dir / init_filename
            if not init_file.exists():
                self.logger.error("Arquivo de condicoes iniciais nao encontrado")
                return False
            
            # 2. Criar links para arquivos FILE
            if not self._create_file_links(boundary_dir, ic_dir):
                return False
            
            # 3. Gerar namelist
            namelist_data = self._generate_boundary_namelist(init_dir)
            namelist_path = boundary_dir / 'namelist.init_atmosphere'
            write_namelist(namelist_path, namelist_data)
            self.logger.info(f"Namelist de fronteira criado: {namelist_path}")
            
            # 4. Gerar streams
            streams_content = self._generate_boundary_streams(init_dir)
            streams_path = boundary_dir / 'streams.init_atmosphere'
            write_streams_file(streams_path, streams_content)
            self.logger.info(f"Streams de fronteira criado: {streams_path}")
            
            # 5. Linkar executavel
            if not self._link_executable(boundary_dir):
                return False
            
            # 6. Executar geracao das condicoes de fronteira
            if not self._run_boundary_generation(boundary_dir):
                return False
            
            self.logger.info("SUCCESS: Condicoes de fronteira geradas com sucesso!")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro durante geracao de condicoes de fronteira: {e}")
            self.logger.exception("Detalhes do erro:")
            return False
    
    def verify_output(self, boundary_dir: Path) -> bool:
        """
        Verifica se as condicoes de fronteira foram geradas corretamente
        
        Args:
            boundary_dir: Diretorio de condicoes de fronteira
            
        Returns:
            True se valido, False caso contrario
        """
        lbc_files = list(boundary_dir.glob("lbc.*.nc"))
        
        if not lbc_files:
            self.logger.error("Nenhum arquivo LBC encontrado")
            return False
        
        # Verificar se arquivos nao estao vazios
        empty_files = []
        total_size = 0
        
        for lbc_file in lbc_files:
            size = lbc_file.stat().st_size
            if size == 0:
                empty_files.append(lbc_file.name)
            else:
                total_size += size
        
        if empty_files:
            self.logger.error(f"Arquivos LBC vazios encontrados: {empty_files}")
            return False
        
        # Verificar numero esperado de arquivos (baseado no intervalo de 3h)
        forecast_hours = self.config.get('data_sources.forecast_hours')
        expected_files = len(range(forecast_hours['start'], forecast_hours['end'] + 1, 3))
        
        if len(lbc_files) < expected_files * 0.8:  # Tolerancia de 20%
            self.logger.warning(f"Numero de arquivos LBC menor que esperado: {len(lbc_files)} < {expected_files}")
        
        total_size_mb = total_size / (1024 * 1024)
        self.logger.info(f"SUCCESS: Condicoes de fronteira validas: {len(lbc_files)} arquivos, {total_size_mb:.1f} MB")
        
        return True
    
    def get_lbc_files(self, boundary_dir: Path) -> list:
        """
        Retorna lista de arquivos LBC gerados
        
        Args:
            boundary_dir: Diretorio de condicoes de fronteira
            
        Returns:
            Lista ordenada de arquivos LBC
        """
        return sorted(boundary_dir.glob("lbc.*.nc"))
    
    def cleanup_temp_files(self, boundary_dir: Path) -> None:
        """
        Remove arquivos temporarios
        
        Args:
            boundary_dir: Diretorio de condicoes de fronteira
        """
        self.logger.info("Limpando arquivos temporarios...")
        
        # Arquivos para remover
        cleanup_patterns = [
            "log.init_atmosphere.*",
            "namelist.init_atmosphere",
            "streams.init_atmosphere"
        ]
        
        removed_count = 0
        for pattern in cleanup_patterns:
            files = list(boundary_dir.glob(pattern))
            for file_path in files:
                try:
                    file_path.unlink()
                    removed_count += 1
                except Exception as e:
                    self.logger.warning(f"Erro ao remover {file_path}: {e}")
        
        if removed_count > 0:
            self.logger.info(f"Removidos {removed_count} arquivos temporarios")
