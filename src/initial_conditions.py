"""
Gerador de Condições Iniciais
==============================

Módulo para gerar condições iniciais do MPAS
"""

import logging
from pathlib import Path

from .config_loader import ConfigLoader
from .utils import create_symbolic_link, run_command, write_namelist, write_streams_file


class InitialConditionsGenerator:
    """Classe para geração de condições iniciais"""
    
    def __init__(self, config: ConfigLoader):
        """
        Inicializa o gerador de condições iniciais
        
        Args:
            config: Objeto de configuração
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        self.paths = config.get_paths()
        self.dates = config.get_dates()
        self.physics = config.get_physics_config()
    
    def _create_file_links(self, init_dir: Path, gfs_dir: Path) -> bool:
        """
        Cria links para arquivos FILE do WPS
        
        Args:
            init_dir: Diretório de condições iniciais
            gfs_dir: Diretório com dados GFS processados
            
        Returns:
            True se sucesso, False caso contrário
        """
        self.logger.info("Criando links para arquivos FILE...")
        
        # Buscar arquivos FILE no diretório GFS
        file_pattern = f"FILE:{self.dates['run_date'][:4]}-*"  # Ex: FILE:2025-*
        file_list = list(gfs_dir.glob(file_pattern))
        
        if not file_list:
            self.logger.error(f"Nenhum arquivo FILE encontrado com padrão: {file_pattern}")
            return False
        
        self.logger.info(f"Encontrados {len(file_list)} arquivos FILE")
        
        # Criar links simbólicos
        success_count = 0
        for file_path in file_list:
            target_path = init_dir / file_path.name
            if create_symbolic_link(file_path, target_path):
                success_count += 1
        
        self.logger.info(f"Criados {success_count} links para arquivos FILE")
        return success_count == len(file_list)
    
    def _generate_init_namelist(self) -> dict:
        """
        Gera namelist para condições iniciais
        
        Returns:
            Dicionário com configurações do namelist
        """
        return {
            'nhyd_model': {
                'config_init_case': 7,
                'config_start_time': self.dates['start_time'],
                'config_stop_time': self.dates['end_time'],
                'config_theta_adv_order': 3,
                'config_coef_3rd_order': 0.25
            },
            'dimensions': {
                'config_nvertlevels': self.physics['nvertlevels'],
                'config_nsoillevels': self.physics['nsoillevels'],
                'config_nfglevels': self.physics['nfglevels'],
                'config_nfgsoillevels': 4
            },
            'data_sources': {
                'config_geog_data_path': self.paths['geog_data_path'],
                'config_met_prefix': 'FILE',
                'config_sfc_prefix': 'SST',
                'config_fg_interval': 86400,
                'config_landuse_data': 'MODIFIED_IGBP_MODIS_NOAH',
                'config_topo_data': 'GMTED2010',
                'config_vegfrac_data': 'MODIS',
                'config_albedo_data': 'MODIS',
                'config_maxsnowalbedo_data': 'MODIS',
                'config_supersample_factor': 3,
                'config_use_spechumd': False
            },
            'vertical_grid': {
                'config_ztop': 30000.0,
                'config_nsmterrain': 1,
                'config_smooth_surfaces': True,
                'config_dzmin': 0.3,
                'config_nsm': 30,
                'config_tc_vertical_grid': True,
                'config_blend_bdy_terrain': False
            },
            'interpolation_control': {
                'config_extrap_airtemp': 'linear'
            },
            'preproc_stages': {
                'config_static_interp': False,
                'config_native_gwd_static': False,
                'config_vertical_grid': True,
                'config_met_interp': True,
                'config_input_sst': False,
                'config_frac_seaice': True
            },
            'io': {
                'config_pio_num_iotasks': 0,
                'config_pio_stride': 1
            },
            'decomposition': {
                'config_block_decomp_file_prefix': self.paths['decomp_file_prefix']
            },
            'limited_area': {
                'config_apply_lbcs': True
            }
        }
    
    def _generate_init_streams(self) -> str:
        """
        Gera arquivo de streams para condições iniciais
        
        Returns:
            Conteúdo XML do arquivo streams
        """
        init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
        return f'''<streams>
<immutable_stream name="input"
                 type="input"
                 precision="single"
                 io_type="pnetcdf,cdf5"
                 filename_template="{self.paths['static_file']}"
                 input_interval="initial_only" />
<immutable_stream name="output"
                 type="output"
                 filename_template="{init_filename}"
                 io_type="pnetcdf,cdf5"
                 packages="initial_conds"
                 output_interval="initial_only" />
</streams>'''
    
    def _link_static_file(self, init_dir: Path) -> bool:
        """
        Cria link para arquivo estático da malha
        
        Args:
            init_dir: Diretório de condições iniciais
            
        Returns:
            True se sucesso, False caso contrário
        """
        static_file = Path(self.paths['static_file'])
        
        if not static_file.exists():
            self.logger.error(f"Arquivo estático não encontrado: {static_file}")
            return False
        
        # O link não é necessário se o caminho absoluto está no streams
        self.logger.info(f"Arquivo estático disponível: {static_file}")
        return True
    
    def _link_executable(self, init_dir: Path) -> bool:
        """
        Cria link para executável init_atmosphere_model
        
        Args:
            init_dir: Diretório de condições iniciais
            
        Returns:
            True se sucesso, False caso contrário
        """
        exe_source = Path(self.paths['mpas_init_exe'])
        exe_target = init_dir / 'init_atmosphere_model'
        
        return create_symbolic_link(exe_source, exe_target)
    
    def _run_init_atmosphere(self, init_dir: Path) -> bool:
        """
        Executa init_atmosphere_model
        
        Args:
            init_dir: Diretório de condições iniciais
            
        Returns:
            True se sucesso, False caso contrário
        """
        self.logger.info("Executando init_atmosphere_model...")
        
        command = "./init_atmosphere_model"
        return_code, stdout, stderr = run_command(command, cwd=init_dir, timeout=3600)
        
        if return_code != 0:
            self.logger.error(f"Erro no init_atmosphere_model: {stderr}")
            return False
        
        # Verificar se arquivo de saída foi criado
        init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
        output_file = init_dir / init_filename
        if not output_file.exists():
            self.logger.error("Arquivo de condições iniciais não foi criado")
            return False
        
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        self.logger.info(f"✓ Condições iniciais geradas: {output_file.name} ({file_size_mb:.1f} MB)")
        
        return True
    
    def generate(self, init_dir: Path, gfs_dir: Path) -> bool:
        """
        Gera condições iniciais do MPAS
        
        Args:
            init_dir: Diretório de condições iniciais
            gfs_dir: Diretório com dados GFS processados
            
        Returns:
            True se sucesso, False caso contrário
        """
        self.logger.info("="*50)
        self.logger.info("GERANDO CONDIÇÕES INICIAIS")
        self.logger.info("="*50)
        
        try:
            # 1. Criar links para arquivos FILE
            if not self._create_file_links(init_dir, gfs_dir):
                return False
            
            # 2. Verificar arquivo estático
            if not self._link_static_file(init_dir):
                return False
            
            # 3. Gerar namelist
            namelist_data = self._generate_init_namelist()
            namelist_path = init_dir / 'namelist.init_atmosphere'
            write_namelist(namelist_path, namelist_data)
            self.logger.info(f"Namelist de inicialização criado: {namelist_path}")
            
            # 4. Gerar streams
            streams_content = self._generate_init_streams()
            streams_path = init_dir / 'streams.init_atmosphere'
            write_streams_file(streams_path, streams_content)
            self.logger.info(f"Streams de inicialização criado: {streams_path}")
            
            # 5. Linkar executável
            if not self._link_executable(init_dir):
                return False
            
            # 6. Executar init_atmosphere_model
            if not self._run_init_atmosphere(init_dir):
                return False
            
            self.logger.info("✓ Condições iniciais geradas com sucesso!")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro durante geração de condições iniciais: {e}")
            self.logger.exception("Detalhes do erro:")
            return False
    
    def verify_output(self, init_dir: Path) -> bool:
        """
        Verifica se as condições iniciais foram geradas corretamente
        
        Args:
            init_dir: Diretório de condições iniciais
            
        Returns:
            True se válido, False caso contrário
        """
        init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
        output_file = init_dir / init_filename
        
        if not output_file.exists():
            self.logger.error("Arquivo de condições iniciais não encontrado")
            return False
        
        # Verificar tamanho mínimo (deve ter pelo menos alguns MB)
        min_size_mb = 10
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        
        if file_size_mb < min_size_mb:
            self.logger.error(f"Arquivo de condições iniciais muito pequeno: {file_size_mb:.1f} MB")
            return False
        
        self.logger.info(f"✓ Condições iniciais válidas: {file_size_mb:.1f} MB")
        return True
    
    def cleanup_temp_files(self, init_dir: Path) -> None:
        """
        Remove arquivos temporários
        
        Args:
            init_dir: Diretório de condições iniciais
        """
        self.logger.info("Limpando arquivos temporários...")
        
        # Arquivos para remover
        cleanup_patterns = [
            "log.init_atmosphere.*",
            "namelist.init_atmosphere",
            "streams.init_atmosphere"
        ]
        
        removed_count = 0
        for pattern in cleanup_patterns:
            files = list(init_dir.glob(pattern))
            for file_path in files:
                try:
                    file_path.unlink()
                    removed_count += 1
                except Exception as e:
                    self.logger.warning(f"Erro ao remover {file_path}: {e}")
        
        if removed_count > 0:
            self.logger.info(f"Removidos {removed_count} arquivos temporários")
