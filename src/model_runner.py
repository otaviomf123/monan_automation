"""
Executor do Modelo MONAN/MPAS
==============================

Módulo para executar o modelo MONAN/MPAS
"""

import logging
import subprocess
from datetime import datetime
from pathlib import Path

from .config_loader import ConfigLoader
from .utils import create_symbolic_link, write_namelist, format_duration


class ModelRunner:
    """Classe para execução do modelo MONAN/MPAS"""
    
    def __init__(self, config: ConfigLoader):
        """
        Inicializa o executor do modelo
        
        Args:
            config: Objeto de configuração
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        self.paths = config.get_paths()
        self.dates = config.get_dates()
        self.physics = config.get_physics_config()
        self.slurm = config.get_slurm_config()
    
    def _create_model_links(self, run_dir: Path, init_dir: Path, boundary_dir: Path) -> bool:
        """
        Cria links simbólicos necessários para o modelo
        
        Args:
            run_dir: Diretório de execução
            init_dir: Diretório com condições iniciais
            boundary_dir: Diretório com condições de fronteira
            
        Returns:
            True se sucesso, False caso contrário
        """
        self.logger.info("Criando links simbólicos do modelo...")
        
        monan_dir = Path(self.paths['monan_dir'])
        
        # Links dos executáveis e tabelas do MONAN
        model_links = [
            (monan_dir / 'atmosphere_model', run_dir / 'atmosphere_model'),
        ]
        
        # Links para tabelas e dados físicos
        table_patterns = ['*DBL', '*TBL', 'RRTMG_*']
        for pattern in table_patterns:
            files = list(monan_dir.glob(pattern))
            for file_path in files:
                target_path = run_dir / file_path.name
                model_links.append((file_path, target_path))
        
        # Link para condições iniciais
        init_file = init_dir / 'brasil_circle.init.nc'
        if init_file.exists():
            model_links.append((init_file, run_dir / 'brasil_circle.init.nc'))
        else:
            self.logger.error("Arquivo de condições iniciais não encontrado")
            return False
        
        # Links para condições de fronteira (sem usar ano para evitar problemas na virada do ano)
        lbc_pattern = 'lbc.*-0*.nc'  # Padrão mais flexível
        lbc_files = list(boundary_dir.glob(lbc_pattern))
        if not lbc_files:
            # Tentar padrão alternativo se não encontrar
            lbc_files = list(boundary_dir.glob('lbc.*.nc'))
        
        if not lbc_files:
            self.logger.error("Nenhum arquivo LBC encontrado")
            return False
        
        for lbc_file in lbc_files:
            target_path = run_dir / lbc_file.name
            model_links.append((lbc_file, target_path))
        
        # Criar todos os links
        success_count = 0
        for source, target in model_links:
            if create_symbolic_link(source, target):
                success_count += 1
            else:
                self.logger.error(f"Falha ao criar link: {target}")
        
        self.logger.info(f"Criados {success_count}/{len(model_links)} links simbólicos")
        return success_count == len(model_links)
    
    def _generate_model_namelist(self) -> dict:
        """
        Gera namelist para o modelo MONAN
        
        Returns:
            Dicionário com configurações do namelist
        """
        # Calcular duração da simulação
        forecast_days = self.config.get('general.forecast_days', 10)
        run_duration = f"{forecast_days}_00:00:00"
        
        return {
            'nhyd_model': {
                'config_time_integration_order': 2,
                'config_dt': self.physics['dt'],
                'config_start_time': self.dates['start_time'],
                'config_run_duration': run_duration,
                'config_split_dynamics_transport': True,
                'config_number_of_sub_steps': 2,
                'config_dynamics_split_steps': 3,
                'config_h_mom_eddy_visc2': 0.0,
                'config_h_mom_eddy_visc4': 0.0,
                'config_v_mom_eddy_visc2': 0.0,
                'config_h_theta_eddy_visc2': 0.0,
                'config_h_theta_eddy_visc4': 0.0,
                'config_v_theta_eddy_visc2': 0.0,
                'config_horiz_mixing': '2d_smagorinsky',
                'config_len_disp': 10000.0,
                'config_visc4_2dsmag': 0.05,
                'config_w_adv_order': 3,
                'config_theta_adv_order': 3,
                'config_scalar_adv_order': 3,
                'config_u_vadv_order': 3,
                'config_w_vadv_order': 3,
                'config_theta_vadv_order': 3,
                'config_scalar_vadv_order': 3,
                'config_scalar_advection': True,
                'config_positive_definite': False,
                'config_monotonic': True,
                'config_coef_3rd_order': 0.25,
                'config_epssm': 0.1,
                'config_smdiv': 0.1
            },
            'damping': {
                'config_zd': 22000.0,
                'config_xnutr': 0.2
            },
            'limited_area': {
                'config_apply_lbcs': True
            },
            'io': {
                'config_pio_num_iotasks': 0,
                'config_pio_stride': 1
            },
            'decomposition': {
                'config_block_decomp_file_prefix': self.paths['decomp_file_prefix']
            },
            'restart': {
                'config_do_restart': False
            },
            'printout': {
                'config_print_global_minmax_vel': True
            },
            'IAU': {
                'config_IAU_option': 'off',
                'config_IAU_window_length_s': 21600.0
            },
            'physics': {
                'config_sst_update': False,
                'config_sstdiurn_update': False,
                'config_deepsoiltemp_update': False,
                'config_radtlw_interval': '00:30:00',
                'config_radtsw_interval': '00:30:00',
                'config_bucket_update': 'none',
                'config_physics_suite': self.physics['physics_suite'],
                'config_radt_cld_scheme': 'cld_fraction',
                'config_mynn_edmf': 0
            },
            'gf_monan': {
                'config_gf_pcvol': 0,
                'config_gf_cporg': 0,
                'config_gf_gustf': 0,
                'config_gf_sub3d': 0
            },
            'soundings': {
                'config_sounding_interval': 'none'
            }
        }
    
    def _copy_stream_files(self, run_dir: Path) -> bool:
        """
        Copia arquivos de streams necessários
        
        Args:
            run_dir: Diretório de execução
            
        Returns:
            True se sucesso, False caso contrário
        """
        self.logger.info("Copiando arquivos de streams...")
        
        # Arquivos de streams dos caminhos configurados
        stream_files = {
            'stream_list.atmosphere.diagnostics': self.paths.get('stream_diagnostics'),
            'stream_list.atmosphere.output': self.paths.get('stream_output'), 
            'stream_list.atmosphere.surface': self.paths.get('stream_surface'),
            'streams.atmosphere': self.paths.get('streams_atmosphere')
        }
        
        success_count = 0
        for target_name, source_path in stream_files.items():
            if source_path is None:
                self.logger.warning(f"Caminho não configurado para: {target_name}")
                continue
                
            source_path = Path(source_path)
            target_path = run_dir / target_name
            
            if source_path.exists():
                try:
                    target_path.write_text(source_path.read_text())
                    success_count += 1
                    self.logger.debug(f"Stream copiado: {target_name}")
                except Exception as e:
                    self.logger.error(f"Erro ao copiar {target_name}: {e}")
            else:
                self.logger.warning(f"Arquivo de stream não encontrado: {source_path}")
        
        self.logger.info(f"Copiados {success_count}/{len(stream_files)} arquivos de streams")
        return success_count > 0  # Pelo menos alguns devem existir
    
    def _generate_slurm_script(self, run_dir: Path) -> Path:
        """
        Gera script SLURM para submissão do job
        
        Args:
            run_dir: Diretório de execução
            
        Returns:
            Caminho do script SLURM gerado
        """
        script_path = run_dir / 'run_mpas.slurm'
        
        timing_file = run_dir.parent / 'mpas_execution_times.log'
        exe_path = run_dir / 'atmosphere_model'
        
        script_content = f'''#!/bin/bash
#SBATCH --partition={self.slurm['partition']}
#SBATCH --nodes={self.slurm['nodes']}
#SBATCH --ntasks-per-node={self.slurm['ntasks_per_node']}
#SBATCH --mem={self.slurm['memory']}
#SBATCH --job-name={self.slurm['job_name']}

source /home/otavio.feitosa/.bashrc

# Arquivo para salvar os tempos de execução
TIMING_FILE="{timing_file}"

# Captura o tempo de início
START_TIME=$(date)
START_SECONDS=$(date +%s)

echo "========================================" >> $TIMING_FILE
echo "Job ID: $SLURM_JOB_ID" >> $TIMING_FILE
echo "Início da execução: $START_TIME" >> $TIMING_FILE
echo "Nós utilizados: $SLURM_JOB_NUM_NODES" >> $TIMING_FILE
echo "Tasks por nó: $SLURM_NTASKS_PER_NODE" >> $TIMING_FILE
echo "Total de tasks: $SLURM_NTASKS" >> $TIMING_FILE

# Executa o modelo MPAS
mpirun -np {self.slurm['ntasks_per_node']} {exe_path}

# Captura o código de saída
EXIT_CODE=$?

# Captura o tempo de fim
END_TIME=$(date)
END_SECONDS=$(date +%s)

# Calcula a duração
DURATION=$((END_SECONDS - START_SECONDS))
HOURS=$((DURATION / 3600))
MINUTES=$(((DURATION % 3600) / 60))
SECONDS=$((DURATION % 60))

# Salva as informações no arquivo
echo "Fim da execução: $END_TIME" >> $TIMING_FILE
echo "Duração total: ${{HOURS}}h ${{MINUTES}}m ${{SECONDS}}s ($DURATION segundos)" >> $TIMING_FILE
echo "Código de saída: $EXIT_CODE" >> $TIMING_FILE
echo "========================================" >> $TIMING_FILE
echo "" >> $TIMING_FILE

# Também exibe na saída padrão
echo "Execução concluída em: ${{HOURS}}h ${{MINUTES}}m ${{SECONDS}}s"
echo "Informações salvas em: $TIMING_FILE"

exit $EXIT_CODE
'''
        
        script_path.write_text(script_content)
        script_path.chmod(0o755)  # Tornar executável
        
        self.logger.info(f"Script SLURM gerado: {script_path}")
        return script_path
    
    def _submit_slurm_job(self, script_path: Path) -> bool:
        """
        Submete job SLURM
        
        Args:
            script_path: Caminho do script SLURM
            
        Returns:
            True se sucesso, False caso contrário
        """
        self.logger.info("Submetendo job SLURM...")
        
        command = f"sbatch {script_path}"
        
        try:
            result = subprocess.run(
                command,
                cwd=script_path.parent,
                shell=True,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                # Extrair job ID da saída
                output_lines = result.stdout.strip().split('\n')
                for line in output_lines:
                    if 'Submitted batch job' in line:
                        job_id = line.split()[-1]
<<<<<<< HEAD
                        self.logger.info(f"✓ Job submetido com sucesso: ID {job_id}")
                        return True
                
                self.logger.info("✓ Job submetido com sucesso")
=======
                        self.logger.info(f" Job submetido com sucesso: ID {job_id}")
                        return True
                
                self.logger.info(" Job submetido com sucesso")
>>>>>>> Melhoramento da descricao, e add a parte de conversao para grade regular e escrita em arquivo netcdf
                return True
            else:
                self.logger.error(f"Erro ao submeter job: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("Timeout ao submeter job SLURM")
            return False
        except Exception as e:
            self.logger.error(f"Erro inesperado ao submeter job: {e}")
            return False
    
    def run_model(self, run_dir: Path, init_dir: Path, boundary_dir: Path) -> bool:
        """
        Executa o modelo MONAN/MPAS
        
        Args:
            run_dir: Diretório de execução
            init_dir: Diretório com condições iniciais
            boundary_dir: Diretório com condições de fronteira
            
        Returns:
            True se configuração bem-sucedida, False caso contrário
        """
        self.logger.info("="*50)
        self.logger.info("CONFIGURANDO EXECUÇÃO DO MODELO MONAN")
        self.logger.info("="*50)
        
        try:
            # 1. Criar links simbólicos
            if not self._create_model_links(run_dir, init_dir, boundary_dir):
                return False
            
            # 2. Gerar namelist do modelo
            namelist_data = self._generate_model_namelist()
            namelist_path = run_dir / 'namelist.atmosphere'
            write_namelist(namelist_path, namelist_data)
            self.logger.info(f"Namelist do modelo criado: {namelist_path}")
            
            # 3. Copiar arquivos de streams
            if not self._copy_stream_files(run_dir):
                self.logger.warning("Alguns arquivos de streams não foram encontrados")
            
            # 4. Gerar script SLURM
            script_path = self._generate_slurm_script(run_dir)
            
            # 5. Submeter job
            if not self._submit_slurm_job(script_path):
                return False
            
<<<<<<< HEAD
            self.logger.info("✓ Modelo configurado e job submetido com sucesso!")
=======
            self.logger.info(" Modelo configurado e job submetido com sucesso!")
>>>>>>> Melhoramento da descricao, e add a parte de conversao para grade regular e escrita em arquivo netcdf
            self.logger.info(f"Monitore a execução em: {run_dir.parent}/mpas_execution_times.log")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro durante configuração do modelo: {e}")
            self.logger.exception("Detalhes do erro:")
            return False
    
    def check_model_output(self, run_dir: Path) -> dict:
        """
        Verifica saídas do modelo
        
        Args:
            run_dir: Diretório de execução
            
        Returns:
            Dicionário com informações sobre as saídas
        """
        output_info = {
            'history_files': [],
            'diagnostic_files': [],
            'restart_files': [],
            'total_size_mb': 0
        }
        
        # Procurar diferentes tipos de arquivo de saída
        patterns = {
            'history_files': 'history.*.nc',
            'diagnostic_files': 'diag.*.nc',
            'restart_files': 'restart.*.nc'
        }
        
        for file_type, pattern in patterns.items():
            files = list(run_dir.glob(pattern))
            output_info[file_type] = files
            
            for file_path in files:
                output_info['total_size_mb'] += file_path.stat().st_size / (1024 * 1024)
        
        return output_info
