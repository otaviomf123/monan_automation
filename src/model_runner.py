"""
Executor do Modelo MONAN/MPAS - VERSÃO CORRIGIDA
=================================================

Correção: Aguarda a conclusão do modelo antes de retornar sucesso
"""

import logging
import os
import subprocess
import time
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
        self.domain = config.get_domain_config()
        self.execution = config.get_execution_config()
        self.slurm = config.get_slurm_config()
        self.mpirun = config.get_mpirun_config()
    
    def _get_cores_count(self) -> int:
        """
        Retorna o numero de cores para execucao
        
        Returns:
            Numero de cores configurado
        """
        # Prioriza execution.cores, senao usa slurm.ntasks_per_node como fallback
        return self.execution.get('cores', self.slurm.get('ntasks_per_node', 128))
    
    def _get_execution_backend(self) -> str:
        """
        Retorna o backend de execucao configurado
        
        Returns:
            Backend de execucao ('slurm' ou 'mpirun')
        """
        return self.execution.get('backend', 'slurm').lower()
    
    def _validate_mpirun_config(self) -> bool:
        """
        Valida configuracao para execucao com mpirun
        
        Returns:
            True se configuracao valida, False caso contrario
        """
        cores = self._get_cores_count()
        hosts = self.mpirun.get('hosts', [])
        
        if cores <= 0:
            self.logger.error("FAILED: Numero de cores deve ser maior que zero")
            return False
        
        self.logger.debug(f"Configuracao mpirun validada: hosts={hosts}, cores={cores}")
        return True
    
    def _create_model_links(self, run_dir: Path, init_dir: Path, boundary_dir: Path) -> bool:
        """
        Cria links simbolicos necessarios para o modelo
        
        Args:
            run_dir: Diretorio de execucao
            init_dir: Diretorio com condicoes iniciais
            boundary_dir: Diretorio com condicoes de fronteira
            
        Returns:
            True se sucesso, False caso contrario
        """
        self.logger.info("Criando links simbolicos do modelo...")
        
        monan_dir = Path(self.paths['monan_dir'])
        
        # Links dos executaveis e tabelas do MONAN
        model_links = [
            (monan_dir / 'atmosphere_model', run_dir / 'atmosphere_model'),
        ]
        
        # Links para tabelas e dados fisicos
        table_patterns = ['*DBL', '*TBL', 'RRTMG_*']
        for pattern in table_patterns:
            files = list(monan_dir.glob(pattern))
            for file_path in files:
                target_path = run_dir / file_path.name
                model_links.append((file_path, target_path))
        
        # Link para condicoes iniciais
        init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
        init_file = init_dir / init_filename
        if init_file.exists():
            model_links.append((init_file, run_dir / init_filename))
        else:
            self.logger.error("Arquivo de condicoes iniciais nao encontrado")
            return False
        
        # Links para condicoes de fronteira (sem usar ano para evitar problemas na virada do ano)
        lbc_pattern = 'lbc.*-0*.nc'  # Padrao mais flexivel
        lbc_files = list(boundary_dir.glob(lbc_pattern))
        if not lbc_files:
            # Tentar padrao alternativo se nao encontrar
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
        
        self.logger.info(f"Criados {success_count}/{len(model_links)} links simbolicos")
        return success_count == len(model_links)
    
    def _validate_physics_config(self) -> bool:
        """
        Valida configuracoes basicas de fisica (sem restringir opcoes)
        
        Returns:
            True sempre (nao restringe opcoes de fisica)
        """
        physics = self.config.get_physics_config()
        
        # Verificar se physics_suite esta definido
        physics_suite = physics.get('physics_suite')
        if not physics_suite:
            self.logger.info("Nenhum physics_suite definido, usando padrao: 'mesoscale_reference_monan'")
        else:
            self.logger.info(f"Physics suite configurado: {physics_suite}")
        
        # Mostrar configuracoes de dt se definido
        dt = physics.get('dt')
        if dt:
            self.logger.info(f"Timestep (dt) configurado: {dt} segundos")
        else:
            self.logger.info("Timestep (dt) nao definido, usando padrao: 60.0 segundos")
        
        # Mostrar intervalos de radiacao se definidos
        radiation = physics.get('radiation', {})
        for rad_type in ['longwave', 'shortwave']:
            if rad_type in radiation:
                interval = radiation[rad_type].get('interval')
                if interval:
                    self.logger.info(f"Intervalo radiacao {rad_type}: {interval}")
        
        self.logger.info("Configuracao de fisica carregada (todas as opcoes sao aceitas)")
        return True
    
    def _generate_model_namelist(self) -> dict:
        """
        Gera namelist para o modelo MONAN
        
        Returns:
            Dicionario com configuracoes do namelist
        """
        # Calcular duracao da simulacao
        forecast_days = self.config.get('general.forecast_days', 10)
        run_duration = f"{forecast_days}_00:00:00"
        
        # Obter configuracoes de fisica expandidas
        physics = self.config.get_physics_config()
        radiation = physics.get('radiation', {})
        longwave = radiation.get('longwave', {})
        shortwave = radiation.get('shortwave', {})
        options = physics.get('options', {})
        convection_options = physics.get('convection_options', {})
        
        # Obter configuracoes do GF-MONAN
        gf_monan_config = self.config.get('gf_monan', {})
        
        return {
            'nhyd_model': {
                'config_time_integration_order': 2,
                'config_dt': physics.get('dt', 60.0),
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
                'config_len_disp': self.domain.get('config_len_disp', 10000.0),  # Resolucao da grade minima (m)
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
                # SST and soil updates
                'config_sst_update': options.get('sst_update', False),
                'config_sstdiurn_update': options.get('sstdiurn_update', False),
                'config_deepsoiltemp_update': options.get('deepsoiltemp_update', False),
                
                # Radiation intervals
                'config_radtlw_interval': longwave.get('interval', '00:30:00'),
                'config_radtsw_interval': shortwave.get('interval', '00:30:00'),
                
                # Convection options
                'config_cu_rad_feedback': True,
                'config_conv_interval': convection_options.get('interval', 900),
                'config_cu_closure': convection_options.get('closure', 'dp'),
                
                # Other physics options
                'config_bucket_update': 'none',
                'config_physics_suite': physics.get('physics_suite', 'mesoscale_reference_monan'),
                'config_radt_cld_scheme': radiation.get('cloud_fraction_scheme', 'cld_fraction'),
                'config_mynn_edmf': 0
            },
            'gf_monan': {
                'config_gf_pcvol': gf_monan_config.get('config_gf_pcvol', 0),
                'config_gf_cporg': gf_monan_config.get('config_gf_cporg', 1),
                'config_gf_gustf': gf_monan_config.get('config_gf_gustf', 1),
                'config_gf_sub3d': gf_monan_config.get('config_gf_sub3d', 0)
            },
            'soundings': {
                'config_sounding_interval': 'none'
            }
        }
    
    def _copy_stream_files(self, run_dir: Path) -> bool:
        """
        Copia arquivos de streams necessarios
        
        Args:
            run_dir: Diretorio de execucao
            
        Returns:
            True se sucesso, False caso contrario
        """
        self.logger.info("Copiando arquivos de streams...")
        
        # Get configured initial conditions filename
        init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
        
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
                    content = source_path.read_text()
                    
                    # Special handling for stream templates to update filenames
                    if target_name == 'streams.atmosphere':
                        import re
                        original_content = content
                        
                        # Replace hardcoded init filename with configured one
                        pattern = r'filename_template="[^"]*\.init\.nc"'
                        replacement = f'filename_template="{init_filename}"'
                        content = re.sub(pattern, replacement, content)
                        
                        # Log changes
                        if content != original_content:
                            self.logger.info(f"Updated init filename in streams.atmosphere: {init_filename}")
                        else:
                            self.logger.debug(f"No init filename to replace in {target_name}")
                    
                    # Handle other stream files if they have configurable parameters
                    elif 'streams.init_atmosphere' in target_name:
                        # This file is generated dynamically, not copied from template
                        pass
                    
                    target_path.write_text(content)
                    success_count += 1
                    self.logger.debug(f"Stream copiado: {target_name}")
                except Exception as e:
                    self.logger.error(f"Erro ao copiar {target_name}: {e}")
            else:
                self.logger.warning(f"Arquivo de stream nao encontrado: {source_path}")
        
        self.logger.info(f"Copiados {success_count}/{len(stream_files)} arquivos de streams")
        return success_count > 0
    
    def _verify_streams_init_filename(self, run_dir: Path) -> bool:
        """
        Verifica se o arquivo streams.atmosphere tem o nome correto do arquivo init
        
        Args:
            run_dir: Diretorio de execucao
            
        Returns:
            True se correto, False caso contrario
        """
        streams_file = run_dir / 'streams.atmosphere'
        init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
        
        if not streams_file.exists():
            self.logger.error("streams.atmosphere file not found")
            return False
        
        content = streams_file.read_text()
        
        if init_filename not in content:
            self.logger.error(f"Init filename '{init_filename}' not found in streams.atmosphere")
            self.logger.error("This will cause model execution to fail")
            return False
        
        self.logger.info(f"Verified streams.atmosphere contains correct init filename: {init_filename}")
        return True
    
    def _generate_slurm_script(self, run_dir: Path) -> Path:
        """
        Gera script SLURM para submissao do job
        
        Args:
            run_dir: Diretorio de execucao
            
        Returns:
            Caminho do script SLURM gerado
        """
        script_path = run_dir / 'run_mpas.slurm'
        
        timing_file = run_dir.parent / 'mpas_execution_times.log'
        exe_path = run_dir / 'atmosphere_model'
        home_dir = os.path.expanduser("~")
        bashrc_path = os.path.join(home_dir, ".bashrc")
        
        script_content = f'''#!/bin/bash
#SBATCH --partition={self.slurm['partition']}
#SBATCH --nodes={self.slurm['nodes']}
#SBATCH --ntasks-per-node={self.slurm['ntasks_per_node']}
#SBATCH --mem={self.slurm['memory']}
#SBATCH --job-name={self.slurm['job_name']}

source {bashrc_path}

# Arquivo para salvar os tempos de execucao
TIMING_FILE="{timing_file}"

# Captura o tempo de inicio
START_TIME=$(date)
START_SECONDS=$(date +%s)

echo "========================================" >> $TIMING_FILE
echo "Job ID: $SLURM_JOB_ID" >> $TIMING_FILE
echo "Inicio da execucao: $START_TIME" >> $TIMING_FILE
echo "Nos utilizados: $SLURM_JOB_NUM_NODES" >> $TIMING_FILE
echo "Tasks por no: $SLURM_NTASKS_PER_NODE" >> $TIMING_FILE
echo "Total de tasks: $SLURM_NTASKS" >> $TIMING_FILE

# Executa o modelo MPAS
infiniband_flag="{self.slurm.get('infiniband', '-iface ibp65s0')}"
if [ -n "$infiniband_flag" ]; then
    mpirun -np {self._get_cores_count()} $infiniband_flag {exe_path}
else
    mpirun -np {self._get_cores_count()} {exe_path}
fi

# Captura o codigo de saida
EXIT_CODE=$?

END_TIME=$(date)
END_SECONDS=$(date +%s)

# Calcula a duracao
DURATION=$((END_SECONDS - START_SECONDS))
HOURS=$((DURATION / 3600))
MINUTES=$(((DURATION % 3600) / 60))
SECONDS=$((DURATION % 60))

# Salva as informacoes no arquivo
echo "Fim da execucao: $END_TIME" >> $TIMING_FILE
echo "Duracao total: ${{HOURS}}h ${{MINUTES}}m ${{SECONDS}}s ($DURATION segundos)" >> $TIMING_FILE
echo "Codigo de saida: $EXIT_CODE" >> $TIMING_FILE
echo "========================================" >> $TIMING_FILE
echo "" >> $TIMING_FILE

# Tambem exibe na saida padrao
echo "Execucao concluida em: ${{HOURS}}h ${{MINUTES}}m ${{SECONDS}}s"
echo "Informacoes salvas em: $TIMING_FILE"

exit $EXIT_CODE
'''
        
        script_path.write_text(script_content)
        script_path.chmod(0o755)
        
        self.logger.info(f"Script SLURM gerado: {script_path}")
        return script_path
    
    def _submit_slurm_job(self, script_path: Path) -> tuple:
        """
        Submete job SLURM e retorna o job ID
        
        Returns:
            tuple: (success: bool, job_id: str or None)
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
                        self.logger.info(f"SUCCESS: Job submetido com sucesso: ID {job_id}")
                        return True, job_id
                
                self.logger.info("SUCCESS: Job submetido com sucesso")
                return True, None
            else:
                self.logger.error(f"Erro ao submeter job: {result.stderr}")
                return False, None
                
        except subprocess.TimeoutExpired:
            self.logger.error("Timeout ao submeter job SLURM")
            return False, None
        except Exception as e:
            self.logger.error(f"Erro inesperado ao submeter job: {e}")
            return False, None
    
    def _wait_for_slurm_job(self, job_id: str, check_interval: int = 60) -> bool:
        """
        Aguarda a conclusão do job SLURM
        
        Args:
            job_id: ID do job SLURM
            check_interval: Intervalo de verificação em segundos
            
        Returns:
            bool: True se job concluído com sucesso, False caso contrário
        """
        self.logger.info(f"Aguardando conclusão do job SLURM {job_id}...")
        self.logger.info(f"Verificando status a cada {check_interval} segundos")
        
        start_time = time.time()
        last_log_time = start_time
        
        while True:
            try:
                # Verificar status do job
                result = subprocess.run(
                    ['squeue', '-j', job_id, '-h', '-o', '%T'],
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                job_status = result.stdout.strip()
                
                # Job não está mais na fila
                if not job_status:
                    self.logger.info("Job não está mais na fila do SLURM")
                    
                    # Verificar se completou com sucesso usando sacct
                    result = subprocess.run(
                        ['sacct', '-j', job_id, '-n', '-o', 'State'],
                        capture_output=True,
                        text=True,
                        timeout=30
                    )
                    
                    final_status = result.stdout.strip().split('\n')[0].strip()
                    
                    if 'COMPLETED' in final_status:
                        elapsed = time.time() - start_time
                        self.logger.info(f"SUCCESS: Job {job_id} concluído com sucesso!")
                        self.logger.info(f"Tempo total de execução: {format_duration(int(elapsed))}")
                        return True
                    else:
                        self.logger.error(f"FAILED: Job {job_id} terminou com status: {final_status}")
                        return False
                
                # Job ainda está rodando ou na fila
                current_time = time.time()
                elapsed = current_time - start_time
                
                # Logar status periodicamente (a cada 10 minutos)
                if current_time - last_log_time >= 600:
                    self.logger.info(f"Job {job_id} ainda em execução (status: {job_status}, tempo decorrido: {format_duration(int(elapsed))})")
                    last_log_time = current_time
                
                # Aguardar antes da próxima verificação
                time.sleep(check_interval)
                
            except subprocess.TimeoutExpired:
                self.logger.warning("Timeout ao verificar status do job, tentando novamente...")
                time.sleep(check_interval)
            except Exception as e:
                self.logger.error(f"Erro ao verificar status do job: {e}")
                time.sleep(check_interval)
    
    def _run_mpirun_direct(self, run_dir: Path) -> bool:
        """Executa o modelo diretamente com mpirun"""
        self.logger.info("Executando modelo MPAS com mpirun direto...")
        
        # Obter configuracoes
        
        hosts = self.mpirun.get('hosts', [])
        np_config = self.mpirun.get('np')
        cores = np_config if np_config is not None else self._get_cores_count()
        timeout_hours = self.mpirun.get('timeout_hours', 24)
        extra_args = self.mpirun.get('extra_args', [])
        infiniband = self.mpirun.get('infiniband', '-iface ibp65s0')
        
        exe_path = run_dir / 'atmosphere_model'
        if not exe_path.exists():
            self.logger.error(f"Executavel nao encontrado: {exe_path}")
            return False
        
        # Construir comando mpirun
        
        cmd_parts = ["mpirun"]
        
        # Adicionar hosts se especificados
        if hosts:
            hosts_str = ",".join(hosts)
            cmd_parts.extend(["--host", hosts_str])
        
        # Adicionar numero de processos
        cmd_parts.extend(["-np", str(cores)])
        
        # Adicionar flag infiniband
        if infiniband.strip():
            cmd_parts.append(infiniband)
        
        # Adicionar argumentos extras
        if extra_args:
            cmd_parts.extend(extra_args)
        
        # Adicionar executavel
        cmd_parts.append("./atmosphere_model")
        
        mpi_cmd = " ".join(cmd_parts)
        
        self.logger.info(f"Comando de execucao: {mpi_cmd}")
        self.logger.info(f"Hosts: {hosts if hosts else 'default'}, Cores: {cores}")
        self.logger.info(f"Timeout: {timeout_hours} horas")
        
        # Arquivo de log de execucao
        timing_file = run_dir.parent / 'mpas_execution_times.log'
        
        try:
            # Registrar inicio
            start_time = datetime.now()
            with open(timing_file, 'a') as f:
                f.write("========================================\n")
                f.write(f"Inicio da execucao (mpirun): {start_time}\n")
                f.write(f"Hosts: {hosts if hosts else 'default'}\n")
                f.write(f"Cores: {cores}\n")
                f.write(f"Comando: {mpi_cmd}\n")
            
            self.logger.info("Iniciando execucao do modelo MPAS (pode levar varias horas)...")
            
            result = subprocess.run(
                mpi_cmd,
                cwd=run_dir,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout_hours * 3600
            )
            
            end_time = datetime.now()
            duration = end_time - start_time
            
            with open(timing_file, 'a') as f:
                f.write(f"Fim da execução: {end_time}\n")
                f.write(f"Duração total: {format_duration(int(duration.total_seconds()))}\n")
                f.write(f"Código de saída: {result.returncode}\n")
                if result.returncode != 0:
                    f.write(f"Erro: {result.stderr[:1000]}\n")
                f.write("========================================\n\n")
            
            if result.returncode == 0:
                self.logger.info(f"SUCCESS: Modelo executado com sucesso em {format_duration(int(duration.total_seconds()))}")
                self.logger.info(f"Log salvo em: {timing_file}")
                return True
            else:
                self.logger.error(f"FAILED: Modelo falhou com codigo {result.returncode}")
                if result.stderr:
                    self.logger.error(f"Erro: {result.stderr[:500]}...")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error(f"FAILED: Execucao excedeu timeout de {timeout_hours} horas")
            
            end_time = datetime.now()
            duration = end_time - start_time
            with open(timing_file, 'a') as f:
                f.write(f"TIMEOUT apos {format_duration(int(duration.total_seconds()))}\n")
                f.write("========================================\n\n")
            return False
            
        except Exception as e:
            self.logger.error(f"FAILED: Erro inesperado durante execucao: {e}")
            self.logger.exception("Detalhes do erro:")
            return False
    
    def run_model(self, run_dir: Path, init_dir: Path, boundary_dir: Path) -> bool:
        """
        Executa o modelo MONAN/MPAS e AGUARDA conclusão
        
        Args:
            run_dir: Diretorio de execucao
            init_dir: Diretorio com condicoes iniciais
            boundary_dir: Diretorio com condicoes de fronteira
            
        Returns:
            True se execucao concluida com sucesso, False caso contrario
        """
        self.logger.info("="*50)
        self.logger.info("CONFIGURANDO E EXECUTANDO MODELO MONAN")
        self.logger.info("="*50)
        
        try:
            # Validar configuracao de fisica
            self._validate_physics_config()
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
                self.logger.warning("Alguns arquivos de streams nao foram encontrados")
            
            # 3.5 Verify streams file has correct init filename
            if not self._verify_streams_init_filename(run_dir):
                self.logger.error("Streams file verification failed")
                return False
            
            # 4. Executar baseado no backend configurado
            execution_backend = self._get_execution_backend()
            self.logger.info(f"Backend de execucao: {execution_backend}")
            
            if execution_backend == 'mpirun':
                # Validar configuracao mpirun
                if not self._validate_mpirun_config():
                    return False
                
                # mpirun já aguarda automaticamente a conclusão
                if not self._run_mpirun_direct(run_dir):
                    return False
                
                self.logger.info("SUCCESS: Modelo executado com sucesso via mpirun!")
                
            elif execution_backend == 'slurm':
                # Gerar script SLURM e submeter job
                script_path = self._generate_slurm_script(run_dir)
                
                # Submeter job e obter job ID
                success, job_id = self._submit_slurm_job(script_path)
                if not success:
                    return False
                
                # CRÍTICO: Aguardar conclusão do job
                if job_id:
                    if not self._wait_for_slurm_job(job_id, check_interval=60):
                        return False
                else:
                    self.logger.warning("Job ID não foi obtido, não é possível aguardar conclusão")
                    self.logger.warning("Verifique manualmente se o job foi concluído antes de prosseguir")
                    return False
                
                self.logger.info("SUCCESS: Modelo executado com sucesso via SLURM!")
                
            else:
                self.logger.error(f"FAILED: Backend de execucao invalido: {execution_backend}")
                self.logger.error("Backends suportados: 'slurm', 'mpirun'")
                return False
            
            # Verificar arquivos de saida foram criados
            diag_files = list(run_dir.glob("diag.*.nc"))
            history_files = list(run_dir.glob("history.*.nc"))
            
            if not diag_files and not history_files:
                self.logger.error("FAILED: Nenhum arquivo de saida foi gerado")
                return False
            
            self.logger.info(f"Arquivos de saida gerados: {len(diag_files)} diag, {len(history_files)} history")
            self.logger.info(f"Monitore a execucao em: {run_dir.parent}/mpas_execution_times.log")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro durante configuracao do modelo: {e}")
            self.logger.exception("Detalhes do erro:")
            return False
    
    def check_model_output(self, run_dir: Path) -> dict:
        """
        Verifica saidas do modelo
        
        Args:
            run_dir: Diretorio de execucao
            
        Returns:
            Dicionario com informacoes sobre as saidas
        """
        output_info = {
            'history_files': [],
            'diagnostic_files': [],
            'restart_files': [],
            'total_size_mb': 0
        }
        
        patterns = {
            'history_files': 'history.*.nc',
            'diagnostic_files': 'diag.*.nc',
            'restart_files': 'restart.*.nc'
        }
        
        # Procurar diferentes tipos de arquivo de saida
        for file_type, pattern in patterns.items():
            files = list(run_dir.glob(pattern))
            output_info[file_type] = files
            
            for file_path in files:
                output_info['total_size_mb'] += file_path.stat().st_size / (1024 * 1024)
        
        return output_info