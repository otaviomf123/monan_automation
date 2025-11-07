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
        self.execution = config.get_execution_config()
        self.slurm = config.get_slurm_config()
        self.mpirun = config.get_mpirun_config()
    
    def _get_cores_count(self) -> int:
        """Retorna o número de cores para execução"""
        return self.execution.get('cores', self.slurm.get('ntasks_per_node', 128))
    
    def _get_execution_backend(self) -> str:
        """Retorna o backend de execução configurado"""
        return self.execution.get('backend', 'slurm').lower()
    
    def _validate_mpirun_config(self) -> bool:
        """Valida configuração para execução com mpirun"""
        cores = self._get_cores_count()
        if cores <= 0:
            self.logger.error("FAILED: Número de cores deve ser maior que zero")
            return False
        self.logger.debug(f"Configuração mpirun validada: cores={cores}")
        return True
    
    def _create_model_links(self, run_dir: Path, init_dir: Path, boundary_dir: Path) -> bool:
        """Cria links simbólicos necessários para o modelo"""
        self.logger.info("Criando links simbólicos do modelo...")
        
        monan_dir = Path(self.paths['monan_dir'])
        
        model_links = [
            (monan_dir / 'atmosphere_model', run_dir / 'atmosphere_model'),
        ]
        
        # Links para tabelas
        table_patterns = ['*DBL', '*TBL', 'RRTMG_*']
        for pattern in table_patterns:
            files = list(monan_dir.glob(pattern))
            for file_path in files:
                target_path = run_dir / file_path.name
                model_links.append((file_path, target_path))
        
        # Link para condições iniciais
        init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
        init_file = init_dir / init_filename
        if init_file.exists():
            model_links.append((init_file, run_dir / init_filename))
        else:
            self.logger.error("Arquivo de condições iniciais não encontrado")
            return False
        
        # Links para condições de fronteira
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
        """Gera namelist para o modelo MONAN"""
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
        """Copia arquivos de streams necessários"""
        self.logger.info("Copiando arquivos de streams...")
        
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
        return success_count > 0
    
    def _generate_slurm_script(self, run_dir: Path) -> Path:
        """Gera script SLURM para submissão do job"""
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

TIMING_FILE="{timing_file}"

START_TIME=$(date)
START_SECONDS=$(date +%s)

echo "========================================" >> $TIMING_FILE
echo "Job ID: $SLURM_JOB_ID" >> $TIMING_FILE
echo "Início da execução: $START_TIME" >> $TIMING_FILE
echo "Nós utilizados: $SLURM_JOB_NUM_NODES" >> $TIMING_FILE
echo "Tasks por nó: $SLURM_NTASKS_PER_NODE" >> $TIMING_FILE
echo "Total de tasks: $SLURM_NTASKS" >> $TIMING_FILE

infiniband_flag="{self.slurm.get('infiniband', '-iface ibp65s0')}"
if [ -n "$infiniband_flag" ]; then
    mpirun -np {self._get_cores_count()} $infiniband_flag {exe_path}
else
    mpirun -np {self._get_cores_count()} {exe_path}
fi

EXIT_CODE=$?

END_TIME=$(date)
END_SECONDS=$(date +%s)

DURATION=$((END_SECONDS - START_SECONDS))
HOURS=$((DURATION / 3600))
MINUTES=$(((DURATION % 3600) / 60))
SECONDS=$((DURATION % 60))

echo "Fim da execução: $END_TIME" >> $TIMING_FILE
echo "Duração total: ${{HOURS}}h ${{MINUTES}}m ${{SECONDS}}s ($DURATION segundos)" >> $TIMING_FILE
echo "Código de saída: $EXIT_CODE" >> $TIMING_FILE
echo "========================================" >> $TIMING_FILE
echo "" >> $TIMING_FILE

echo "Execução concluída em: ${{HOURS}}h ${{MINUTES}}m ${{SECONDS}}s"
echo "Informações salvas em: $TIMING_FILE"

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
        
        hosts = self.mpirun.get('hosts', [])
        np_config = self.mpirun.get('np')
        cores = np_config if np_config is not None else self._get_cores_count()
        timeout_hours = self.mpirun.get('timeout_hours', 24)
        extra_args = self.mpirun.get('extra_args', [])
        infiniband = self.mpirun.get('infiniband', '-iface ibp65s0')
        
        exe_path = run_dir / 'atmosphere_model'
        if not exe_path.exists():
            self.logger.error(f"Executável não encontrado: {exe_path}")
            return False
        
        cmd_parts = ["mpirun"]
        
        if hosts:
            hosts_str = ",".join(hosts)
            cmd_parts.extend(["--host", hosts_str])
        
        cmd_parts.extend(["-np", str(cores)])
        
        if infiniband.strip():
            cmd_parts.append(infiniband)
        
        if extra_args:
            cmd_parts.extend(extra_args)
        
        cmd_parts.append("./atmosphere_model")
        
        mpi_cmd = " ".join(cmd_parts)
        
        self.logger.info(f"Comando de execução: {mpi_cmd}")
        self.logger.info(f"Hosts: {hosts if hosts else 'default'}, Cores: {cores}")
        self.logger.info(f"Timeout: {timeout_hours} horas")
        
        timing_file = run_dir.parent / 'mpas_execution_times.log'
        
        try:
            start_time = datetime.now()
            with open(timing_file, 'a') as f:
                f.write("========================================\n")
                f.write(f"Início da execução (mpirun): {start_time}\n")
                f.write(f"Hosts: {hosts if hosts else 'default'}\n")
                f.write(f"Cores: {cores}\n")
                f.write(f"Comando: {mpi_cmd}\n")
            
            self.logger.info("Iniciando execução do modelo MPAS (pode levar várias horas)...")
            
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
                self.logger.error(f"FAILED: Modelo falhou com código {result.returncode}")
                if result.stderr:
                    self.logger.error(f"Erro: {result.stderr[:500]}...")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error(f"FAILED: Execução excedeu timeout de {timeout_hours} horas")
            
            end_time = datetime.now()
            duration = end_time - start_time
            with open(timing_file, 'a') as f:
                f.write(f"TIMEOUT após {format_duration(int(duration.total_seconds()))}\n")
                f.write("========================================\n\n")
            return False
            
        except Exception as e:
            self.logger.error(f"FAILED: Erro inesperado durante execução: {e}")
            self.logger.exception("Detalhes do erro:")
            return False
    
    def run_model(self, run_dir: Path, init_dir: Path, boundary_dir: Path) -> bool:
        """
        Executa o modelo MONAN/MPAS e AGUARDA conclusão
        
        Args:
            run_dir: Diretório de execução
            init_dir: Diretório com condições iniciais
            boundary_dir: Diretório com condições de fronteira
            
        Returns:
            True se execução concluída com sucesso, False caso contrário
        """
        self.logger.info("="*50)
        self.logger.info("CONFIGURANDO E EXECUTANDO MODELO MONAN")
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
            
            # 4. Executar baseado no backend configurado
            execution_backend = self._get_execution_backend()
            self.logger.info(f"Backend de execução: {execution_backend}")
            
            if execution_backend == 'mpirun':
                # Validar e executar diretamente com mpirun
                if not self._validate_mpirun_config():
                    return False
                
                # mpirun já aguarda automaticamente a conclusão
                if not self._run_mpirun_direct(run_dir):
                    return False
                
                self.logger.info("SUCCESS: Modelo executado com sucesso via mpirun!")
                
            elif execution_backend == 'slurm':
                # Gerar script SLURM
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
                self.logger.error(f"FAILED: Backend de execução inválido: {execution_backend}")
                self.logger.error("Backends suportados: 'slurm', 'mpirun'")
                return False
            
            # Verificar arquivos de saída foram criados
            diag_files = list(run_dir.glob("diag.*.nc"))
            history_files = list(run_dir.glob("history.*.nc"))
            
            if not diag_files and not history_files:
                self.logger.error("FAILED: Nenhum arquivo de saída foi gerado")
                return False
            
            self.logger.info(f"Arquivos de saída gerados: {len(diag_files)} diag, {len(history_files)} history")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro durante execução do modelo: {e}")
            self.logger.exception("Detalhes do erro:")
            return False
    
    def check_model_output(self, run_dir: Path) -> dict:
        """Verifica saídas do modelo"""
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
        
        for file_type, pattern in patterns.items():
            files = list(run_dir.glob(pattern))
            output_info[file_type] = files
            
            for file_path in files:
                output_info['total_size_mb'] += file_path.stat().st_size / (1024 * 1024)
        
        return output_info
