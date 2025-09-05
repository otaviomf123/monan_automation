#!/usr/bin/env python3
"""
Script de configuração e instalação do MONAN/MPAS Runner
"""

from pathlib import Path
import yaml
import shutil
import os

def create_config_example():
    """Cria arquivo de configuração de exemplo"""
    
    config_example = {
        'general': {
            'base_dir': "/home/SEU_USUARIO/limited_area/test_furnas",
            'forecast_days': 10
        },
        'dates': {
            'run_date': "20250727",
            'cycle': "00",
            'start_time': "2025-07-27_00:00:00",
            'end_time': "2025-08-06_00:00:00"
        },
        'data_sources': {
            'gfs_base_url': "https://noaa-gfs-bdp-pds.s3.amazonaws.com",
            'forecast_hours': {
                'start': 0,
                'end': 240,
                'step': 3
            }
        },
        'paths': {
            'wps_dir': "/home/SEU_USUARIO/mpas/wps",
            'link_grib': "/home/SEU_USUARIO/mpas/wps/link_grib.csh",
            'ungrib_exe': "/home/SEU_USUARIO/mpas/wps/ungrib.exe",
            'vtable_gfs': "/home/SEU_USUARIO/mpas/wps/ungrib/Variable_Tables/Vtable.GFS",
            'mpas_init_exe': "/home/SEU_USUARIO/mpas/init/init_atmosphere_model",
            'monan_exe': "/home/SEU_USUARIO/mpas/monan/atmosphere_model",
            'monan_dir': "/home/SEU_USUARIO/mpas/monan",
            'geog_data_path': "/mnt/cempa/01/SEU_USUARIO/atm_models/geog/mpas_static",
            'wps_geog_path': "/glade/work/wrfhelp/WPS_GEOG/",
            'static_file': "/home/SEU_USUARIO/limited_area/MPAS-Limited-Area/brasil_circle.static.nc",
            'decomp_file_prefix': "/home/SEU_USUARIO/limited_area/test_furnas/brasil_circle.graph.info.part."
        },
        'domain': {
            'dx': 15000,
            'dy': 15000,
            'ref_lat': 33.00,
            'ref_lon': -79.00,
            'truelat1': 30.0,
            'truelat2': 60.0,
            'stand_lon': -79.0,
            'e_we': 150,
            'e_sn': 130
        },
        'physics': {
            'nvertlevels': 55,
            'nsoillevels': 4,
            'nfglevels': 34,
            'dt': 60.0,
            'physics_suite': "mesoscale_reference_monan"
        },
        'slurm': {
            'partition': "fat",
            'nodes': 1,
            'ntasks_per_node': 128,
            'memory': "300G",
            'job_name': "MPAS_model"
        },
        'logging': {
            'level': "INFO",
            'format': "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            'file': "monan_execution.log"
        }
    }
    
    # Salvar arquivo de exemplo
    with open('config.yml.example', 'w', encoding='utf-8') as f:
        yaml.dump(config_example, f, default_flow_style=False, 
                 allow_unicode=True, indent=2)
    
    print("SUCCESS: Arquivo config.yml.example criado")
    
    # Se config.yml não existir, criar cópia
    if not Path('config.yml').exists():
        shutil.copy('config.yml.example', 'config.yml')
        print("SUCCESS: Arquivo config.yml criado (baseado no exemplo)")
        print("WARNING:  IMPORTANTE: Edite config.yml com seus caminhos específicos!")
    else:
        print("INFO:  Arquivo config.yml já existe (não foi sobrescrito)")

def create_directory_structure():
    """Cria estrutura de diretórios do projeto"""
    
    directories = [
        'src',
        'logs',
        'templates',
        'docs',
        'tests',
        'examples',
        'output'
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
    
    print("SUCCESS: Estrutura de diretórios criada")

def create_sample_scripts():
    """Cria scripts de exemplo e utilitários"""
    
    # Script de limpeza
    cleanup_script = '''#!/bin/bash
# Script para limpeza de arquivos temporários

echo "Limpando arquivos temporários do MONAN/MPAS Runner..."

# Limpar logs antigos (mais de 30 dias)
find logs/ -name "*.log" -mtime +30 -delete 2>/dev/null

# Limpar arquivos temporários do WPS
find . -name "GRIBFILE.*" -delete 2>/dev/null
find . -name "ungrib.log*" -delete 2>/dev/null

# Limpar arquivos temporários do MPAS
find . -name "log.init_atmosphere.*" -delete 2>/dev/null
find . -name "log.atmosphere.*" -delete 2>/dev/null

echo "SUCCESS: Limpeza concluída"
'''
    
    with open('cleanup.sh', 'w') as f:
        f.write(cleanup_script)
    os.chmod('cleanup.sh', 0o755)
    
    # Script de monitoramento
    monitor_script = '''#!/bin/bash
# Script para monitorar execução do MONAN

echo "=== MONAN/MPAS Monitor ==="
echo "Data: $(date)"
echo

# Verificar jobs SLURM
echo "Jobs SLURM ativos:"
squeue -u $USER -o "%.18i %.9P %.50j %.8u %.8T %.10M %.9l %.6D %R" 2>/dev/null || echo "SLURM não disponível"
echo

# Verificar logs recentes
if [ -f "monan_execution.log" ]; then
    echo "Últimas 10 linhas do log:"
    tail -10 monan_execution.log
else
    echo "Log principal não encontrado"
fi
echo

# Verificar uso de disco
echo "Uso de disco nas simulações:"
du -sh */20*/  2>/dev/null | tail -5 || echo "Nenhuma simulação encontrada"
'''
    
    with open('monitor.sh', 'w') as f:
        f.write(monitor_script)
    os.chmod('monitor.sh', 0o755)
    
    print("SUCCESS: Scripts utilitários criados (cleanup.sh, monitor.sh)")

def check_dependencies():
    """Verifica se dependências estão instaladas"""
    
    print("Verificando dependências Python...")
    
    required_packages = ['yaml', 'requests', 'tqdm']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"SUCCESS: {package}")
        except ImportError:
            missing_packages.append(package)
            print(f"ERROR: {package}")
    
    if missing_packages:
        print(f"\nWARNING:  Pacotes faltantes: {', '.join(missing_packages)}")
        print("Execute: pip install -r requirements.txt")
        return False
    else:
        print("\nSUCCESS: Todas as dependências estão instaladas")
        return True

def main():
    """Função principal de configuração"""
    
    print("="*60)
    print("CONFIGURAÇÃO DO MONAN/MPAS RUNNER")
    print("="*60)
    
    try:
        # 1. Verificar dependências
        deps_ok = check_dependencies()
        
        # 2. Criar estrutura de diretórios
        create_directory_structure()
        
        # 3. Criar arquivo de configuração
        create_config_example()
        
        # 4. Criar scripts utilitários
        create_sample_scripts()
        
        print("\n" + "="*60)
        print("CONFIGURAÇÃO CONCLUÍDA")
        print("="*60)
        
        if deps_ok:
            print("SUCCESS: Sistema pronto para uso!")
        else:
            print("WARNING:  Instale as dependências antes de continuar")
        
        print("\nPróximos passos:")
        print("1. Edite config.yml com seus caminhos específicos")
        print("2. Execute: python main.py --help")
        print("3. Para teste: python main.py --step download --verbose")
        
        print("\nArquivos criados:")
        print("- config.yml.example (template de configuração)")
        print("- config.yml (sua configuração)")
        print("- cleanup.sh (script de limpeza)")
        print("- monitor.sh (script de monitoramento)")
        
    except Exception as e:
        print(f"ERROR: Erro durante configuração: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
