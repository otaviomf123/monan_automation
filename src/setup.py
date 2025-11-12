#!/usr/bin/env python3
"""
Script de configuracao e instalacao do MONAN/MPAS Runner
"""

from pathlib import Path
import yaml
import shutil
import os

def create_config_example():
    """Cria arquivo de configuracao de exemplo"""
    
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
        'data_source': {
            'type': "gfs"  # Options: "gfs" or "era5"
        },
        'data_sources': {
            'gfs_base_url': "https://noaa-gfs-bdp-pds.s3.amazonaws.com",
            'forecast_hours': {
                'start': 0,
                'end': 240,
                'step': 3
            }
        },
        'era5': {
            'pressure_levels': [
                '10', '30', '50', '70', '100', '150', '200', '250', '300', '350',
                '400', '500', '600', '650', '700', '750', '775', '800', '825',
                '850', '875', '900', '925', '950', '975', '1000'
            ],
            'grid_resolution': '0.25/0.25',
            'download_interval_hours': 3
        },
        'paths': {
            'wps_dir': "/home/SEU_USUARIO/mpas/wps",
            'link_grib': "/home/SEU_USUARIO/mpas/wps/link_grib.csh",
            'ungrib_exe': "/home/SEU_USUARIO/mpas/wps/ungrib.exe",
            'vtable_gfs': "/home/SEU_USUARIO/mpas/wps/ungrib/Variable_Tables/Vtable.GFS",
            'vtable_ecmwf': "/home/SEU_USUARIO/mpas/wps/ungrib/Variable_Tables/Vtable.ECMWF",
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
    
    # Se config.yml nao existir, criar copia
    if not Path('config.yml').exists():
        shutil.copy('config.yml.example', 'config.yml')
        print("SUCCESS: Arquivo config.yml criado (baseado no exemplo)")
        print("WARNING:  IMPORTANTE: Edite config.yml com seus caminhos especificos!")
    else:
        print("INFO:  Arquivo config.yml ja existe (nao foi sobrescrito)")

def create_directory_structure():
    """Cria estrutura de diretorios do projeto"""
    
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
    
    print("SUCCESS: Estrutura de diretorios criada")

def create_sample_scripts():
    """Cria scripts de exemplo e utilitarios"""
    
    # Script de limpeza
    cleanup_script = '''#!/bin/bash
# Script para limpeza de arquivos temporarios

echo "Limpando arquivos temporarios do MONAN/MPAS Runner..."

# Limpar logs antigos (mais de 30 dias)
find logs/ -name "*.log" -mtime +30 -delete 2>/dev/null

# Limpar arquivos temporarios do WPS
find . -name "GRIBFILE.*" -delete 2>/dev/null
find . -name "ungrib.log*" -delete 2>/dev/null

# Limpar arquivos temporarios do MPAS
find . -name "log.init_atmosphere.*" -delete 2>/dev/null
find . -name "log.atmosphere.*" -delete 2>/dev/null

echo "SUCCESS: Limpeza concluida"
'''
    
    with open('cleanup.sh', 'w') as f:
        f.write(cleanup_script)
    os.chmod('cleanup.sh', 0o755)
    
    # Script de monitoramento
    monitor_script = '''#!/bin/bash
# Script para monitorar execucao do MONAN

echo "=== MONAN/MPAS Monitor ==="
echo "Data: $(date)"
echo

# Verificar jobs SLURM
echo "Jobs SLURM ativos:"
squeue -u $USER -o "%.18i %.9P %.50j %.8u %.8T %.10M %.9l %.6D %R" 2>/dev/null || echo "SLURM nao disponivel"
echo

# Verificar logs recentes
if [ -f "monan_execution.log" ]; then
    echo "Ultimas 10 linhas do log:"
    tail -10 monan_execution.log
else
    echo "Log principal nao encontrado"
fi
echo

# Verificar uso de disco
echo "Uso de disco nas simulacoes:"
du -sh */20*/  2>/dev/null | tail -5 || echo "Nenhuma simulacao encontrada"
'''
    
    with open('monitor.sh', 'w') as f:
        f.write(monitor_script)
    os.chmod('monitor.sh', 0o755)
    
    print("SUCCESS: Scripts utilitarios criados (cleanup.sh, monitor.sh)")

def check_dependencies():
    """Verifica se dependencias estao instaladas"""
    
    print("Verificando dependencias Python...")
    
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
        print("\nSUCCESS: Todas as dependencias estao instaladas")
        return True

def main():
    """Funcao principal de configuracao"""
    
    print("="*60)
    print("CONFIGURACAO DO MONAN/MPAS RUNNER")
    print("="*60)
    
    try:
        # 1. Verificar dependencias
        deps_ok = check_dependencies()
        
        # 2. Criar estrutura de diretorios
        create_directory_structure()
        
        # 3. Criar arquivo de configuracao
        create_config_example()
        
        # 4. Criar scripts utilitarios
        create_sample_scripts()
        
        print("\n" + "="*60)
        print("CONFIGURACAO CONCLUIDA")
        print("="*60)
        
        if deps_ok:
            print("SUCCESS: Sistema pronto para uso!")
        else:
            print("WARNING:  Instale as dependencias antes de continuar")
        
        print("\nProximos passos:")
        print("1. Edite config.yml com seus caminhos especificos")
        print("2. Execute: python main.py --help")
        print("3. Para teste: python main.py --step download --verbose")
        
        print("\nArquivos criados:")
        print("- config.yml.example (template de configuracao)")
        print("- config.yml (sua configuracao)")
        print("- cleanup.sh (script de limpeza)")
        print("- monitor.sh (script de monitoramento)")
        
    except Exception as e:
        print(f"ERROR: Erro durante configuracao: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
