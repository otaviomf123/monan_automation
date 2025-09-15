# MONAN/MPAS Runner

Sistema automatizado para execução do modelo MONAN/MPAS versão 1.3/8.x no ambiente CEMPA.

## Visão Geral

O sistema automatiza completamente o pipeline de execução do modelo MONAN/MPAS, desde o download dos dados GFS até a submissão do job no SLURM e conversão dos dados para grade regular.

O pipeline é dividido em 6 etapas principais:

1. **Download de Dados** - Download automático dos dados GFS do NOAA
2. **Processamento WPS** - Preparação dos dados meteorológicos usando ungrib
3. **Condições Iniciais** - Geração das condições iniciais do modelo
4. **Condições de Fronteira** - Geração das condições de fronteira laterais
5. **Execução do Modelo** - Configuração e submissão do job SLURM
6. **Conversão de Dados** - Conversão automática dos dados de saída para grade regular

## Pré-requisitos

### Software Necessário

- Python 3.8+
- MONAN/MPAS 1.3+ compilado
- WPS (Weather Research and Forecasting Preprocessing System)
- Sistema SLURM para submissão de jobs
- MPI (OpenMPI ou MPICH)

### Bibliotecas Python

```bash
pip install -r requirements.txt
```

**requirements.txt:**
```
PyYAML>=6.0
requests>=2.28.0
tqdm>=4.64.0
xarray>=2023.1.0
numpy>=1.21.0
scikit-learn>=1.3.0
netCDF4>=1.6.0
```

### Estrutura de Diretórios Esperada

```
/home/otavio.feitosa/
├── limited_area/
│   ├── test_furnas/          # Diretório base (configurável)
│   └── MPAS-Limited-Area/    # Malha e arquivos estáticos
├── mpas/
│   ├── wps/                  # WPS compilado
│   ├── init/                 # init_atmosphere_model
│   └── monan/                # atmosphere_model e tabelas
└── .bashrc                   # Configurações do ambiente
```

## Instalação

1. **Clone o repositório:**
```bash
git clone https://github.com/seu-usuario/monan-mpas-runner.git
cd monan-mpas-runner
```

2. **Instale as dependências:**
```bash
pip install -r requirements.txt
```

3. **Configure o sistema:**
```bash
python setup.py
```

4. **Edite o arquivo de configuração:**
```bash
# Edite config.yml com seus caminhos específicos
nano config.yml
```

## Configuração

### Arquivo Principal: config.yml

#### Configurações Gerais
```yaml
general:
  base_dir: "/home/otavio.feitosa/limited_area/test_furnas"
  forecast_days: 10
```

#### Datas da Simulação
```yaml
dates:
  run_date: "20250727"      # Data da rodada (AAAAMMDD)
  cycle: "00"               # Ciclo (00, 06, 12, 18)
  start_time: "2025-07-27_00:00:00"
  end_time: "2025-08-06_00:00:00"
```

#### Caminhos dos Executáveis
```yaml
paths:
  # WPS
  wps_dir: "/home/otavio.feitosa/mpas/wps"
  ungrib_exe: "/home/otavio.feitosa/mpas/wps/ungrib.exe"
  
  # MPAS/MONAN
  mpas_init_exe: "/home/otavio.feitosa/mpas/init/init_atmosphere_model"
  monan_exe: "/home/otavio.feitosa/mpas/monan/atmosphere_model"
  
  # Dados geográficos
  static_file: "/home/otavio.feitosa/limited_area/MPAS-Limited-Area/brasil_circle.static.nc"
  decomp_file_prefix: "/home/otavio.feitosa/limited_area/test_furnas/brasil_circle.graph.info.part."
```

#### Configurações de Conversão
```yaml
conversion:
  enabled: true           # Habilitar conversão automática
  grid:
    lon_min: -90         # Longitude mínima
    lon_max: -20         # Longitude máxima  
    lat_min: -45         # Latitude mínima
    lat_max: 25          # Latitude máxima
    resolution: 0.1      # Resolução em graus
    max_dist_km: 30      # Distância máxima para interpolação
```

#### Configurações de Execução

##### Modo SLURM (Padrão)
```yaml
# Configuração de execução
execution:
  mode: "slurm"        # Modo de execução: "slurm" ou "mpirun"
  cores: 128           # Número de processos MPI (compartilhado entre ambos os modos)

# Configurações específicas do SLURM
slurm:
  partition: "fat"
  nodes: 1
  memory: "300G"
  job_name: "MPAS_model"
```

##### Modo MPI Direto
```yaml
# Configuração de execução
execution:
  mode: "mpirun"       # Execução direta com mpirun
  cores: 128           # Número de processos MPI

# Configurações específicas do mpirun
mpirun:
  host: "localhost"            # Host de destino para execução
  mpi_extra_args: ""           # Argumentos adicionais do mpirun (opcional)
  timeout_hours: 24            # Timeout de execução em horas
```

## Uso

### Verificar Configuração

Antes de executar, verifique se tudo está configurado corretamente:

```bash
python verify_setup.py
```

### Execução Completa

Executar todo o pipeline automaticamente:

```bash
python main.py
```

### Execução por Etapas

Execute etapas específicas conforme necessário:

```bash
# Apenas download dos dados
python main.py --step download

# Apenas processamento WPS
python main.py --step wps

# Apenas condições iniciais
python main.py --step init

# Apenas condições de fronteira
python main.py --step boundary

# Apenas execução do modelo
python main.py --step run

# Apenas conversão de dados
python main.py --step convert
```

### Opções Avançadas

```bash
# Usar arquivo de configuração específico
python main.py --config minha_config.yml

# Modo verboso (debug)
python main.py --verbose

# Combinação de opções
python main.py --config config_teste.yml --step download --verbose
```

### Exemplo de Uso Típico

```bash
# 1. Verificar configuração inicial
python verify_setup.py

# 2. Configurar nova simulação
cp config.yml config_20250727.yml
# Editar datas e parâmetros específicos

# 3. Executar pipeline completo
python main.py --config config_20250727.yml --verbose

# 4. Monitorar execução
tail -f monan_execution.log
```

## Estrutura do Projeto

```
monan-mpas-runner/
├── main.py                    # Script principal
├── verify_setup.py            # Verificação de configuração
├── config.yml                 # Configuração principal
├── requirements.txt           # Dependências Python
├── README.md                  # Esta documentação
├── src/                       # Módulos do sistema
│   ├── __init__.py
│   ├── config_loader.py       # Carregador de configuração
│   ├── data_downloader.py     # Download de dados GFS
│   ├── wps_processor.py       # Processamento WPS
│   ├── initial_conditions.py  # Condições iniciais
│   ├── boundary_conditions.py # Condições de fronteira
│   ├── model_runner.py        # Executor do modelo
│   ├── data_converter.py      # Conversor para grade regular
│   └── utils.py               # Utilitários gerais
└── logs/                      # Logs de execução (criado automaticamente)
```

### Estrutura de Saída

Para cada simulação, a seguinte estrutura é criada:

```
/base_dir/AAAAMMDD/
├── gfs/                    # Dados GFS baixados e processados
│   ├── gfs.t00z.pgrb2.0p25.f000
│   ├── gfs.t00z.pgrb2.0p25.f003
│   ├── ...
│   └── FILE:2025-07-*      # Saída do WPS
├── init/                   # Condições iniciais
│   ├── FILE:2025-07-* -> ../gfs/
│   └── brasil_circle.init.nc
├── bound/                  # Condições de fronteira
│   ├── FILE:2025-07-* -> ../gfs/
│   └── lbc.2025-07-*.nc
└── run/                    # Execução do modelo
    ├── atmosphere_model -> /path/to/monan/
    ├── *.TBL -> /path/to/monan/
    ├── brasil_circle.init.nc -> ../init/
    ├── lbc.*.nc -> ../bound/
    ├── namelist.atmosphere
    ├── streams.atmosphere
    ├── run_mpas.slurm
    ├── diag.*.nc           # Dados de saída do modelo
    ├── history.*.nc        # Arquivos de história
    ├── interpolation_weights/  # Pesos de interpolação
    └── regular_grid/       # Dados convertidos para grade regular
        └── regular_diag.*.nc
```

## Etapas do Pipeline

### 1. Download de Dados GFS

- **Função**: Baixa dados meteorológicos do NOAA
- **Fonte**: https://noaa-gfs-bdp-pds.s3.amazonaws.com
- **Formato**: GRIB2, resolução 0.25°
- **Cobertura**: 0 a 240h com intervalo de 3h
- **Saída**: Arquivos `gfs.t00z.pgrb2.0p25.fXXX`

### 2. Processamento WPS

- **Função**: Converte GRIB2 para formato WPS
- **Executável**: `ungrib.exe`
- **Configuração**: `namelist.wps` gerado automaticamente
- **Saída**: Arquivos `FILE:AAAA-MM-DD_HH`

### 3. Condições Iniciais

- **Função**: Interpola dados para malha MPAS
- **Executável**: `init_atmosphere_model`
- **Configuração**: `config_init_case = 7`
- **Saída**: `brasil_circle.init.nc`

### 4. Condições de Fronteira

- **Função**: Gera condições de fronteira laterais
- **Executável**: `init_atmosphere_model`
- **Configuração**: `config_init_case = 9`
- **Saída**: Arquivos `lbc.AAAA-MM-DD_HH.00.00.nc`

### 5. Execução do Modelo

- **Função**: Executa simulação atmosférica
- **Executável**: `atmosphere_model`
- **Sistemas Suportados**:
  - **SLURM**: Submissão de job para cluster (padrão)
  - **mpirun**: Execução direta via MPI
- **Saída**: Arquivos de história e diagnóstico

### 6. Conversão de Dados

- **Função**: Converte dados MPAS para grade regular
- **Método**: Interpolação por distância inversa ponderada
- **Entrada**: Arquivos `diag.*.nc`, `history.*.nc`
- **Saída**: Arquivos `regular_*.nc` em grade lat/lon regular
- **Configuração**: Grade configurável via `conversion` no config.yml

## Monitoramento

### Logs do Sistema

- **Log principal**: `monan_execution.log`
- **Logs SLURM**: Arquivos gerados pelo sistema de filas
- **Log de timing**: `mpas_execution_times.log`

### Comandos Úteis

```bash
# Monitorar log principal
tail -f monan_execution.log

# Verificar jobs SLURM
squeue -u $USER

# Monitorar timing da execução
tail -f /caminho/para/20250727/mpas_execution_times.log

# Verificar uso de disco
du -sh /caminho/para/20250727/
```

### Indicadores de Sucesso

- Download: Todos os arquivos GFS baixados
- WPS: Arquivos FILE:* gerados
- Init: Arquivo `brasil_circle.init.nc` criado (>10MB)
- Boundary: Arquivos `lbc.*.nc` gerados
- Run: Job submetido com sucesso
- Convert: Arquivos `regular_*.nc` gerados

## Resolução de Problemas

### Verificação do Setup

Antes de executar o pipeline, verifique se tudo está configurado corretamente:

```bash
# Verificar configuração e caminhos
python verify_setup.py
```

### Problemas Comuns

#### 1. Falha no Download
```bash
# Verificar conectividade
curl -I https://noaa-gfs-bdp-pds.s3.amazonaws.com

# Executar apenas download
python main.py --step download --verbose
```

#### 2. Erro no WPS
```bash
# Verificar executáveis
ls -la /home/otavio.feitosa/mpas/wps/ungrib.exe

# Verificar Vtable
ls -la /home/otavio.feitosa/mpas/wps/ungrib/Variable_Tables/Vtable.GFS
```

#### 3. Erro nas Condições Iniciais
```bash
# Verificar arquivo estático
ls -la /home/otavio.feitosa/limited_area/MPAS-Limited-Area/brasil_circle.static.nc

# Verificar arquivos FILE
ls -la 20250727/gfs/FILE:*
```

#### 4. Erro na Execução do Modelo

##### Modo SLURM
```bash
# Verificar partição disponível
sinfo

# Verificar limites de recursos
sacctmgr show user $USER withassoc

# Testar submissão manual
sbatch 20250727/run/run_mpas.slurm

# Verificar status do job
squeue -u $USER
```

##### Modo mpirun
```bash
# Verificar se MPI está disponível
which mpirun
mpirun --version

# Testar conectividade com host
ping compute-node-01  # Se host não for localhost

# Verificar recursos disponíveis
htop  # Verificar cores disponíveis

# Executar manualmente (para debug)
cd 20250727/run
mpirun -np 128 ./atmosphere_model
```

##### Problemas Comuns de Execução
```bash
# Host não configurado (mpirun)
# ERRO: Host não configurado para mpirun
# SOLUÇÃO: Configure mpirun.host no config.yml

# Timeout na execução (mpirun)
# ERRO: Execução excedeu timeout de X horas
# SOLUÇÃO: Aumente mpirun.timeout_hours no config.yml

# Modo de execução inválido
# ERRO: Modo de execução inválido: xxx
# SOLUÇÃO: Configure execution.mode como "slurm" ou "mpirun"
```

#### 5. Erro na Conversão
```bash
# Verificar dependências
python -c "import xarray, numpy, sklearn; print('OK')"

# Verificar arquivos de saída do modelo
ls -la 20250727/run/diag.*.nc
ls -la 20250727/run/history.*.nc

# Executar apenas conversão
python main.py --step convert --verbose
```

### Logs de Debug

Para diagnóstico detalhado:

```bash
# Executar com máximo detalhe
python main.py --verbose 2>&1 | tee debug.log

# Verificar logs específicos de cada etapa
grep "ERROR\|FATAL" monan_execution.log
```

### Limpeza e Reinício

```bash
# Limpar dados de uma rodada específica
rm -rf /base_dir/20250727

# Limpar apenas arquivos temporários
find /base_dir/20250727 -name "*.log" -delete
find /base_dir/20250727 -name "GRIBFILE.*" -delete
```

## Teste e Validação

### Teste Rápido

Execute um teste com menos dados:

```yaml
# Em config.yml, modificar temporariamente:
data_sources:
  forecast_hours:
    start: 0
    end: 12    # Apenas 12h em vez de 240h
    step: 3

general:
  forecast_days: 1  # Apenas 1 dia
```

### Validação de Resultados

```bash
# Verificar tamanhos dos arquivos
ls -lh 20250727/*/
du -sh 20250727/

# Verificar dados convertidos
ls -lh 20250727/run/regular_grid/
du -sh 20250727/run/regular_grid/

# Verificar conteúdo NetCDF (se ncdump disponível)
ncdump -h 20250727/init/brasil_circle.init.nc
ncdump -h 20250727/run/history.*.nc
ncdump -h 20250727/run/regular_grid/regular_diag.*.nc
```

## Exemplos Práticos

### Conversão de Dados

```python
# Exemplo de uso direto do conversor
from src.config_loader import ConfigLoader
from src.data_converter import MPASDataConverter

config = ConfigLoader('config.yml')
converter = MPASDataConverter(config)

# Converter todos os arquivos de uma simulação
run_dir = Path("20250727/run")
static_file = Path(config.get('paths.static_file'))
converter.convert_all_diag_files(run_dir, static_file)
```

### Configuração de Grade Personalizada

```yaml
# config.yml - Grade de alta resolução
conversion:
  enabled: true
  grid:
    lon_min: -60
    lon_max: -40
    lat_min: -30
    lat_max: -10
    resolution: 0.05  # ~5 km
    max_dist_km: 15
```

### Processamento em Lote

```bash
# Script para processar múltiplas datas
#!/bin/bash
for date in 20250725 20250726 20250727; do
    echo "Processando $date..."
    sed "s/run_date: .*/run_date: \"$date\"/" config.yml > config_$date.yml
    python main.py --config config_$date.yml
done
```

### Exemplos de Configuração para Diferentes Ambientes

#### Cluster com SLURM
```yaml
# config_cluster.yml
execution:
  mode: "slurm"
  cores: 256

slurm:
  partition: "compute"
  nodes: 2
  memory: "500G"
  job_name: "MONAN_simulation"
```

#### Estação de Trabalho Local
```yaml
# config_workstation.yml
execution:
  mode: "mpirun"
  cores: 32

mpirun:
  host: "localhost"
  timeout_hours: 48
  mpi_extra_args: "--bind-to core --report-bindings"
```

#### Execução em Host Remoto
```yaml
# config_remote.yml
execution:
  mode: "mpirun"
  cores: 128

mpirun:
  host: "compute-node-01"
  timeout_hours: 72
  mpi_extra_args: "-x LD_LIBRARY_PATH -x PATH"
```

## Changelog

### v1.1.0 (2025-09-10)
- **NOVO**: Suporte para execução direta com mpirun (alternativa ao SLURM)
- **NOVO**: Configuração unificada de cores (evita duplicação)
- **NOVO**: Validação e tratamento de erro robusto para ambos os modos
- **MELHORADO**: Sistema de logging com timeout e informações de execução
- **MELHORADO**: Documentação com exemplos para diferentes ambientes

### v1.0.0 (2025-07-28)
- Implementação inicial do pipeline completo
- Suporte para MONAN/MPAS 1.3
- Configuração via YAML
- Sistema de logging robusto
- Conversão automática para grade regular
- Interpolação por distância inversa ponderada
- Documentação completa em português

---

**Desenvolvido para o ambiente CEMPA - Centro de Estudos Meteorológicos e Pesquisas Aplicadas**
