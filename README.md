# MONAN/MPAS Runner

Sistema automatizado para execução do modelo MONAN/MPAS versão 1.3/8.x no ambiente CEMPA.

## Índice

- [Visão Geral](#visão-geral)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Uso](#uso)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Etapas do Pipeline](#etapas-do-pipeline)
- [Monitoramento](#monitoramento)
- [Resolução de Problemas](#resolução-de-problemas)

## Visão Geral

Este sistema automatiza completamente o pipeline de execução do modelo MONAN/MPAS, desde o download dos dados GFS até a submissão do job no SLURM. O pipeline é dividido em 5 etapas principais:

1. **Download de Dados**: Download automático dos dados GFS do NOAA
2. **Processamento WPS**: Preparação dos dados meteorológicos usando ungrib
3. **Condições Iniciais**: Geração das condições iniciais do modelo
4. **Condições de Fronteira**: Geração das condições de fronteira laterais
5. **Execução do Modelo**: Configuração e submissão do job SLURM

## Pré-requisitos

### Software Necessário

- Python 3.8+
- MONAN/MPAS 1.3 compilado
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

3. **Configure o arquivo de configuração:**
```bash
cp config.yml.example config.yml
# Edite config.yml com seus caminhos específicos
```

## Configuração

### Arquivo Principal: `config.yml`

O arquivo `config.yml` centraliza todas as configurações do sistema. Principais seções:

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

#### Configurações SLURM
```yaml
slurm:
  partition: "fat"
  nodes: 1
  ntasks_per_node: 128
  memory: "300G"
  job_name: "MPAS_model"
```

### Configurações Específicas

Para adaptar o sistema ao seu ambiente, edite especialmente:

- `general.base_dir`: Seu diretório de trabalho
- `paths.*`: Caminhos para seus executáveis compilados
- `slurm.*`: Configurações do seu cluster
- `domain.*`: Parâmetros do seu domínio de simulação

## Uso

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
    └── run_mpas.slurm
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
- **Sistema**: SLURM com MPI
- **Saída**: Arquivos de história e diagnóstico

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

-  **Download**: Todos os arquivos GFS baixados
-  **WPS**: Arquivos FILE:* gerados
-  **Init**: Arquivo `brasil_circle.init.nc` criado (>10MB)
-  **Boundary**: Arquivos `lbc.*.nc` gerados
-  **Run**: Job submetido com sucesso

## 🔧 Resolução de Problemas

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

#### 4. Erro no SLURM
```bash
# Verificar partição disponível
sinfo

# Verificar limites de recursos
sacctmgr show user $USER withassoc

# Testar submissão manual
sbatch 20250727/run/run_mpas.slurm
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

##  Testes e Validação

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

# Verificar conteúdo NetCDF (se ncdump disponível)
ncdump -h 20250727/init/brasil_circle.init.nc
ncdump -h 20250727/run/history.*.nc
```

### Reportar Problemas

Ao reportar problemas, inclua:

- Versão do Python e dependências
- Arquivo de configuração (sem dados sensíveis)
- Logs relevantes
- Passos para reproduzir o erro
- Ambiente (SO, cluster, etc.)

##  Suporte

- **Documentação**: Este README e comentários no código
- **Issues**: Use o sistema de issues do GitHub
- **Email**: [otavio.feitosa@cmcc.it]

##  Changelog

### v1.0.0 (2025-07-28)
- Implementação inicial do pipeline completo
- Suporte para MONAN/MPAS 1.3
- Configuração via YAML
- Sistema de logging robusto
- Documentação completa em português

---

**Desenvolvido para o ambiente CEMPA - Centro de Estudos Meteorológicos e Pesquisas Aplicadas**
