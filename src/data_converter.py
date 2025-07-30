"""
Conversor de Dados MPAS
========================

Módulo para converter dados MPAS de grade não estruturada para grade regular
"""

import logging
import numpy as np
import xarray as xr
from pathlib import Path
from sklearn.neighbors import BallTree
from typing import Optional, Dict, Tuple, List
import os

from .config_loader import ConfigLoader
from .utils import validate_file_exists, get_file_size_mb


class MPASDataConverter:
    """Classe para conversão de dados MPAS para grade regular"""
    
    def __init__(self, config: ConfigLoader):
        """
        Inicializa o conversor de dados
        
        Args:
            config: Objeto de configuração
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Configurações de conversão
        self.conversion_config = config.get('conversion', {})
        self.grid_config = self.conversion_config.get('grid', {})
        
        # Configurações padrão se não estiverem no config
        self.lon_min = self.grid_config.get('lon_min', -90)
        self.lon_max = self.grid_config.get('lon_max', -20)
        self.lat_min = self.grid_config.get('lat_min', -45)
        self.lat_max = self.grid_config.get('lat_max', 25)
        self.resolution = self.grid_config.get('resolution', 0.1)
        self.max_dist_km = self.grid_config.get('max_dist_km', 30)
        
    def setup_interpolation_weights(self, static_file: Path, weights_dir: Path) -> Dict:
        """
        Configura os pesos de interpolação e salva para reutilização
        
        Args:
            static_file: Arquivo estático MPAS
            weights_dir: Diretório para salvar os pesos
            
        Returns:
            Dicionário com informações da grade regular
        """
        self.logger.info("Configurando pesos de interpolação...")
        
        # Criar diretório se não existir
        weights_dir.mkdir(parents=True, exist_ok=True)
        
        # Carregar arquivo estático
        with xr.open_dataset(static_file) as ds_static:
            # Coordenadas MPAS
            lat_mpas = np.rad2deg(ds_static['latCell'].values)
            lon_mpas = np.rad2deg(ds_static['lonCell'].values)
            lon_mpas = np.where(lon_mpas > 180, lon_mpas - 360, lon_mpas)
        
        # Construir BallTree
        coords = np.column_stack([np.deg2rad(lat_mpas), np.deg2rad(lon_mpas)])
        tree_mpas = BallTree(coords, metric='haversine')
        
        # Grade regular
        lon_reg = np.arange(self.lon_min, self.lon_max, self.resolution)
        lat_reg = np.arange(self.lat_min, self.lat_max, self.resolution)
        lon_reg_2d, lat_reg_2d = np.meshgrid(lon_reg, lat_reg)
        
        # Query BallTree
        coords_reg = np.column_stack([
            np.deg2rad(lat_reg_2d.ravel()), 
            np.deg2rad(lon_reg_2d.ravel())
        ])
        
        self.logger.info("Calculando distâncias e índices...")
        dists, ilocs = tree_mpas.query(coords_reg, k=3)
        dists_km = dists * 6371  # converter para km
        
        # Calcular pesos de distância inversa
        self.logger.info("Calculando pesos...")
        pesos_inv = 1.0 / (dists_km**2 + 1e-10)
        pesos_inv = pesos_inv / np.sum(pesos_inv, axis=-1)[:, np.newaxis]
        
        # Salvar arquivos
        ilocs_file = weights_dir / "ilocs_interpolation.npy"
        dists_file = weights_dir / "dists_interpolation.npy"
        pesos_file = weights_dir / "pesos_interpolation.npy"
        
        np.save(ilocs_file, ilocs)
        np.save(dists_file, dists_km)
        np.save(pesos_file, pesos_inv)
        
        # Salvar informações da grade
        grid_info = {
            'lon_reg_2d': lon_reg_2d,
            'lat_reg_2d': lat_reg_2d,
            'shape': lon_reg_2d.shape,
            'ilocs_file': str(ilocs_file),
            'dists_file': str(dists_file),
            'pesos_file': str(pesos_file)
        }
        
        self.logger.info(f"Pesos salvos em: {weights_dir}")
        self.logger.info(f"Shape da grade regular: {lon_reg_2d.shape}")
        
        return grid_info
    
    def load_interpolation_weights(self, weights_dir: Path) -> Dict:
        """
        Carrega os pesos de interpolação salvos
        
        Args:
            weights_dir: Diretório com os pesos salvos
            
        Returns:
            Dicionário com índices, distâncias e pesos
        """
        ilocs_file = weights_dir / "ilocs_interpolation.npy"
        dists_file = weights_dir / "dists_interpolation.npy"
        pesos_file = weights_dir / "pesos_interpolation.npy"
        
        if not all(f.exists() for f in [ilocs_file, dists_file, pesos_file]):
            raise FileNotFoundError("Arquivos de pesos não encontrados. Execute setup_interpolation_weights primeiro.")
        
        ilocs = np.load(ilocs_file)
        dists_km = np.load(dists_file)
        pesos = np.load(pesos_file)
        
        return {
            'ilocs': ilocs,
            'dists_km': dists_km,
            'pesos': pesos
        }
    
    def interpolate_field_to_regular(self, field_mpas: np.ndarray, 
                                   interpolation_data: Dict, 
                                   grid_shape: Tuple) -> np.ndarray:
        """
        Interpola um campo MPAS para grade regular
        
        Args:
            field_mpas: Campo nos pontos MPAS
            interpolation_data: Dados de interpolação
            grid_shape: Forma da grade regular
            
        Returns:
            Campo interpolado na grade regular
        """
        ilocs = interpolation_data['ilocs']
        dists_km = interpolation_data['dists_km']
        pesos = interpolation_data['pesos']
        
        # Interpolar
        field_interp = np.sum(field_mpas[ilocs] * pesos, axis=-1)
        
        # Aplicar máscara de distância
        field_interp = np.where(dists_km[:, 0] > self.max_dist_km, np.nan, field_interp)
        
        # Reshape para grade 2D
        return field_interp.reshape(grid_shape)
    
    def get_netcdf_description(self, ds: xr.Dataset) -> Dict:
        """
        Extrai descrição automática do NetCDF
        
        Args:
            ds: Dataset NetCDF
            
        Returns:
            Informações sobre variáveis 2D
        """
        vars_2d = {}
        
        for var_name, var in ds.data_vars.items():
            # Verificar se é 2D (Time, nCells)
            if len(var.dims) == 2 and 'nCells' in var.dims:
                info = {
                    'dims': var.dims,
                    'shape': var.shape,
                    'dtype': var.dtype,
                    'attrs': dict(var.attrs) if var.attrs else {}
                }
                
                # Tentar extrair descrição dos atributos
                description = ""
                if 'long_name' in var.attrs:
                    description = var.attrs['long_name']
                elif 'description' in var.attrs:
                    description = var.attrs['description']
                elif 'standard_name' in var.attrs:
                    description = var.attrs['standard_name']
                
                info['description'] = description
                vars_2d[var_name] = info
        
        return vars_2d
    
    def convert_single_file(self, input_file: Path, output_file: Path, 
                          static_file: Path, weights_dir: Path,
                          force_recalc: bool = False) -> bool:
        """
        Converte um único arquivo NetCDF MPAS para grade regular
        
        Args:
            input_file: Arquivo NetCDF MPAS de entrada
            output_file: Arquivo NetCDF de saída
            static_file: Arquivo estático MPAS
            weights_dir: Diretório dos pesos
            force_recalc: Forçar recálculo dos pesos
            
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            self.logger.info(f"Convertendo: {input_file.name}")
            
            # Verificar se arquivo de entrada existe
            if not validate_file_exists(input_file, min_size=1024):  # Min 1KB
                self.logger.error(f"Arquivo de entrada inválido: {input_file}")
                return False
            
            # Carregar datasets
            ds_input = xr.open_dataset(input_file)
            
            # Configurar ou carregar pesos
            weights_exist = all((weights_dir / f).exists() 
                              for f in ["ilocs_interpolation.npy", "dists_interpolation.npy", "pesos_interpolation.npy"])
            
            if not weights_exist or force_recalc:
                self.logger.info("Calculando pesos de interpolação...")
                grid_info = self.setup_interpolation_weights(static_file, weights_dir)
            else:
                self.logger.debug("Carregando pesos existentes...")
                # Recriar informações da grade
                lon_reg = np.arange(self.lon_min, self.lon_max, self.resolution)
                lat_reg = np.arange(self.lat_min, self.lat_max, self.resolution)
                lon_reg_2d, lat_reg_2d = np.meshgrid(lon_reg, lat_reg)
                grid_info = {
                    'lon_reg_2d': lon_reg_2d,
                    'lat_reg_2d': lat_reg_2d,
                    'shape': lon_reg_2d.shape
                }
            
            # Carregar dados de interpolação
            interpolation_data = self.load_interpolation_weights(weights_dir)
            
            # Obter descrição das variáveis
            vars_2d = self.get_netcdf_description(ds_input)
            self.logger.debug(f"Encontradas {len(vars_2d)} variáveis 2D")
            
            # Criar dataset de saída
            lon_reg_1d = np.arange(self.lon_min, self.lon_max, self.resolution)
            lat_reg_1d = np.arange(self.lat_min, self.lat_max, self.resolution)
            
            # Coordenadas
            coords = {
                'lon': lon_reg_1d,
                'lat': lat_reg_1d,
                'time': ds_input.coords['Time'] if 'Time' in ds_input.coords else [0]
            }
            
            # Interpolar todas as variáveis 2D
            data_vars = {}
            
            for var_name, var_info in vars_2d.items():
                self.logger.debug(f"Interpolando {var_name}...")
                
                var_data = ds_input[var_name].values
                
                # Verificar se tem dimensão temporal
                if var_data.ndim == 2:  # (Time, nCells)
                    interpolated_data = np.zeros((var_data.shape[0], *grid_info['shape']))
                    
                    for t in range(var_data.shape[0]):
                        interpolated_data[t] = self.interpolate_field_to_regular(
                            var_data[t], interpolation_data, grid_info['shape']
                        )
                        
                    data_vars[var_name] = (
                        ['time', 'lat', 'lon'], 
                        interpolated_data,
                        var_info['attrs']
                    )
                else:  # (nCells,) - sem dimensão temporal
                    interpolated_data = self.interpolate_field_to_regular(
                        var_data, interpolation_data, grid_info['shape']
                    )
                    
                    data_vars[var_name] = (
                        ['lat', 'lon'], 
                        interpolated_data,
                        var_info['attrs']
                    )
            
            # Adicionar coordenadas lat/lon 2D
            data_vars['longitude'] = (['lat', 'lon'], grid_info['lon_reg_2d'])
            data_vars['latitude'] = (['lat', 'lon'], grid_info['lat_reg_2d'])
            
            # Criar dataset
            ds_output = xr.Dataset(data_vars, coords=coords)
            
            # Adicionar atributos globais
            ds_output.attrs.update({
                'title': 'MPAS data interpolated to regular grid',
                'source_file': str(input_file),
                'interpolation_method': 'Inverse distance weighting (k=3)',
                'max_distance_km': self.max_dist_km,
                'grid_resolution_deg': self.resolution,
                'grid_bounds': f'lon:[{self.lon_min}, {self.lon_max}], lat:[{self.lat_min}, {self.lat_max}]'
            })
            
            # Criar diretório de saída se necessário
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Salvar
            self.logger.debug(f"Salvando em: {output_file}")
            ds_output.to_netcdf(output_file)
            
            # Fechar datasets
            ds_input.close()
            ds_output.close()
            
            # Verificar arquivo de saída
            output_size_mb = get_file_size_mb(output_file)
            self.logger.info(f" Conversão concluída: {output_file.name} ({output_size_mb:.1f} MB)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao converter {input_file}: {e}")
            self.logger.exception("Detalhes do erro:")
            return False
    
    def find_diag_files(self, run_dir: Path) -> List[Path]:
        """
        Encontra todos os arquivos de diagnóstico na pasta run
        
        Args:
            run_dir: Diretório de execução do modelo
            
        Returns:
            Lista de arquivos de diagnóstico encontrados
        """
        # Padrões de arquivos de diagnóstico
        patterns = [
            'diag.*.nc',
            'history.*.nc',
            'output.*.nc'
        ]
        
        diag_files = []
        for pattern in patterns:
            files = list(run_dir.glob(pattern))
            diag_files.extend(files)
        
        # Ordenar por nome (que inclui data/hora)
        diag_files.sort()
        
        self.logger.info(f"Encontrados {len(diag_files)} arquivos de diagnóstico")
        for f in diag_files[:5]:  # Mostrar apenas os primeiros 5
            self.logger.debug(f"  - {f.name}")
        if len(diag_files) > 5:
            self.logger.debug(f"  ... e mais {len(diag_files) - 5} arquivos")
        
        return diag_files
    
    def convert_all_diag_files(self, run_dir: Path, static_file: Path) -> bool:
        """
        Converte todos os arquivos de diagnóstico para grade regular
        
        Args:
            run_dir: Diretório de execução do modelo
            static_file: Arquivo estático MPAS
            
        Returns:
            True se todas as conversões foram bem-sucedidas
        """
        self.logger.info("="*50)
        self.logger.info("CONVERTENDO DADOS PARA GRADE REGULAR")
        self.logger.info("="*50)
        
        try:
            # Encontrar arquivos de diagnóstico
            diag_files = self.find_diag_files(run_dir)
            
            if not diag_files:
                self.logger.warning("Nenhum arquivo de diagnóstico encontrado")
                return True  # Não é erro fatal
            
            # Verificar arquivo estático
            if not validate_file_exists(static_file, min_size=1024*1024):  # Min 1MB
                self.logger.error(f"Arquivo estático inválido: {static_file}")
                return False
            
            # Diretórios
            weights_dir = run_dir / "interpolation_weights"
            output_dir = run_dir / "regular_grid"
            
            # Contadores
            success_count = 0
            total_files = len(diag_files)
            
            # Converter cada arquivo
            for i, diag_file in enumerate(diag_files, 1):
                self.logger.info(f"[{i}/{total_files}] Processando: {diag_file.name}")
                
                # Nome do arquivo de saída
                output_name = f"regular_{diag_file.name}"
                output_file = output_dir / output_name
                
                # Pular se já existe e é recente
                if (output_file.exists() and 
                    output_file.stat().st_mtime > diag_file.stat().st_mtime):
                    self.logger.info(f"  ↳ Arquivo já convertido (mais recente): {output_name}")
                    success_count += 1
                    continue
                
                # Converter
                if self.convert_single_file(diag_file, output_file, static_file, 
                                          weights_dir, force_recalc=(i == 1)):
                    success_count += 1
                else:
                    self.logger.error(f"  ↳ Falha na conversão: {diag_file.name}")
            
            # Relatório final
            self.logger.info("="*50)
            self.logger.info("RELATÓRIO DE CONVERSÃO")
            self.logger.info("="*50)
            self.logger.info(f"Total de arquivos: {total_files}")
            self.logger.info(f"Conversões bem-sucedidas: {success_count}")
            self.logger.info(f"Conversões com falha: {total_files - success_count}")
            
            if success_count == total_files:
                self.logger.info("Todas as conversões foram bem-sucedidas!")
                self.logger.info(f"Arquivos convertidos salvos em: {output_dir}")
            else:
                self.logger.warning(f"  {total_files - success_count} conversões falharam")
            
            return success_count > 0  # Sucesso se pelo menos uma conversão funcionou
            
        except Exception as e:
            self.logger.error(f"Erro durante conversão de dados: {e}")
            self.logger.exception("Detalhes do erro:")
            return False
    
    def get_conversion_summary(self, run_dir: Path) -> Dict:
        """
        Retorna resumo das conversões realizadas
        
        Args:
            run_dir: Diretório de execução
            
        Returns:
            Dicionário com resumo das conversões
        """
        output_dir = run_dir / "regular_grid"
        
        if not output_dir.exists():
            return {'converted_files': 0, 'total_size_mb': 0, 'files': []}
        
        converted_files = list(output_dir.glob("regular_*.nc"))
        total_size = sum(f.stat().st_size for f in converted_files)
        
        return {
            'converted_files': len(converted_files),
            'total_size_mb': total_size / (1024 * 1024),
            'files': [f.name for f in converted_files],
            'output_dir': str(output_dir)
        }
