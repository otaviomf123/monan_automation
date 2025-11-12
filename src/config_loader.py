"""
Carregador de Configuracao
==========================

Modulo para carregar e gerenciar configuracoes do MONAN/MPAS
"""

import logging
import yaml
from pathlib import Path
from typing import Any, Dict, Optional


class ConfigLoader:
    """Carregador e gerenciador de configuracoes"""
    
    def __init__(self, config_file: str = 'config.yml'):
        """
        Inicializa o carregador de configuracao
        
        Args:
            config_file: Caminho para o arquivo de configuracao YAML
        """
        self.config_file = Path(config_file)
        self.config = self._load_config()
        self.logger = logging.getLogger(__name__)
        
    def _load_config(self) -> Dict[str, Any]:
        """
        Carrega o arquivo de configuracao YAML
        
        Returns:
            Dicionario com as configuracoes
            
        Raises:
            FileNotFoundError: Se o arquivo nao existir
            yaml.YAMLError: Se houver erro no parsing do YAML
        """
        if not self.config_file.exists():
            raise FileNotFoundError(f"Arquivo de configuracao nao encontrado: {self.config_file}")
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Erro ao processar arquivo YAML: {e}")
    
    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Obtem um valor da configuracao usando notacao de ponto
        
        Args:
            key: Chave em notacao de ponto (ex: 'general.base_dir')
            default: Valor padrao se a chave nao existir
            
        Returns:
            Valor da configuracao ou valor padrao
            
        Examples:
            >>> config.get('general.base_dir')
            '/home/otavio.feitosa/limited_area/test_furnas'
            >>> config.get('dates.run_date')
            '20250727'
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
                
        return value
    
    def set(self, key: str, value: Any) -> None:
        """
        Define um valor na configuracao usando notacao de ponto
        
        Args:
            key: Chave em notacao de ponto
            value: Valor a ser definido
        """
        keys = key.split('.')
        config_ref = self.config
        
        # Navega ate a penultima chave, criando dicionarios se necessario
        for k in keys[:-1]:
            if k not in config_ref:
                config_ref[k] = {}
            config_ref = config_ref[k]
        
        # Define o valor na ultima chave
        config_ref[keys[-1]] = value
    
    def get_paths(self) -> Dict[str, str]:
        """
        Retorna todos os caminhos configurados
        
        Returns:
            Dicionario com todos os caminhos
        """
        return self.get('paths', {})
    
    def get_dates(self) -> Dict[str, str]:
        """
        Retorna todas as configuracoes de data
        
        Returns:
            Dicionario com configuracoes de data
        """
        return self.get('dates', {})
    
    def get_domain_config(self) -> Dict[str, Any]:
        """
        Retorna configuracoes do dominio
        
        Returns:
            Dicionario com configuracoes do dominio
        """
        return self.get('domain', {})
    
    def get_physics_config(self) -> Dict[str, Any]:
        """
        Retorna configuracoes de fisica
        
        Returns:
            Dicionario com configuracoes de fisica
        """
        return self.get('physics', {})
    
    def get_slurm_config(self) -> Dict[str, Any]:
        """
        Retorna configuracoes do SLURM
        
        Returns:
            Dicionario com configuracoes do SLURM
        """
        return self.get('slurm', {})
    
    def get_execution_config(self) -> Dict[str, Any]:
        """
        Retorna configuracoes de execucao
        
        Returns:
            Dicionario com configuracoes de execucao (mode, cores)
        """
        return self.get('execution', {})
    
    def get_mpirun_config(self) -> Dict[str, Any]:
        """
        Retorna configuracoes do mpirun
        
        Returns:
            Dicionario com configuracoes do mpirun
        """
        return self.get('mpirun', {})
    
    def get_era5_config(self) -> Dict[str, Any]:
        """
        Retorna configuracoes do ERA5
        
        Returns:
            Dicionario com configuracoes do ERA5
        """
        return self.get('era5', {})
    
    def get_data_source_type(self) -> str:
        """
        Retorna o tipo de fonte de dados configurado
        
        Returns:
            'gfs' ou 'era5'
        """
        return self.get('data_source.type', 'gfs').lower()
    
    def get_vtable_path(self) -> str:
        """
        Retorna o caminho do Vtable apropriado baseado na fonte de dados
        
        Returns:
            Caminho para Vtable.GFS ou Vtable.ECMWF
        """
        data_source = self.get_data_source_type()
        
        if data_source == 'era5':
            return self.get('paths.vtable_ecmwf')
        else:
            return self.get('paths.vtable_gfs')
    
    def validate_config(self) -> bool:
        """
        Valida se as configuracoes essenciais estao presentes
        
        Returns:
            True se valido, False caso contrario
        """
        required_keys = [
            'general.base_dir',
            'dates.run_date',
            'dates.start_time',
            'dates.end_time',
            'paths.mpas_init_exe',
            'paths.monan_exe',
            'paths.static_file'
        ]
        
        missing_keys = []
        for key in required_keys:
            if self.get(key) is None:
                missing_keys.append(key)
        
        if missing_keys:
            self.logger.error(f"ERROR: Required configuration keys missing: {missing_keys}")
            return False
        
        return True
    
    def save_config(self, output_file: Optional[str] = None) -> None:
        """
        Salva a configuracao atual em um arquivo YAML
        
        Args:
            output_file: Arquivo de saida (padrao: mesmo arquivo carregado)
        """
        output_path = Path(output_file) if output_file else self.config_file
        
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.config, f, default_flow_style=False, 
                     allow_unicode=True, indent=2)
        
        self.logger.info(f"INFO: Configuration saved to: {output_path}")
    
    def __str__(self) -> str:
        """Representacao em string da configuracao"""
        return f"ConfigLoader(config_file='{self.config_file}')"
    
    def __repr__(self) -> str:
        """Representacao detalhada da configuracao"""
        return f"ConfigLoader(config_file='{self.config_file}', keys={list(self.config.keys())})"
