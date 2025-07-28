"""
Carregador de Configuração
==========================

Módulo para carregar e gerenciar configurações do MONAN/MPAS
"""

import logging
import yaml
from pathlib import Path
from typing import Any, Dict, Optional


class ConfigLoader:
    """Carregador e gerenciador de configurações"""
    
    def __init__(self, config_file: str = 'config.yml'):
        """
        Inicializa o carregador de configuração
        
        Args:
            config_file: Caminho para o arquivo de configuração YAML
        """
        self.config_file = Path(config_file)
        self.config = self._load_config()
        self.logger = logging.getLogger(__name__)
        
    def _load_config(self) -> Dict[str, Any]:
        """
        Carrega o arquivo de configuração YAML
        
        Returns:
            Dicionário com as configurações
            
        Raises:
            FileNotFoundError: Se o arquivo não existir
            yaml.YAMLError: Se houver erro no parsing do YAML
        """
        if not self.config_file.exists():
            raise FileNotFoundError(f"Arquivo de configuração não encontrado: {self.config_file}")
        
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Erro ao processar arquivo YAML: {e}")
    
    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Obtém um valor da configuração usando notação de ponto
        
        Args:
            key: Chave em notação de ponto (ex: 'general.base_dir')
            default: Valor padrão se a chave não existir
            
        Returns:
            Valor da configuração ou valor padrão
            
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
        Define um valor na configuração usando notação de ponto
        
        Args:
            key: Chave em notação de ponto
            value: Valor a ser definido
        """
        keys = key.split('.')
        config_ref = self.config
        
        # Navega até a penúltima chave, criando dicionários se necessário
        for k in keys[:-1]:
            if k not in config_ref:
                config_ref[k] = {}
            config_ref = config_ref[k]
        
        # Define o valor na última chave
        config_ref[keys[-1]] = value
    
    def get_paths(self) -> Dict[str, str]:
        """
        Retorna todos os caminhos configurados
        
        Returns:
            Dicionário com todos os caminhos
        """
        return self.get('paths', {})
    
    def get_dates(self) -> Dict[str, str]:
        """
        Retorna todas as configurações de data
        
        Returns:
            Dicionário com configurações de data
        """
        return self.get('dates', {})
    
    def get_domain_config(self) -> Dict[str, Any]:
        """
        Retorna configurações do domínio
        
        Returns:
            Dicionário com configurações do domínio
        """
        return self.get('domain', {})
    
    def get_physics_config(self) -> Dict[str, Any]:
        """
        Retorna configurações de física
        
        Returns:
            Dicionário com configurações de física
        """
        return self.get('physics', {})
    
    def get_slurm_config(self) -> Dict[str, Any]:
        """
        Retorna configurações do SLURM
        
        Returns:
            Dicionário com configurações do SLURM
        """
        return self.get('slurm', {})
    
    def validate_config(self) -> bool:
        """
        Valida se as configurações essenciais estão presentes
        
        Returns:
            True se válido, False caso contrário
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
            self.logger.error(f"Configurações obrigatórias ausentes: {missing_keys}")
            return False
        
        return True
    
    def save_config(self, output_file: Optional[str] = None) -> None:
        """
        Salva a configuração atual em um arquivo YAML
        
        Args:
            output_file: Arquivo de saída (padrão: mesmo arquivo carregado)
        """
        output_path = Path(output_file) if output_file else self.config_file
        
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(self.config, f, default_flow_style=False, 
                     allow_unicode=True, indent=2)
        
        self.logger.info(f"Configuração salva em: {output_path}")
    
    def __str__(self) -> str:
        """Representação em string da configuração"""
        return f"ConfigLoader(config_file='{self.config_file}')"
    
    def __repr__(self) -> str:
        """Representação detalhada da configuração"""
        return f"ConfigLoader(config_file='{self.config_file}', keys={list(self.config.keys())})"
