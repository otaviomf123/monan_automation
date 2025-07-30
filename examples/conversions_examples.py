#!/usr/bin/env python3
"""
Exemplo de Uso da Conversão MPAS para Grade Regular
===================================================

Este script demonstra como usar o conversor de dados MPAS separadamente
"""

import sys
from pathlib import Path

# Adicionar src ao path
sys.path.append(str(Path(__file__).parent.parent))

from src.config_loader import ConfigLoader
from src.data_converter import MPASDataConverter
from src.utils import setup_logging


def example_single_file_conversion():
    """Exemplo de conversão de um único arquivo"""
    
    print("="*60)
    print("EXEMPLO: CONVERSÃO DE ARQUIVO ÚNICO")
    print("="*60)
    
    # Configurar logging
    setup_logging()
    
    # Carregar configuração
    config = ConfigLoader('config.yml')
    
    # Criar conversor
    converter = MPASDataConverter(config)
    
    # Arquivos de exemplo (ajuste os caminhos conforme necessário)
    input_file = Path("20250727/run/diag.2025-07-27_06.00.00.nc")
    output_file = Path("output/diag_regular_single.nc")
    static_file = Path(config.get('paths.static_file'))
    weights_dir = Path("interpolation_weights")
    
    # Verificar se arquivo existe
    if not input_file.exists():
        print(f" Arquivo não encontrado: {input_file}")
        print("Ajuste o caminho do arquivo no script")
        return
    
    # Converter
    success = converter.convert_single_file(
        input_file=input_file,
        output_file=output_file,
        static_file=static_file,
        weights_dir=weights_dir
    )
    
    if success:
        print(f" Conversão bem-sucedida!")
        print(f"Arquivo salvo: {output_file}")
    else:
        print(" Conversão falhou")


def example_batch_conversion():
    """Exemplo de conversão em lote"""
    
    print("="*60)
    print("EXEMPLO: CONVERSÃO EM LOTE")
    print("="*60)
    
    # Configurar logging
    setup_logging()
    
    # Carregar configuração
    config = ConfigLoader('config.yml')
    
    # Criar conversor
    converter = MPASDataConverter(config)
    
    # Diretórios
    run_dir = Path("20250727/run")
    static_file = Path(config.get('paths.static_file'))
    
    if not run_dir.exists():
        print(f" Diretório não encontrado: {run_dir}")
        print("Ajuste o caminho do diretório no script")
        return
    
    # Converter todos os arquivos
    success = converter.convert_all_diag_files(run_dir, static_file)
    
    if success:
        # Mostrar resumo
        summary = converter.get_conversion_summary(run_dir)
        print("\n RESUMO DA CONVERSÃO:")
        print(f"Arquivos convertidos: {summary['converted_files']}")
        print(f"Tamanho total: {summary['total_size_mb']:.1f} MB")
        print(f"Diretório de saída: {summary['output_dir']}")
    else:
        print(" Algumas conversões falharam")


def example_custom_grid():
    """Exemplo com grade personalizada"""
    
    print("="*60)
    print("EXEMPLO: GRADE PERSONALIZADA")
    print("="*60)
    
    # Configurar logging
    setup_logging()
    
    # Carregar configuração
    config = ConfigLoader('config.yml')
    
    # Modificar configurações de grade
    config.set('conversion.grid.lon_min', -85)
    config.set('conversion.grid.lon_max', -30)
    config.set('conversion.grid.lat_min', -35)
    config.set('conversion.grid.lat_max', 15)
    config.set('conversion.grid.resolution', 0.05)  # Resolução mais fina
    config.set('conversion.grid.max_dist_km', 25)
    
    # Criar conversor com nova configuração
    converter = MPASDataConverter(config)
    
    print("Configuração de grade personalizada:")
    print(f"Longitude: {converter.lon_min} a {converter.lon_max}")
    print(f"Latitude: {converter.lat_min} a {converter.lat_max}")
    print(f"Resolução: {converter.resolution}°")
    print(f"Distância máxima: {converter.max_dist_km} km")
    
    # Converter arquivo único com nova configuração
    input_file = Path("20250727/run/diag.2025-07-27_06.00.00.nc")
    output_file = Path("output/diag_custom_grid.nc")
    static_file = Path(config.get('paths.static_file'))
    weights_dir = Path("interpolation_weights_custom")
    
    if input_file.exists():
        success = converter.convert_single_file(
            input_file=input_file,
            output_file=output_file,
            static_file=static_file,
            weights_dir=weights_dir,
            force_recalc=True  # Recalcular pesos para nova grade
        )
        
        if success:
            print(f" Conversão com grade personalizada bem-sucedida!")
        else:
            print(" Conversão falhou")
    else:
        print(f" Arquivo não encontrado: {input_file}")


def example_analysis():
    """Exemplo de análise dos dados convertidos"""
    
    print("="*60)
    print("EXEMPLO: ANÁLISE DOS DADOS CONVERTIDOS")
    print("="*60)
    
    try:
        import xarray as xr
        import numpy as np
        
        # Abrir arquivo convertido
        output_file = Path("output/diag_regular_single.nc")
        
        if not output_file.exists():
            print(f" Execute primeiro a conversão: {output_file}")
            return
        
        # Carregar dados
        ds = xr.open_dataset(output_file)
        
        print(" INFORMAÇÕES DO DATASET:")
        print(f"Dimensões: {dict(ds.dims)}")
        print(f"Coordenadas: {list(ds.coords)}")
        print(f"Variáveis: {list(ds.data_vars)}")
        
        print("\n INFORMAÇÕES DA GRADE:")
        print(f"Longitude: {ds.lon.min().values:.2f} a {ds.lon.max().values:.2f}")
        print(f"Latitude: {ds.lat.min().values:.2f} a {ds.lat.max().values:.2f}")
        print(f"Resolução lon: {np.diff(ds.lon.values).mean():.3f}°")
        print(f"Resolução lat: {np.diff(ds.lat.values).mean():.3f}°")
        
        # Análise de uma variável (se existir)
        if len(ds.data_vars) > 0:
            var_name = list(ds.data_vars)[0]
            var_data = ds[var_name]
            
            print(f"\n ANÁLISE DA VARIÁVEL '{var_name}':")
            print(f"Shape: {var_data.shape}")
            print(f"Tipo: {var_data.dtype}")
            
            if hasattr(var_data, 'long_name'):
                print(f"Descrição: {var_data.long_name}")
            
            # Estatísticas básicas
            valid_data = var_data.where(~np.isnan(var_data))
            if valid_data.size > 0:
                print(f"Mínimo: {valid_data.min().values:.2f}")
                print(f"Máximo: {valid_data.max().values:.2f}")
                print(f"Média: {valid_data.mean().values:.2f}")
                print(f"Valores válidos: {(~np.isnan(var_data)).sum().values}")
                print(f"Valores NaN: {np.isnan(var_data).sum().values}")
        
        ds.close()
        
    except ImportError:
        print(" xarray e numpy são necessários para análise")
        print("Instale com: pip install xarray numpy")


def main():
    """Função principal com menu de exemplos"""
    
    print("  EXEMPLOS DE CONVERSÃO MPAS")
    print("="*60)
    print("Escolha um exemplo:")
    print("1. Conversão de arquivo único")
    print("2. Conversão em lote")
    print("3. Grade personalizada")
    print("4. Análise dos dados convertidos")
    print("5. Executar todos os exemplos")
    print("="*60)
    
    try:
        choice = input("Digite sua escolha (1-5): ").strip()
        
        if choice == '1':
            example_single_file_conversion()
        elif choice == '2':
            example_batch_conversion()
        elif choice == '3':
            example_custom_grid()
        elif choice == '4':
            example_analysis()
        elif choice == '5':
            example_single_file_conversion()
            print("\n" + "="*60 + "\n")
            example_batch_conversion()
            print("\n" + "="*60 + "\n")
            example_custom_grid()
            print("\n" + "="*60 + "\n")
            example_analysis()
        else:
            print(" Escolha inválida")
            
    except KeyboardInterrupt:
        print("\n\n Exemplo interrompido pelo usuário")
    except Exception as e:
        print(f"\n Erro durante execução: {e}")


if __name__ == "__main__":
    # Criar diretório de saída
    Path("output").mkdir(exist_ok=True)
    
    main()
