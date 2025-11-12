"""
MPAS Data Converter
===================

Module for converting MPAS unstructured grid data to regular lat-lon grid.
Generates NetCDF files compatible with CDO and GrADS.

Features:
- Automated 3D variable detection
- Nearest neighbor interpolation
- CF-compliant NetCDF output
- CDO and GrADS compatibility

Author: Otavio Feitosa
Date: 2025
"""

import logging
import numpy as np
import xarray as xr
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime

try:
    from sklearn.neighbors import BallTree
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logging.warning("scikit-learn not available. Install with: pip install scikit-learn")


class MPASDataConverter:
    """
    Converts MPAS unstructured mesh data to regular lat-lon grid.
    
    This converter handles atmospheric model output from MPAS, performing
    spatial interpolation from the native unstructured mesh to a regular
    latitude-longitude grid suitable for analysis and visualization.
    
    Attributes
    ----------
    config : ConfigLoader
        Configuration object containing conversion parameters
    logger : logging.Logger
        Logger instance for this module
    lat_resolution : float
        Latitude grid spacing in degrees
    lon_resolution : float
        Longitude grid spacing in degrees
    """
    
    def __init__(self, config):
        """
        Initialize the MPAS data converter.
        
        Parameters
        ----------
        config : ConfigLoader
            Configuration object containing conversion settings
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Get conversion parameters
        resolution = config.get('conversion.grid.resolution', 0.25)
        self.lat_resolution = resolution
        self.lon_resolution = resolution
        
        self.logger.info(f"MPAS Data Converter initialized")
        self.logger.info(f"Grid resolution: {self.lat_resolution} deg x {self.lon_resolution} deg")
        
        if not SKLEARN_AVAILABLE:
            self.logger.error("scikit-learn is required for interpolation")
            raise ImportError("Please install scikit-learn: pip install scikit-learn")
    
    def _create_regular_grid(self, lat_bounds: Tuple[float, float], 
                            lon_bounds: Tuple[float, float]) -> Tuple[np.ndarray, np.ndarray]:
        """
        Create regular lat-lon grid.
        
        Parameters
        ----------
        lat_bounds : tuple
            (min_lat, max_lat) in degrees
        lon_bounds : tuple
            (min_lon, max_lon) in degrees
            
        Returns
        -------
        lat_grid : np.ndarray
            2D array of latitudes
        lon_grid : np.ndarray
            2D array of longitudes
        """
        min_lat, max_lat = lat_bounds
        min_lon, max_lon = lon_bounds
        
        lat_points = np.arange(min_lat, max_lat + self.lat_resolution, self.lat_resolution)
        lon_points = np.arange(min_lon, max_lon + self.lon_resolution, self.lon_resolution)
        
        lon_grid, lat_grid = np.meshgrid(lon_points, lat_points)
        
        self.logger.info(f"Regular grid created: {lat_grid.shape[0]} x {lon_grid.shape[1]} points")
        
        return lat_grid, lon_grid
    
    def _build_interpolation_tree(self, lat_mpas: np.ndarray, 
                                  lon_mpas: np.ndarray) -> Tuple[BallTree, np.ndarray]:
        """
        Build KD-tree for nearest neighbor interpolation.
        
        Parameters
        ----------
        lat_mpas : np.ndarray
            MPAS cell latitudes in degrees
        lon_mpas : np.ndarray
            MPAS cell longitudes in degrees
            
        Returns
        -------
        tree : BallTree
            Spatial index for nearest neighbor search
        mpas_coords : np.ndarray
            MPAS coordinates in radians (n_points, 2)
        """
        lat_rad = np.radians(lat_mpas)
        lon_rad = np.radians(lon_mpas)
        
        mpas_coords = np.column_stack([lat_rad, lon_rad])
        
        tree = BallTree(mpas_coords, metric='haversine')
        
        self.logger.info(f"Interpolation tree built with {len(lat_mpas)} MPAS cells")
        
        return tree, mpas_coords
    
    def _build_interpolation_indices(self, tree: BallTree, 
                                      lat_grid: np.ndarray, 
                                      lon_grid: np.ndarray,
                                      max_dist_km: float = 30.0) -> Dict:
        """
        Calculate interpolation indices and weights once for reuse.
        Uses 3 nearest neighbors with inverse distance weighting.
        Points beyond max_dist_km are masked as NaN.
        
        Parameters
        ----------
        tree : BallTree
            Spatial index for MPAS cells
        lat_grid : np.ndarray
            Target grid latitudes (2D)
        lon_grid : np.ndarray
            Target grid longitudes (2D)
        max_dist_km : float
            Maximum distance in km for valid interpolation
            
        Returns
        -------
        interp_data : dict
            Dictionary with indices, distances, weights, and mask
        """
        lat_grid_rad = np.radians(lat_grid.ravel())
        lon_grid_rad = np.radians(lon_grid.ravel())
        
        grid_coords = np.column_stack([lat_grid_rad, lon_grid_rad])
        
        # Query for 3 nearest neighbors
        distances, indices = tree.query(grid_coords, k=3)
        
        # Convert distances to km (haversine returns radians, Earth radius = 6371 km)
        distances_km = distances * 6371.0
        
        # Calculate inverse distance weights
        weights = 1.0 / (distances_km**2 + 1e-10)
        weights = weights / np.sum(weights, axis=1, keepdims=True)
        
        # Create mask for points too far from any MPAS cell
        valid_mask = distances_km[:, 0] <= max_dist_km
        
        self.logger.info(f"Interpolation indices calculated for {len(indices)} grid points")
        self.logger.info(f"Valid points (within {max_dist_km} km): {np.sum(valid_mask)} / {len(valid_mask)}")
        
        return {
            'indices': indices,
            'distances_km': distances_km,
            'weights': weights,
            'valid_mask': valid_mask,
            'grid_shape': lat_grid.shape
        }
    
    def _interpolate_to_grid(self, interp_data: Dict,
                            data_mpas: np.ndarray) -> np.ndarray:
        """
        Interpolate MPAS data to regular grid using precomputed weights.
        Uses inverse distance weighting with 3 nearest neighbors.
        
        Parameters
        ----------
        interp_data : dict
            Precomputed interpolation data (indices, weights, mask)
        data_mpas : np.ndarray
            MPAS data values (nCells,) or (nCells, nLevels)
            
        Returns
        -------
        data_grid : np.ndarray
            Interpolated data on regular grid
        """
        indices = interp_data['indices']
        weights = interp_data['weights']
        valid_mask = interp_data['valid_mask']
        grid_shape = interp_data['grid_shape']
        
        if data_mpas.ndim == 1:
            # 2D field (no vertical levels)
            # Weighted average of 3 nearest neighbors
            data_interp = np.sum(data_mpas[indices] * weights, axis=1)
            
            # Apply distance mask - set invalid points to NaN
            data_interp = np.where(valid_mask, data_interp, np.nan)
            
            # Reshape to 2D grid
            data_grid = data_interp.reshape(grid_shape)
            
        else:
            # 3D field (with vertical levels)
            n_levels = data_mpas.shape[1]
            data_grid = np.zeros((grid_shape[0], grid_shape[1], n_levels))
            
            for lev in range(n_levels):
                # Weighted average for this level
                data_interp = np.sum(data_mpas[indices, lev] * weights, axis=1)
                
                # Apply distance mask
                data_interp = np.where(valid_mask, data_interp, np.nan)
                
                # Reshape and store
                data_grid[:, :, lev] = data_interp.reshape(grid_shape)
        
        return data_grid
    
    def _detect_3d_variables(self, ds: xr.Dataset) -> List[str]:
        """
        Detect 3D atmospheric variables in MPAS dataset.
        
        Parameters
        ----------
        ds : xr.Dataset
            MPAS dataset
            
        Returns
        -------
        vars_3d : list
            List of 3D variable names
        """
        vars_3d = []
        
        vertical_dims = [
            'nVertLevels',      # Model native vertical levels
            'nVertLevelsP1',    # Extended vertical levels
            'nSoilLevels',      # Soil levels
            't_iso_levels',     # Isobaric (pressure) levels
            'nIsoLevelsT'       # Alternative isobaric naming
        ]
        
        for var_name in ds.data_vars:
            var = ds[var_name]
            
            if 'nCells' not in var.dims:
                continue
            
            has_vertical = any(vdim in var.dims for vdim in vertical_dims)
            
            if has_vertical:
                vars_3d.append(var_name)
        
        return vars_3d
    
    def _detect_2d_variables(self, ds: xr.Dataset) -> List[str]:
        """
        Detect 2D spatial variables in MPAS dataset.
        
        Parameters
        ----------
        ds : xr.Dataset
            MPAS dataset
            
        Returns
        -------
        vars_2d : list
            List of 2D variable names
        """
        vars_2d = []
        
        vertical_dims = [
            'nVertLevels', 'nVertLevelsP1', 'nSoilLevels', 
            't_iso_levels', 'nIsoLevelsT'
        ]
        
        # Skip non-spatial variables
        skip_vars = ['xtime', 'initial_time']
        
        for var_name in ds.data_vars:
            if var_name in skip_vars:
                continue
            
            var = ds[var_name]
            
            # Must have nCells (spatial) and Time dimensions
            if 'nCells' not in var.dims:
                continue
            
            # Must NOT have vertical dimension
            has_vertical = any(vdim in var.dims for vdim in vertical_dims)
            
            if not has_vertical:
                vars_2d.append(var_name)
        
        return vars_2d
    
    def _get_variable_attributes(self, var: xr.DataArray) -> Dict[str, str]:
        """
        Extract and sanitize variable attributes for CF compliance.
        
        Parameters
        ----------
        var : xr.DataArray
            Source variable
            
        Returns
        -------
        attrs : dict
            CF-compliant attributes
        """
        attrs = {}
        
        if hasattr(var, 'long_name'):
            attrs['long_name'] = str(var.long_name)
        else:
            attrs['long_name'] = var.name
        
        if hasattr(var, 'units'):
            attrs['units'] = str(var.units)
        else:
            attrs['units'] = '1'
        
        if hasattr(var, 'description'):
            attrs['description'] = str(var.description)
        
        return attrs
    
    def _create_cf_compliant_dataset(self, 
                                     data_dict: Dict[str, np.ndarray],
                                     lat_grid: np.ndarray,
                                     lon_grid: np.ndarray,
                                     times: np.ndarray,
                                     levels: Optional[np.ndarray],
                                     attrs_dict: Dict[str, Dict]) -> xr.Dataset:
        """
        Create CF-compliant NetCDF dataset for CDO and GrADS.
        
        Parameters
        ----------
        data_dict : dict
            Dictionary mapping variable names to data arrays
        lat_grid : np.ndarray
            Latitude coordinates (1D)
        lon_grid : np.ndarray
            Longitude coordinates (1D)
        times : np.ndarray
            Time coordinates
        levels : np.ndarray, optional
            Vertical level coordinates
        attrs_dict : dict
            Variable attributes
            
        Returns
        -------
        ds : xr.Dataset
            CF-compliant dataset
        """
        coords = {
            'time': times,
            'lat': lat_grid[:, 0],
            'lon': lon_grid[0, :]
        }
        
        if levels is not None:
            coords['level'] = levels
        
        data_vars = {}
        
        for var_name, data in data_dict.items():
            attrs = attrs_dict.get(var_name, {})
            
            if data.ndim == 3:
                dims = ('time', 'lat', 'lon')
            elif data.ndim == 4:
                dims = ('time', 'level', 'lat', 'lon')
            else:
                continue
            
            data_vars[var_name] = (dims, data, attrs)
        
        ds = xr.Dataset(data_vars, coords=coords)
        
        # Add coordinate attributes for CF compliance
        ds['time'].attrs = {
            'long_name': 'time',
            'standard_name': 'time',
            'axis': 'T'
        }
        
        ds['lat'].attrs = {
            'long_name': 'latitude',
            'standard_name': 'latitude',
            'units': 'degrees_north',
            'axis': 'Y'
        }
        
        ds['lon'].attrs = {
            'long_name': 'longitude',
            'standard_name': 'longitude',
            'units': 'degrees_east',
            'axis': 'X'
        }
        
        if levels is not None:
            # Determine if these are pressure levels or model levels
            is_pressure = levels[0] > 10  # Heuristic: pressure levels are > 10
            
            if is_pressure:
                ds['level'].attrs = {
                    'long_name': 'pressure',
                    'standard_name': 'air_pressure',
                    'units': 'hPa',
                    'axis': 'Z',
                    'positive': 'down'
                }
            else:
                ds['level'].attrs = {
                    'long_name': 'model_level',
                    'units': '1',
                    'axis': 'Z',
                    'positive': 'down'
                }
        
        # Global attributes
        ds.attrs = {
            'title': 'MPAS model output on regular grid',
            'institution': 'CEMPA/INPE',
            'source': 'MONAN/MPAS atmospheric model',
            'history': f'Created on {datetime.now().isoformat()}',
            'Conventions': 'CF-1.8',
            'grid_type': 'regular_lat_lon',
            'grid_resolution_lat': f'{self.lat_resolution} degrees',
            'grid_resolution_lon': f'{self.lon_resolution} degrees'
        }
        
        return ds
    
    def _save_netcdf(self, ds: xr.Dataset, output_file: Path) -> bool:
        """
        Save dataset to NetCDF file with CDO/GrADS compatibility.
        
        Parameters
        ----------
        ds : xr.Dataset
            Dataset to save
        output_file : Path
            Output file path
            
        Returns
        -------
        success : bool
            True if successful
        """
        try:
            encoding = {}
            
            for var in ds.data_vars:
                encoding[var] = {
                    'zlib': True,
                    'complevel': 4,
                    'shuffle': True,
                    'dtype': 'float32',
                    '_FillValue': -999.0
                }
            
            # Coordinate encoding
            for coord in ds.coords:
                if coord == 'time':
                    encoding[coord] = {
                        'dtype': 'float64',
                        'units': 'hours since 1970-01-01 00:00:00',
                        'calendar': 'proleptic_gregorian'
                    }
                else:
                    encoding[coord] = {'dtype': 'float32'}
            
            ds.to_netcdf(
                output_file,
                format='NETCDF4_CLASSIC',
                encoding=encoding,
                unlimited_dims=['time']
            )
            
            file_size_mb = output_file.stat().st_size / (1024 * 1024)
            self.logger.info(f"NetCDF file saved: {output_file.name} ({file_size_mb:.1f} MB)")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save NetCDF file: {e}")
            return False
    
    def convert_diag_file(self, diag_file: Path, static_file: Path, 
                         output_file: Optional[Path] = None) -> bool:
        """
        Convert single MPAS diagnostic file to regular grid.
        
        Parameters
        ----------
        diag_file : Path
            MPAS diagnostic file
        static_file : Path
            MPAS static file with grid coordinates
        output_file : Path, optional
            Output file path (auto-generated if None)
            
        Returns
        -------
        success : bool
            True if conversion successful
        """
        self.logger.info(f"Converting: {diag_file.name}")
        
        try:
            ds_diag = xr.open_dataset(diag_file)
            ds_static = xr.open_dataset(static_file)
            
            lat_mpas = np.degrees(ds_static['latCell'].values)
            lon_mpas = np.degrees(ds_static['lonCell'].values)
            
            # Get grid bounds from configuration
            lat_bounds = (
                self.config.get('conversion.grid.lat_min', lat_mpas.min()),
                self.config.get('conversion.grid.lat_max', lat_mpas.max())
            )
            lon_bounds = (
                self.config.get('conversion.grid.lon_min', lon_mpas.min()),
                self.config.get('conversion.grid.lon_max', lon_mpas.max())
            )
            
            self.logger.info(f"Grid bounds: lat({lat_bounds[0]:.2f}, {lat_bounds[1]:.2f}), lon({lon_bounds[0]:.2f}, {lon_bounds[1]:.2f})")
            
            lat_grid, lon_grid = self._create_regular_grid(lat_bounds, lon_bounds)
            
            tree, mpas_coords = self._build_interpolation_tree(lat_mpas, lon_mpas)
            
            # Calculate interpolation weights ONCE for all variables and timesteps
            # Uses 3 nearest neighbors with inverse distance weighting
            max_dist_km = self.config.get('conversion.grid.max_dist_km', 30.0)
            interp_data = self._build_interpolation_indices(tree, lat_grid, lon_grid, max_dist_km)
            self.logger.info(f"Interpolation weights computed (max dist: {max_dist_km} km)")
            
            # Detect both 3D and 2D variables
            vars_3d = self._detect_3d_variables(ds_diag)
            vars_2d = self._detect_2d_variables(ds_diag)
            
            total_vars = len(vars_3d) + len(vars_2d)
            
            if total_vars == 0:
                self.logger.warning(f"No spatial variables found in {diag_file.name}")
                return False
            
            if vars_3d:
                self.logger.info(f"Processing {len(vars_3d)} 3D variables: {vars_3d}")
            if vars_2d:
                self.logger.info(f"Processing {len(vars_2d)} 2D variables: {vars_2d}")
            
            # Handle time coordinate - convert xtime bytes to datetime
            if 'xtime' in ds_diag:
                xtime_raw = ds_diag['xtime'].values
                if xtime_raw.dtype.kind == 'S':  # Byte string
                    # Convert MPAS format "2025-10-20_09:00:00" to ISO format "2025-10-20T09:00:00"
                    times = np.array([np.datetime64(xtime_raw[i].decode().strip().replace('_', 'T')) 
                                     for i in range(len(xtime_raw))])
                else:
                    times = xtime_raw
            elif 'Time' in ds_diag.coords:
                times = ds_diag['Time'].values
            else:
                # Create dummy time coordinate
                times = np.array([np.datetime64('2000-01-01T00:00:00')])
            
            n_times = len(times)
            
            data_dict = {}
            attrs_dict = {}
            levels = None
            
            # Process 3D variables
            for var_name in vars_3d:
                var = ds_diag[var_name]
                
                vert_dim = None
                for vd in ['nVertLevels', 'nVertLevelsP1', 'nSoilLevels', 't_iso_levels', 'nIsoLevelsT']:
                    if vd in var.dims:
                        vert_dim = vd
                        break
                
                if vert_dim:
                    n_levels = var.sizes[vert_dim]
                    
                    # Extract real vertical coordinate values from MPAS
                    if levels is None:
                        if vert_dim in ds_diag.coords:
                            levels = ds_diag[vert_dim].values
                        elif vert_dim in ds_diag.dims:
                            levels = np.arange(1, n_levels + 1, dtype=np.float32)
                        else:
                            levels = np.arange(1, n_levels + 1, dtype=np.float32)
                    
                    data_grid = np.zeros((n_times, n_levels, lat_grid.shape[0], lat_grid.shape[1]))
                    
                    for t in range(n_times):
                        data_time = var.isel(Time=t).values
                        data_interp = self._interpolate_to_grid(interp_data, data_time)
                        for lev in range(n_levels):
                            data_grid[t, lev, :, :] = data_interp[:, :, lev]
                else:
                    data_grid = np.zeros((n_times, lat_grid.shape[0], lat_grid.shape[1]))
                    
                    for t in range(n_times):
                        data_time = var.isel(Time=t).values
                        data_interp = self._interpolate_to_grid(interp_data, data_time)
                        data_grid[t, :, :] = data_interp
                
                data_dict[var_name] = data_grid
                attrs_dict[var_name] = self._get_variable_attributes(var)
                
                self.logger.info(f"  {var_name}: {data_grid.shape}")
            
            # Process 2D variables
            for var_name in vars_2d:
                var = ds_diag[var_name]
                
                data_grid = np.zeros((n_times, lat_grid.shape[0], lat_grid.shape[1]))
                
                for t in range(n_times):
                    data_time = var.isel(Time=t).values
                    data_interp = self._interpolate_to_grid(interp_data, data_time)
                    data_grid[t, :, :] = data_interp
                
                data_dict[var_name] = data_grid
                attrs_dict[var_name] = self._get_variable_attributes(var)
                
                self.logger.info(f"  {var_name}: {data_grid.shape}")
            
            ds_output = self._create_cf_compliant_dataset(
                data_dict, lat_grid, lon_grid, times, levels, attrs_dict
            )
            
            if output_file is None:
                output_file = diag_file.parent / f"regular_grid_{diag_file.name}"
            
            success = self._save_netcdf(ds_output, output_file)
            
            ds_diag.close()
            ds_static.close()
            ds_output.close()
            
            if success:
                self.logger.info(f"SUCCESS: Converted {diag_file.name}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error converting {diag_file.name}: {e}")
            self.logger.exception("Details:")
            return False
    
    def convert_all_diag_files(self, run_dir: Path, static_file: Path) -> bool:
        """
        Convert all diagnostic files in run directory.
        
        Parameters
        ----------
        run_dir : Path
            Directory containing MPAS diagnostic files
        static_file : Path
            MPAS static file
            
        Returns
        -------
        success : bool
            True if all conversions successful
        """
        self.logger.info("="*60)
        self.logger.info("CONVERTING MPAS DATA TO REGULAR GRID")
        self.logger.info("="*60)
        
        diag_files = sorted(run_dir.glob("diag.*.nc"))
        
        if not diag_files:
            self.logger.error(f"No diagnostic files found in {run_dir}")
            return False
        
        self.logger.info(f"Found {len(diag_files)} diagnostic files")
        
        output_dir = run_dir / "regular_grid"
        output_dir.mkdir(exist_ok=True)
        
        success_count = 0
        failed_files = []
        
        for i, diag_file in enumerate(diag_files, 1):
            self.logger.info(f"[{i}/{len(diag_files)}] Processing {diag_file.name}")
            
            output_file = output_dir / f"regular_{diag_file.name}"
            
            if self.convert_diag_file(diag_file, static_file, output_file):
                success_count += 1
            else:
                failed_files.append(diag_file.name)
        
        self.logger.info("="*60)
        self.logger.info("CONVERSION SUMMARY")
        self.logger.info("="*60)
        self.logger.info(f"Total files: {len(diag_files)}")
        self.logger.info(f"Successful: {success_count}")
        self.logger.info(f"Failed: {len(failed_files)}")
        
        if failed_files:
            self.logger.warning("Failed files:")
            for fname in failed_files:
                self.logger.warning(f"  - {fname}")
        
        if success_count == len(diag_files):
            self.logger.info("SUCCESS: All files converted successfully")
            return True
        else:
            self.logger.error("FAILED: Some conversions failed")
            return False
