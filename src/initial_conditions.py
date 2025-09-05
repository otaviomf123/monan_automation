"""
MPAS Initial Conditions Generator
================================

Module for generating initial atmospheric conditions for MPAS model using
WPS-preprocessed meteorological data. This module handles the conversion
from WPS intermediate format (FILE:*) to MPAS-compatible NetCDF files.

MPAS Initial Conditions Workflow:
1. Links WPS FILE:* output files to working directory
2. Creates namelist.init_atmosphere with model configuration
3. Creates streams.init_atmosphere with I/O specifications
4. Runs init_atmosphere_model to interpolate data to MPAS grid
5. Produces *.init.nc file for model initialization

Key Namelist Parameters:
- config_init_case: 7 for real-data cases
- config_nvertlevels: Number of vertical levels (typically 55)
- config_met_prefix: Input data prefix ('FILE')
- config_geog_data_path: Path to static geographic data

Output Files:
- {grid_name}.init.nc: Initial conditions on MPAS grid
- Contains fields: u, v, w, temperature, pressure, moisture, etc.

Author: Otavio Feitosa
Date: 2025
"""

import logging
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any

from .config_loader import ConfigLoader
from .utils import create_symbolic_link, run_command, write_namelist, write_streams_file


class InitialConditionsGenerator:
    """
    Generates initial atmospheric conditions for MPAS model runs.
    
    This class orchestrates the initialization process that converts WPS
    intermediate format files to MPAS-compatible NetCDF initial conditions.
    The process involves configuring namelists, streams, and running the
    init_atmosphere_model executable.
    
    Attributes
    ----------
    config : ConfigLoader
        Configuration object containing model parameters
    logger : logging.Logger
        Logger instance for this module
    paths : Dict[str, str]
        Dictionary of file/directory paths from configuration
    dates : Dict[str, str]
        Dictionary of date/time settings
    physics : Dict[str, Any]
        Dictionary of physics configuration parameters
    
    Examples
    --------
    >>> from src.config_loader import ConfigLoader
    >>> config = ConfigLoader('config.yml')
    >>> generator = InitialConditionsGenerator(config)
    >>> success = generator.generate(init_dir, gfs_dir)
    """
    
    def __init__(self, config: ConfigLoader) -> None:
        """
        Initialize the initial conditions generator.
        
        Parameters
        ----------
        config : ConfigLoader
            Configuration object containing:
            - paths.mpas_init_exe: Path to init_atmosphere_model executable
            - paths.static_file: Path to MPAS static file (*.static.nc)
            - paths.geog_data_path: Path to geographic static data
            - paths.decomp_file_prefix: Domain decomposition file prefix
            - paths.init_filename: Output initial conditions filename
            - dates: Model start/end times
            - physics: Vertical levels and physics options
            
        Raises
        ------
        KeyError
            If required configuration parameters are missing
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Extract configuration sections
        self.paths = config.get_paths()
        self.dates = config.get_dates()
        self.physics = config.get_physics_config()
        
        self.logger.info("[INFO] Initial Conditions Generator initialized")
        
        # Log key configuration
        init_filename = self.paths.get('init_filename', 'brasil_circle.init.nc')
        self.logger.info(f"[INFO] Output filename: {init_filename}")
        self.logger.info(f"[INFO] Vertical levels: {self.physics.get('nvertlevels', 'Not configured')}")
    
    def _create_file_links(self, init_dir: Path, gfs_dir: Path) -> bool:
        """
        Create symbolic links to WPS FILE outputs in initialization directory.
        
        The init_atmosphere_model expects FILE:YYYY-MM-DD_HH files to be present
        in the working directory. This method creates symbolic links to avoid
        copying large files.
        
        Parameters
        ----------
        init_dir : Path
            Target directory for initial conditions generation
        gfs_dir : Path
            Source directory containing WPS FILE:* outputs
            
        Returns
        -------
        bool
            True if all required FILE links created successfully
            
        Notes
        -----
        - Searches for FILE:YYYY-* pattern based on run_date
        - Creates symbolic links to preserve disk space
        - Verifies all expected time steps are present
        """
        self.logger.info("[INFO] Creating symbolic links to WPS FILE outputs...")
        
        # Generate search pattern based on run date
        run_year = self.dates['run_date'][:4]
        file_pattern = f"FILE:{run_year}-*"
        
        self.logger.debug(f"[DEBUG] Searching pattern: {file_pattern}")
        self.logger.debug(f"[DEBUG] In directory: {gfs_dir}")
        
        # Find FILE outputs from WPS
        file_list = list(gfs_dir.glob(file_pattern))
        
        if not file_list:
            self.logger.error(f"FAILED: No WPS FILE outputs found with pattern: {file_pattern}")
            self.logger.error("[DEBUG] Check that WPS processing completed successfully")
            return False
        
        # Sort files chronologically
        file_list.sort()
        
        self.logger.info(f"[INFO] Found {len(file_list)} WPS FILE outputs")
        self.logger.debug(f"[DEBUG] First file: {file_list[0].name}")
        self.logger.debug(f"[DEBUG] Last file: {file_list[-1].name}")
        
        # Create symbolic links in initialization directory
        success_count = 0
        failed_files = []
        
        for file_path in file_list:
            target_path = init_dir / file_path.name
            
            # Remove existing link if present
            if target_path.exists() or target_path.is_symlink():
                target_path.unlink()
            
            if create_symbolic_link(file_path, target_path):
                success_count += 1
                self.logger.debug(f"[DEBUG] Linked: {file_path.name}")
            else:
                failed_files.append(file_path.name)
                self.logger.error(f"FAILED: Could not link: {file_path.name}")
        
        if failed_files:
            self.logger.error(f"FAILED: Could not link {len(failed_files)} files")
            return False
        
        self.logger.info(f"SUCCESS: Created {success_count} FILE links")
        return True
    
    def _generate_init_namelist(self) -> Dict[str, Dict[str, Any]]:
        """
        Generate namelist configuration for init_atmosphere_model.
        
        The namelist controls how meteorological data is interpolated onto
        the MPAS grid, including vertical grid generation, data sources,
        and interpolation methods.
        
        Returns
        -------
        Dict[str, Dict[str, Any]]
            Nested dictionary containing namelist sections and parameters
            
        Notes
        -----
        Key namelist sections:
        - nhyd_model: Core model settings and time configuration
        - dimensions: Vertical grid dimensions
        - data_sources: Input data paths and formats
        - vertical_grid: Vertical coordinate generation
        - interpolation_control: Interpolation method settings
        - preproc_stages: Processing stage control flags
        
        Important parameters:
        - config_init_case: 7 for real-data initialization
        - config_nvertlevels: Number of model vertical levels
        - config_met_prefix: 'FILE' for WPS intermediate format
        - config_geog_data_path: Path to static geographic datasets
        """
        namelist_config = {
            # Core model configuration
            'nhyd_model': {
                'config_init_case': 7,  # Real-data case initialization
                'config_start_time': self.dates['start_time'],
                'config_stop_time': self.dates['end_time'],
                'config_theta_adv_order': 3,  # Third-order advection scheme
                'config_coef_3rd_order': 0.25  # Coefficient for 3rd-order scheme
            },
            
            # Grid dimensions
            'dimensions': {
                'config_nvertlevels': self.physics['nvertlevels'],      # Model vertical levels
                'config_nsoillevels': self.physics['nsoillevels'],     # Soil levels
                'config_nfglevels': self.physics['nfglevels'],         # First-guess levels
                'config_nfgsoillevels': 4                              # First-guess soil levels
            },
            
            # Data source configuration
            'data_sources': {
                'config_geog_data_path': self.paths['geog_data_path'],
                'config_met_prefix': 'FILE',  # WPS intermediate file prefix
                'config_sfc_prefix': 'SST',   # Sea surface temperature prefix
                'config_fg_interval': 86400,  # First-guess interval (seconds)
                'config_landuse_data': 'MODIFIED_IGBP_MODIS_NOAH',
                'config_topo_data': 'GMTED2010',
                'config_vegfrac_data': 'MODIS',
                'config_albedo_data': 'MODIS',
                'config_maxsnowalbedo_data': 'MODIS',
                'config_supersample_factor': 3,  # Terrain supersampling
                'config_use_spechumd': False     # Use specific humidity
            },
            
            # Vertical grid generation
            'vertical_grid': {
                'config_ztop': 30000.0,           # Model top height (m)
                'config_nsmterrain': 1,           # Terrain smoothing passes
                'config_smooth_surfaces': True,   # Smooth surface fields
                'config_dzmin': 0.3,             # Minimum layer thickness
                'config_nsm': 30,                # Smoothing iterations
                'config_tc_vertical_grid': True,  # Use terrain-following grid
                'config_blend_bdy_terrain': False # Blend boundary terrain
            },
            
            # Interpolation controls
            'interpolation_control': {
                'config_extrap_airtemp': 'linear'  # Temperature extrapolation method
            },
            
            # Processing stage flags
            'preproc_stages': {
                'config_static_interp': False,    # Skip static field interpolation
                'config_native_gwd_static': False,# Skip native gravity wave drag
                'config_vertical_grid': True,     # Generate vertical grid
                'config_met_interp': True,        # Interpolate meteorological data
                'config_input_sst': False,        # Skip SST input
                'config_frac_seaice': True        # Process fractional sea ice
            },
            
            # I/O configuration
            'io': {
                'config_pio_num_iotasks': 0,      # Parallel I/O tasks (0=auto)
                'config_pio_stride': 1            # I/O task stride
            },
            
            # Domain decomposition
            'decomposition': {
                'config_block_decomp_file_prefix': self.paths['decomp_file_prefix']
            },
            
            # Limited area model settings
            'limited_area': {
                'config_apply_lbcs': True         # Apply lateral boundary conditions
            }
        }
        
        self.logger.info("[INFO] Generated init_atmosphere namelist configuration")
        self.logger.info(f"[INFO] Vertical levels: {namelist_config['dimensions']['config_nvertlevels']}")
        self.logger.info(f"[INFO] Model top: {namelist_config['vertical_grid']['config_ztop']} m")
        
        return namelist_config
    
    def _generate_init_streams(self) -> str:
        """
        Generate streams configuration for init_atmosphere_model I/O.
        
        The streams file defines input and output data sources and formats
        for the initialization process.
        
        Returns
        -------
        str
            XML content for streams.init_atmosphere file
            
        Notes
        -----
        Defines two streams:
        - input: MPAS static file (*.static.nc) containing grid definition
        - output: Initial conditions file (*.init.nc) to be created
        
        Stream attributes:
        - type: 'input' or 'output'
        - precision: 'single' for smaller file sizes
        - io_type: 'pnetcdf,cdf5' for parallel NetCDF with CDF-5 format
        - filename_template: Template for file naming
        - packages: Data packages to include ('initial_conds')
        """
        # Get configured initial conditions filename
        init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
        static_file = self.paths['static_file']
        
        streams_xml = f'''<streams>
<immutable_stream name="input"
                 type="input"
                 precision="single"
                 io_type="pnetcdf,cdf5"
                 filename_template="{static_file}"
                 input_interval="initial_only" />
<immutable_stream name="output"
                 type="output"
                 filename_template="{init_filename}"
                 io_type="pnetcdf,cdf5"
                 packages="initial_conds"
                 output_interval="initial_only" />
</streams>'''
        
        self.logger.info("[INFO] Generated init_atmosphere streams configuration")
        self.logger.info(f"[INFO] Input static file: {static_file}")
        self.logger.info(f"[INFO] Output init file: {init_filename}")
        
        return streams_xml
    
    def _verify_static_file(self, init_dir: Path) -> bool:
        """
        Verify that MPAS static file exists and is accessible.
        
        The static file contains the MPAS grid definition and is required
        for initialization. This method checks file existence and basic
        properties.
        
        Parameters
        ----------
        init_dir : Path
            Working directory for initialization
            
        Returns
        -------
        bool
            True if static file is valid and accessible
            
        Notes
        -----
        The static file typically contains:
        - Grid cell centers and vertices
        - Topography and land use data
        - Coriolis parameter
        - Grid metrics and coefficients
        """
        static_file = Path(self.paths['static_file'])
        
        if not static_file.exists():
            self.logger.error(f"FAILED: MPAS static file not found: {static_file}")
            return False
        
        # Check file size (should be substantial)
        file_size_mb = static_file.stat().st_size / (1024 * 1024)
        
        if file_size_mb < 1.0:
            self.logger.error(f"FAILED: Static file suspiciously small: {file_size_mb:.1f} MB")
            return False
        
        self.logger.info(f"SUCCESS: MPAS static file verified: {static_file}")
        self.logger.info(f"[INFO] Static file size: {file_size_mb:.1f} MB")
        
        return True
    
    def _link_executable(self, init_dir: Path) -> bool:
        """
        Create symbolic link to init_atmosphere_model executable.
        
        Parameters
        ----------
        init_dir : Path
            Working directory where executable link will be created
            
        Returns
        -------
        bool
            True if executable link created successfully
            
        Notes
        -----
        The init_atmosphere_model executable must be available in the
        working directory. A symbolic link is used to avoid copying
        the executable file.
        """
        exe_source = Path(self.paths['mpas_init_exe'])
        exe_target = init_dir / 'init_atmosphere_model'
        
        if not exe_source.exists():
            self.logger.error(f"FAILED: init_atmosphere_model executable not found: {exe_source}")
            return False
        
        # Remove existing link if present
        if exe_target.exists() or exe_target.is_symlink():
            exe_target.unlink()
        
        if create_symbolic_link(exe_source, exe_target):
            self.logger.info(f"SUCCESS: Created executable link: {exe_target.name}")
            return True
        else:
            self.logger.error(f"FAILED: Could not create executable link: {exe_target}")
            return False
    
    def _run_init_atmosphere(self, init_dir: Path) -> bool:
        """
        Execute init_atmosphere_model to generate initial conditions.
        
        This method runs the MPAS initialization executable that interpolates
        meteorological data from WPS format onto the MPAS grid structure.
        
        Parameters
        ----------
        init_dir : Path
            Working directory containing all required input files
            
        Returns
        -------
        bool
            True if initialization completed successfully
            
        Notes
        -----
        Required input files in working directory:
        - init_atmosphere_model (executable)
        - namelist.init_atmosphere (configuration)
        - streams.init_atmosphere (I/O specification)
        - FILE:YYYY-MM-DD_HH files (meteorological data)
        - Static file referenced in streams
        
        Typical execution time: 5-30 minutes depending on:
        - Grid resolution
        - Number of vertical levels
        - Forecast period length
        - Available computational resources
        """
        self.logger.info("[INFO] Executing init_atmosphere_model...")
        
        # Verify prerequisites exist
        prerequisites = [
            init_dir / 'init_atmosphere_model',
            init_dir / 'namelist.init_atmosphere', 
            init_dir / 'streams.init_atmosphere'
        ]
        
        for prereq in prerequisites:
            if not prereq.exists():
                self.logger.error(f"FAILED: Required file missing: {prereq}")
                return False
        
        # Check for FILE inputs
        file_inputs = list(init_dir.glob("FILE:*"))
        if not file_inputs:
            self.logger.error("FAILED: No WPS FILE inputs found")
            return False
        
        self.logger.info(f"[INFO] Processing {len(file_inputs)} time steps")
        
        try:
            # Execute init_atmosphere_model
            self.logger.info("[INFO] Starting initialization (this may take 5-30 minutes)...")
            
            result = subprocess.run(
                ["./init_atmosphere_model"],
                cwd=init_dir,
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            if result.returncode != 0:
                self.logger.error(f"FAILED: init_atmosphere_model returned error code {result.returncode}")
                self.logger.error(f"[DEBUG] stderr: {result.stderr}")
                
                # Check for common error patterns
                if "STOP" in result.stderr:
                    self.logger.error("[DEBUG] Model encountered fatal error")
                if "ERROR" in result.stderr:
                    self.logger.error("[DEBUG] Check input data and configuration")
                    
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("FAILED: init_atmosphere_model timed out after 1 hour")
            return False
        except Exception as e:
            self.logger.error(f"FAILED: Error executing init_atmosphere_model: {e}")
            return False
        
        # Verify output file was created
        init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
        output_file = init_dir / init_filename
        
        if not output_file.exists():
            self.logger.error("FAILED: Initial conditions file was not created")
            return False
        
        # Check output file size
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        
        if file_size_mb < 10.0:  # Minimum reasonable size
            self.logger.error(f"FAILED: Output file suspiciously small: {file_size_mb:.1f} MB")
            return False
        
        self.logger.info(f"SUCCESS: Initial conditions generated: {output_file.name}")
        self.logger.info(f"[INFO] Output file size: {file_size_mb:.1f} MB")
        
        return True
    
    def generate(self, init_dir: Path, gfs_dir: Path) -> bool:
        """
        Execute complete initial conditions generation workflow.
        
        This method orchestrates the full initialization process:
        1. Create links to WPS FILE outputs
        2. Verify MPAS static file availability
        3. Generate namelist.init_atmosphere configuration
        4. Generate streams.init_atmosphere I/O specification
        5. Link init_atmosphere_model executable
        6. Execute initialization process
        7. Verify output file creation
        
        Parameters
        ----------
        init_dir : Path
            Target directory for initialization workflow
        gfs_dir : Path
            Source directory containing WPS FILE:* outputs
            
        Returns
        -------
        bool
            True if complete workflow succeeded, False otherwise
            
        Notes
        -----
        - Creates all necessary configuration files
        - Handles symbolic linking to minimize disk usage
        - Provides detailed progress logging
        - Performs validation at each step
        - Total execution time typically 10-45 minutes
        """
        self.logger.info("=" * 60)
        self.logger.info("STARTING INITIAL CONDITIONS GENERATION")
        self.logger.info("=" * 60)
        self.logger.info(f"[INFO] Working directory: {init_dir}")
        self.logger.info(f"[INFO] WPS data source: {gfs_dir}")
        
        # Ensure working directory exists
        init_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Step 1: Create WPS FILE links
            self.logger.info("[INFO] Step 1/6: Creating WPS FILE links...")
            if not self._create_file_links(init_dir, gfs_dir):
                self.logger.error("FAILED: Could not create WPS FILE links")
                return False
            
            # Step 2: Verify static file
            self.logger.info("[INFO] Step 2/6: Verifying MPAS static file...")
            if not self._verify_static_file(init_dir):
                self.logger.error("FAILED: MPAS static file verification failed")
                return False
            
            # Step 3: Generate namelist
            self.logger.info("[INFO] Step 3/6: Generating initialization namelist...")
            namelist_data = self._generate_init_namelist()
            namelist_path = init_dir / 'namelist.init_atmosphere'
            
            if not write_namelist(namelist_path, namelist_data):
                self.logger.error("FAILED: Could not write namelist.init_atmosphere")
                return False
                
            self.logger.info(f"SUCCESS: Namelist created: {namelist_path}")
            
            # Step 4: Generate streams
            self.logger.info("[INFO] Step 4/6: Generating streams configuration...")
            streams_content = self._generate_init_streams()
            streams_path = init_dir / 'streams.init_atmosphere'
            
            if not write_streams_file(streams_path, streams_content):
                self.logger.error("FAILED: Could not write streams.init_atmosphere")
                return False
                
            self.logger.info(f"SUCCESS: Streams file created: {streams_path}")
            
            # Step 5: Link executable
            self.logger.info("[INFO] Step 5/6: Linking init_atmosphere_model...")
            if not self._link_executable(init_dir):
                self.logger.error("FAILED: Could not link executable")
                return False
            
            # Step 6: Run initialization
            self.logger.info("[INFO] Step 6/6: Running initialization process...")
            if not self._run_init_atmosphere(init_dir):
                self.logger.error("FAILED: Initialization process failed")
                return False
            
            # Final verification
            if not self.verify_output(init_dir):
                self.logger.error("FAILED: Output verification failed")
                return False
            
            self.logger.info("=" * 60)
            self.logger.info("SUCCESS: INITIAL CONDITIONS GENERATION COMPLETED")
            self.logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"FAILED: Unexpected error during initialization: {e}")
            self.logger.exception("[DEBUG] Full traceback:")
            return False
    
    def verify_output(self, init_dir: Path) -> bool:
        """
        Verify that initial conditions were generated successfully.
        
        Parameters
        ----------
        init_dir : Path
            Directory containing initialization outputs
            
        Returns
        -------
        bool
            True if output file is valid, False otherwise
            
        Notes
        -----
        Verification checks:
        - Output file existence
        - Minimum file size (>10 MB)
        - File accessibility
        """
        init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
        output_file = init_dir / init_filename
        
        if not output_file.exists():
            self.logger.error("FAILED: Initial conditions file not found")
            return False
        
        # Check minimum file size
        min_size_mb = 10
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        
        if file_size_mb < min_size_mb:
            self.logger.error(f"FAILED: Output file too small: {file_size_mb:.1f} MB (minimum {min_size_mb} MB)")
            return False
        
        self.logger.info(f"SUCCESS: Initial conditions verified: {file_size_mb:.1f} MB")
        return True
    
    def cleanup_temp_files(self, init_dir: Path, keep_logs: bool = False) -> None:
        """
        Clean up temporary files from initialization process.
        
        Parameters
        ----------
        init_dir : Path
            Directory containing temporary files
        keep_logs : bool, optional
            If True, preserve log files for debugging (default: False)
            
        Notes
        -----
        Removes temporary files:
        - log.init_atmosphere.* (if keep_logs=False)
        - namelist.init_atmosphere
        - streams.init_atmosphere
        - WPS FILE:* symbolic links
        """
        self.logger.info("[INFO] Cleaning up initialization temporary files...")
        
        # Define cleanup patterns
        cleanup_patterns = [
            "namelist.init_atmosphere",
            "streams.init_atmosphere",
            "FILE:*"  # WPS file links
        ]
        
        if not keep_logs:
            cleanup_patterns.append("log.init_atmosphere.*")
        
        removed_count = 0
        
        for pattern in cleanup_patterns:
            files = list(init_dir.glob(pattern))
            
            for file_path in files:
                try:
                    if file_path.is_symlink() or file_path.is_file():
                        file_path.unlink()
                        removed_count += 1
                        self.logger.debug(f"[DEBUG] Removed: {file_path.name}")
                        
                except Exception as e:
                    self.logger.warning(f"WARNING: Could not remove {file_path}: {e}")
        
        if removed_count > 0:
            self.logger.info(f"SUCCESS: Removed {removed_count} temporary files")
        else:
            self.logger.info("[INFO] No temporary files found to remove")
    
    def get_output_info(self, init_dir: Path) -> Dict[str, Any]:
        """
        Get information about generated initial conditions.
        
        Parameters
        ----------
        init_dir : Path
            Directory containing initialization outputs
            
        Returns
        -------
        Dict[str, Any]
            Dictionary containing output file information
        """
        init_filename = self.config.get('paths.init_filename', 'brasil_circle.init.nc')
        output_file = init_dir / init_filename
        
        if not output_file.exists():
            return {
                'success': False,
                'filename': init_filename,
                'size_mb': 0,
                'path': str(output_file),
                'error': 'File not found'
            }
        
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        
        return {
            'success': True,
            'filename': init_filename,
            'size_mb': round(file_size_mb, 1),
            'path': str(output_file),
            'directory': str(init_dir),
            'start_time': self.dates.get('start_time'),
            'end_time': self.dates.get('end_time')
        }