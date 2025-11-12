"""
WPS (WRF Preprocessing System) Processor
=======================================

Module for processing meteorological data using WPS ungrib utility.
WPS is part of the WRF model suite and handles format conversion
from GRIB to intermediate format for use with MPAS models.

IMPORTANTE: As configuracoes de dominio (dx, dy, e_we, e_sn, etc.) 
sao usadas APENAS pelo WPS para processar dados meteorologicos.
O MONAN usa malha nao-estruturada definida no arquivo .static.nc.

WPS Components Used:
- ungrib: Converts GRIB2 files to intermediate WPS format
- link_grib.csh: Creates symbolic links to GRIB files
- Vtable: Variable translation table for GFS data

Output Format:
- FILE:YYYY-MM-DD_HH files containing meteorological fields
- Compatible with MPAS init_atmosphere_model

Typical Usage:
1. Download GFS GRIB2 files
2. Link GRIB files using link_grib.csh  
3. Run ungrib.exe to convert to WPS format
4. Use FILE outputs for MPAS initialization

Author: Otavio Feitosa
Date: 2025
"""

import logging
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any

from .config_loader import ConfigLoader
from .utils import create_symbolic_link, run_command, write_namelist


class WPSProcessor:
    """
    Processes meteorological data using WPS (WRF Preprocessing System).
    
    This class handles the conversion of GFS GRIB2 files to WPS intermediate
    format using the ungrib utility. The output files are then used as input
    for MPAS model initialization.
    
    Attributes
    ----------
    config : ConfigLoader
        Configuration object containing WPS settings
    logger : logging.Logger
        Logger instance for this module
    paths : Dict[str, str]
        Dictionary of file/directory paths from configuration
    dates : Dict[str, str]
        Dictionary of date/time settings from configuration
    domain : Dict[str, Any]
        Dictionary of domain configuration parameters
    
    Examples
    --------
    >>> from src.config_loader import ConfigLoader
    >>> config = ConfigLoader('config.yml')
    >>> wps = WPSProcessor(config)
    >>> success = wps.process(Path('./ic_data'))
    """
    
    def __init__(self, config: ConfigLoader) -> None:
        """
        Initialize the WPS processor with configuration parameters.
        
        Parameters
        ----------
        config : ConfigLoader
            Configuration object containing:
            - paths.wps_dir: WPS installation directory
            - paths.link_grib: Path to link_grib.csh script
            - paths.ungrib_exe: Path to ungrib.exe executable
            - paths.vtable_gfs: Path to GFS variable table
            - paths.wps_geog_path: Path to WPS geographic data
            - dates: Start/end times for processing
            - domain: Grid domain configuration
            
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
        self.domain = config.get_domain_config()
        
        self.logger.info("[INFO] WPS Processor initialized")
        self.logger.info(f"[INFO] WPS directory: {self.paths.get('wps_dir', 'Not configured')}")
    
    def _create_wps_links(self, work_dir: Path) -> bool:
        """
        Create symbolic links for required WPS executables and files.
        Uses Vtable.ECMWF for ERA5 data, Vtable.GFS for GFS data.
        
        This method creates symbolic links in the working directory for:
        - link_grib.csh: Script to link GRIB files
        - ungrib.exe: UNGRIB executable
        - Vtable: Variable translation table (GFS or ECMWF based on data source)
        
        Parameters
        ----------
        work_dir : Path
            Working directory where links will be created
            
        Returns
        -------
        bool
            True if all links created successfully, False otherwise
            
        Notes
        -----
        Symbolic links are used instead of copies to save disk space
        and ensure we're using the latest versions of executables.
        """
        self.logger.info("[INFO] Creating WPS symbolic links...")
        
        # Get appropriate Vtable based on data source
        vtable_path = self.config.get_vtable_path()
        data_source = self.config.get_data_source_type()
        
        self.logger.info(f"[INFO] Using Vtable for {data_source.upper()} data: {vtable_path}")
        
        # Define required links: (source, target)
        required_links = [
            (self.paths['link_grib'], work_dir / 'link_grib.csh'),
            (self.paths['ungrib_exe'], work_dir / 'ungrib.exe'), 
            (vtable_path, work_dir / 'Vtable')  # CHANGED: Use dynamic Vtable
        ]
        
        success = True
        
        for source, target in required_links:
            source_path = Path(source)
            
            # Check if source exists
            if not source_path.exists():
                self.logger.error(f"FAILED: Source file does not exist: {source}")
                success = False
                continue
            
            # Remove existing link if present
            if target.exists() or target.is_symlink():
                target.unlink()
            
            # Create symbolic link
            if create_symbolic_link(source_path, target):
                self.logger.info(f"SUCCESS: Created link: {target.name} -> {source}")
            else:
                self.logger.error(f"FAILED: Could not create link: {target}")
                success = False
        
        return success
    
    def _generate_wps_namelist(self) -> Dict[str, Dict[str, Any]]:
        """
        Generate namelist configuration for WPS processing.
        
        The namelist controls ungrib behavior and domain specifications.
        Key sections:
        - share: Common settings for all WPS components
        - geogrid: Geographic grid definition (not used for ungrib only)
        - ungrib: Output format and file prefix settings
        - metgrid: Meteorological grid settings (not used for ungrib only)
        
        Returns
        -------
        Dict[str, Dict[str, Any]]
            Nested dictionary containing namelist sections and parameters
            
        Notes
        -----
        The namelist follows Fortran namelist format with specific sections:
        - interval_seconds: Time interval between data (10800 = 3 hours)
        - out_format: 'WPS' for intermediate format
        - prefix: 'FILE' creates FILE:YYYY-MM-DD_HH output files
        """
        namelist_config = {
            'share': {
                'wrf_core': 'ARW',
                'max_dom': 1,
                'start_date': self.dates['start_time'],
                'end_date': self.dates['end_time'],
                'interval_seconds': 10800  # 3 hours in seconds
            },
            'geogrid': {
                'parent_id': 1,
                'parent_grid_ratio': 1,
                'i_parent_start': 1,
                'j_parent_start': 1,
                'e_we': self.domain['e_we'],
                'e_sn': self.domain['e_sn'],
                'geog_data_res': 'default',
                'dx': self.domain['dx'],
                'dy': self.domain['dy'],
                'map_proj': 'lambert',
                'ref_lat': self.domain['ref_lat'],
                'ref_lon': self.domain['ref_lon'],
                'truelat1': self.domain['truelat1'],
                'truelat2': self.domain['truelat2'],
                'stand_lon': self.domain['stand_lon'],
                'geog_data_path': self.paths['wps_geog_path']
            },
            'ungrib': {
                'out_format': 'WPS',  # Output in WPS intermediate format
                'prefix': 'FILE'      # Output files named FILE:YYYY-MM-DD_HH
            },
            'metgrid': {
                'fg_name': 'FILE'     # Input file prefix from ungrib
            }
        }
        
        self.logger.info("[INFO] Generated WPS namelist configuration")
        self.logger.info(f"[INFO] Processing period: {self.dates['start_time']} to {self.dates['end_time']}")
        
        return namelist_config
    
    def _link_grib_files(self, work_dir: Path, ic_dir: Path) -> bool:
        """
        Execute link_grib.csh script to prepare GRIB files for ungrib.
        Handles both GFS and ERA5 GRIB formats.
        
        The link_grib.csh script creates numbered symbolic links (GRIBFILE.AAA,
        GRIBFILE.AAB, etc.) that ungrib can process sequentially.
        
        Parameters
        ----------
        work_dir : Path
            Working directory containing link_grib.csh
        ic_dir : Path
            Directory containing GRIB files (GFS or ERA5)
            
        Returns
        -------
        bool
            True if linking successful, False otherwise
            
        Notes
        -----
        - Requires csh or tcsh shell to be available
        - Creates GRIBFILE.* links in working directory
        - Links are created in alphabetical order of source files
        """
        self.logger.info("[INFO] Executing link_grib.csh to prepare GRIB files...")
        
        # Detect data source and use appropriate patterns
        data_source = self.config.get_data_source_type()
        
        if data_source == 'era5':
            grib_patterns = ["era5_*.grib"]
            self.logger.info("[INFO] Using ERA5 GRIB patterns")
        else:
            grib_patterns = ["gfs.*.pgrb2.*", "gfs.*.grb2"]
            self.logger.info("[INFO] Using GFS GRIB patterns")
        
        # Find available GRIB files
        grib_files = []
        for pattern in grib_patterns:
            grib_files.extend(ic_dir.glob(pattern))
        
        if not grib_files:
            self.logger.error("FAILED: No GRIB files found in directory")
            self.logger.error(f"[DEBUG] Searched patterns: {grib_patterns}")
            self.logger.error(f"[DEBUG] In directory: {ic_dir}")
            return False
        
        # Sort files to ensure consistent processing order
        grib_files.sort()
        
        self.logger.info(f"[INFO] Found {len(grib_files)} GRIB files to link")
        self.logger.debug(f"[DEBUG] First file: {grib_files[0].name}")
        self.logger.debug(f"[DEBUG] Last file: {grib_files[-1].name}")
        
        # Execute link_grib.csh script with appropriate pattern
        # Note: Using shell=True to handle csh script execution
        if data_source == 'era5':
            grib_pattern = str(ic_dir / "era5_*.grib")  # Match ERA5 files
        else:
            grib_pattern = str(ic_dir / "gfs.*.pgrb2.*")
        
        command = f"./link_grib.csh {grib_pattern}"
        
        try:
            self.logger.info(f"[INFO] Running command: {command}")
            result = subprocess.run(
                command,
                shell=True,
                cwd=work_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode != 0:
                self.logger.error(f"FAILED: link_grib.csh returned error code {result.returncode}")
                self.logger.error(f"[DEBUG] stderr: {result.stderr}")
                self.logger.error(f"[DEBUG] stdout: {result.stdout}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("FAILED: link_grib.csh timed out after 5 minutes")
            return False
        except Exception as e:
            self.logger.error(f"FAILED: Error executing link_grib.csh: {e}")
            return False
        
        # Verify GRIBFILE links were created
        gribfiles = list(work_dir.glob("GRIBFILE.*"))
        
        if not gribfiles:
            self.logger.error("FAILED: No GRIBFILE links created")
            return False
        
        self.logger.info(f"SUCCESS: Created {len(gribfiles)} GRIBFILE links")
        
        # Log first few GRIBFILE names for verification
        for i, gribfile in enumerate(sorted(gribfiles)[:3]):
            target = gribfile.resolve() if gribfile.is_symlink() else "unknown"
            self.logger.debug(f"[DEBUG] {gribfile.name} -> {target.name}")
        
        return True
    
    def _run_ungrib(self, work_dir: Path) -> bool:
        """
        Execute ungrib.exe to convert GRIB files to WPS intermediate format.
        
        Ungrib reads the linked GRIBFILE.* files and the Vtable to extract
        meteorological fields, then writes them in WPS intermediate format
        as FILE:YYYY-MM-DD_HH files.
        
        Parameters
        ----------
        work_dir : Path
            Working directory containing ungrib.exe and input files
            
        Returns
        -------
        bool
            True if ungrib completed successfully, False otherwise
            
        Notes
        -----
        - Requires namelist.wps file in working directory
        - Creates FILE:YYYY-MM-DD_HH output files
        - Typical processing time: 5-15 minutes for 10-day forecast
        - Uses 30-minute timeout for robustness
        """
        self.logger.info("[INFO] Executing ungrib.exe to convert GRIB to WPS format...")
        
        # Check prerequisites
        prerequisites = [
            work_dir / 'ungrib.exe',
            work_dir / 'namelist.wps',
            work_dir / 'Vtable'
        ]
        
        for prereq in prerequisites:
            if not prereq.exists():
                self.logger.error(f"FAILED: Required file missing: {prereq}")
                return False
        
        # Check for GRIBFILE inputs
        gribfiles = list(work_dir.glob("GRIBFILE.*"))
        if not gribfiles:
            self.logger.error("FAILED: No GRIBFILE inputs found for ungrib")
            return False
        
        self.logger.info(f"[INFO] Processing {len(gribfiles)} GRIB files")
        
        try:
            # Execute ungrib
            result = subprocess.run(
                ["./ungrib.exe"],
                cwd=work_dir,
                capture_output=True,
                text=True,
                timeout=1800  # 30 minute timeout
            )
            
            if result.returncode != 0:
                self.logger.error(f"FAILED: ungrib.exe returned error code {result.returncode}")
                self.logger.error(f"[DEBUG] stderr: {result.stderr}")
                
                # Check for common error patterns
                if "STOP" in result.stderr or "ERROR" in result.stderr:
                    self.logger.error("[DEBUG] Ungrib encountered a fatal error")
                    
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("FAILED: ungrib.exe timed out after 30 minutes")
            return False
        except Exception as e:
            self.logger.error(f"FAILED: Error executing ungrib.exe: {e}")
            return False
        
        # Verify FILE outputs were created
        file_outputs = list(work_dir.glob("FILE:*"))
        
        if not file_outputs:
            self.logger.error("FAILED: ungrib.exe produced no FILE outputs")
            return False
        
        # Sort and analyze outputs
        file_outputs.sort()
        
        # Check for empty files
        empty_files = [f for f in file_outputs if f.stat().st_size == 0]
        if empty_files:
            self.logger.warning(f"WARNING: {len(empty_files)} empty FILE outputs detected")
            for empty_file in empty_files[:3]:  # Show first 3
                self.logger.warning(f"[DEBUG] Empty file: {empty_file.name}")
        
        # Calculate total output size
        total_size = sum(f.stat().st_size for f in file_outputs)
        total_size_mb = total_size / (1024 * 1024)
        
        self.logger.info(f"SUCCESS: ungrib.exe completed successfully")
        self.logger.info(f"[INFO] Generated {len(file_outputs)} FILE outputs ({total_size_mb:.1f} MB total)")
        self.logger.info(f"[INFO] Time range: {file_outputs[0].name} to {file_outputs[-1].name}")
        
        return True
    
    def process(self, ic_dir: Path) -> bool:
        """
        Execute complete WPS processing workflow.
        
        This method orchestrates the full WPS processing pipeline:
        1. Create symbolic links to WPS executables and data files
        2. Generate namelist.wps configuration file
        3. Link GRIB files using link_grib.csh
        4. Run ungrib.exe to convert GRIB to WPS format
        5. Verify outputs are created successfully
        
        Parameters
        ----------
        ic_dir : Path
            Directory containing GRIB2 files to process (GFS or ERA5)
            
        Returns
        -------
        bool
            True if entire workflow completed successfully, False otherwise
            
        Notes
        -----
        - Uses ic_dir as working directory for all operations
        - Creates temporary files that can be cleaned up afterward
        - Typical processing time: 10-20 minutes for 10-day forecast
        - Requires ~2-5 GB additional disk space for intermediate files
        """
        self.logger.info("=" * 60)
        self.logger.info("STARTING WPS PROCESSING WORKFLOW")
        self.logger.info("=" * 60)
        self.logger.info(f"[INFO] IC data directory: {ic_dir}")
        
        # Use IC directory as working directory
        work_dir = ic_dir
        
        try:
            # Step 1: Create WPS symbolic links
            self.logger.info("[INFO] Step 1/4: Creating WPS symbolic links...")
            if not self._create_wps_links(work_dir):
                self.logger.error("FAILED: Could not create required symbolic links")
                return False
            
            # Step 2: Generate WPS namelist
            self.logger.info("[INFO] Step 2/4: Generating WPS namelist...")
            namelist_data = self._generate_wps_namelist()
            namelist_path = work_dir / 'namelist.wps'
            
            if not write_namelist(namelist_path, namelist_data):
                self.logger.error("FAILED: Could not write namelist.wps")
                return False
                
            self.logger.info(f"SUCCESS: WPS namelist created: {namelist_path}")
            
            # Step 3: Link GRIB files
            self.logger.info("[INFO] Step 3/4: Linking GRIB files...")
            if not self._link_grib_files(work_dir, ic_dir):
                self.logger.error("FAILED: Could not link GRIB files")
                return False
            
            # Step 4: Run ungrib
            self.logger.info("[INFO] Step 4/4: Running ungrib conversion...")
            if not self._run_ungrib(work_dir):
                self.logger.error("FAILED: ungrib processing failed")
                return False
            
            # Final verification
            if not self.verify_wps_output(work_dir):
                self.logger.error("FAILED: WPS output verification failed")
                return False
            
            self.logger.info("=" * 60)
            self.logger.info("SUCCESS: WPS PROCESSING COMPLETED")
            self.logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            self.logger.error(f"FAILED: Unexpected error during WPS processing: {e}")
            self.logger.exception("[DEBUG] Full traceback:")
            return False
    
    def verify_wps_output(self, work_dir: Path) -> bool:
        """
        Verify WPS processing outputs are valid and complete.
        
        Parameters
        ----------
        work_dir : Path
            Directory containing WPS outputs to verify
            
        Returns
        -------
        bool
            True if all outputs are valid, False otherwise
            
        Notes
        -----
        Verification includes:
        - Presence of FILE:* output files
        - Non-zero file sizes
        - Expected number of time steps
        - Basic file naming convention checks
        """
        self.logger.info("[INFO] Verifying WPS processing outputs...")
        
        # Find all FILE outputs
        file_outputs = list(work_dir.glob("FILE:*"))
        
        if not file_outputs:
            self.logger.error("FAILED: No FILE outputs found")
            return False
        
        # Sort outputs for sequential analysis
        file_outputs.sort()
        
        # Check for empty files
        empty_files = [f for f in file_outputs if f.stat().st_size == 0]
        if empty_files:
            self.logger.error(f"FAILED: {len(empty_files)} empty FILE outputs found")
            for empty_file in empty_files:
                self.logger.error(f"[DEBUG] Empty file: {empty_file.name}")
            return False
        
        # Calculate statistics
        total_files = len(file_outputs)
        total_size = sum(f.stat().st_size for f in file_outputs)
        total_size_mb = total_size / (1024 * 1024)
        avg_size_mb = total_size_mb / total_files
        
        # Log verification results
        self.logger.info(f"SUCCESS: WPS verification passed")
        self.logger.info(f"[INFO] Total FILE outputs: {total_files}")
        self.logger.info(f"[INFO] Total size: {total_size_mb:.1f} MB")
        self.logger.info(f"[INFO] Average file size: {avg_size_mb:.1f} MB")
        self.logger.info(f"[INFO] First file: {file_outputs[0].name}")
        self.logger.info(f"[INFO] Last file: {file_outputs[-1].name}")
        
        return True
    
    def cleanup_wps_files(self, work_dir: Path, keep_outputs: bool = True) -> None:
        """
        Clean up temporary WPS processing files.
        
        Parameters
        ----------
        work_dir : Path
            Directory containing WPS files to clean
        keep_outputs : bool, optional
            If True, preserve FILE:* outputs (default: True)
            
        Notes
        -----
        Removes temporary files created during WPS processing:
        - GRIBFILE.* symbolic links
        - ungrib.log files
        - namelist.wps
        - Optionally FILE:* outputs if keep_outputs=False
        """
        self.logger.info("[INFO] Cleaning up WPS temporary files...")
        
        # Define cleanup patterns
        cleanup_patterns = [
            "GRIBFILE.*",
            "ungrib.log*",
            "namelist.wps"
        ]
        
        if not keep_outputs:
            cleanup_patterns.append("FILE:*")
            self.logger.warning("[WARNING] Removing FILE outputs as requested")
        
        removed_count = 0
        
        for pattern in cleanup_patterns:
            files = list(work_dir.glob(pattern))
            
            for file_path in files:
                try:
                    if file_path.is_symlink() or file_path.is_file():
                        file_path.unlink()
                        removed_count += 1
                        self.logger.debug(f"[DEBUG] Removed: {file_path.name}")
                        
                except Exception as e:
                    self.logger.warning(f"WARNING: Could not remove {file_path}: {e}")
        
        self.logger.info(f"SUCCESS: Removed {removed_count} temporary files")
    
    def get_file_outputs(self, work_dir: Path) -> List[Path]:
        """
        Get list of FILE outputs generated by WPS processing.
        
        Parameters
        ----------
        work_dir : Path
            Directory containing WPS outputs
            
        Returns
        -------
        List[Path]
            Sorted list of FILE:* output file paths
            
        Examples
        --------
        >>> wps = WPSProcessor(config)
        >>> wps.process(ic_dir)
        >>> outputs = wps.get_file_outputs(ic_dir)
        >>> print(f"Generated {len(outputs)} FILE outputs")
        """
        file_outputs = list(work_dir.glob("FILE:*"))
        file_outputs.sort()
        
        return file_outputs
    
    def get_processing_summary(self, work_dir: Path) -> Dict[str, Any]:
        """
        Get summary information about WPS processing results.
        
        Parameters
        ----------
        work_dir : Path
            Directory containing WPS outputs
            
        Returns
        -------
        Dict[str, Any]
            Dictionary containing processing statistics and file information
        """
        file_outputs = self.get_file_outputs(work_dir)
        
        if not file_outputs:
            return {
                'success': False,
                'file_count': 0,
                'total_size_mb': 0,
                'first_time': None,
                'last_time': None,
                'error': 'No FILE outputs found'
            }
        
        # Calculate statistics
        total_size = sum(f.stat().st_size for f in file_outputs)
        total_size_mb = total_size / (1024 * 1024)
        
        # Extract time information from filenames
        first_time = file_outputs[0].name.split(':')[1] if ':' in file_outputs[0].name else 'unknown'
        last_time = file_outputs[-1].name.split(':')[1] if ':' in file_outputs[-1].name else 'unknown'
        
        return {
            'success': True,
            'file_count': len(file_outputs),
            'total_size_mb': round(total_size_mb, 1),
            'average_size_mb': round(total_size_mb / len(file_outputs), 1),
            'first_time': first_time,
            'last_time': last_time,
            'work_directory': str(work_dir),
            'files': [f.name for f in file_outputs]
        }