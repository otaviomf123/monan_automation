"""
GFS Data Downloader
==================

Module for automatic download of GFS data from NOAA's AWS S3 bucket.
Handles GRIB2 files with forecast data at 0.25 degree resolution.

The GFS (Global Forecast System) data is available at:
- Base URL: https://noaa-gfs-bdp-pds.s3.amazonaws.com  
- File format: gfs.t{cycle}z.pgrb2.0p25.f{forecast_hour}
- Update cycles: 00, 06, 12, 18 UTC
- Forecast hours: 0 to 240+ hours (typically 3-hour intervals)

Typical file sizes:
- Each GRIB2 file: ~350-450 MB
- Full 10-day forecast: ~35-40 GB

Author: Otavio Feitosa
Date: 2025
"""

import logging
import time
from pathlib import Path
from typing import List, Tuple, Optional
from tqdm import tqdm
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from .config_loader import ConfigLoader


class GFSDownloader:
    """
    Downloads GFS meteorological data from NOAA's AWS S3 bucket.
    
    This class handles the automatic download of Global Forecast System (GFS)
    GRIB2 files required for MONAN/MPAS model initialization and boundary 
    conditions.
    
    Attributes
    ----------
    config : ConfigLoader
        Configuration object containing download parameters
    logger : logging.Logger
        Logger instance for this module
    base_url : str
        Base URL for GFS data on AWS S3
    run_date : str
        Model run date in YYYYMMDD format
    cycle : str
        Model cycle in HH format (00, 06, 12, 18)
    forecast_hours : range
        Range of forecast hours to download
    session : requests.Session
        HTTP session with retry strategy for robust downloads
    
    Examples
    --------
    >>> from src.config_loader import ConfigLoader
    >>> config = ConfigLoader('config.yml')
    >>> downloader = GFSDownloader(config)
    >>> success = downloader.download_gfs_data(Path('./ic_data'))
    """
    
    def __init__(self, config: ConfigLoader) -> None:
        """
        Initialize the GFS downloader with configuration parameters.
        
        Parameters
        ----------
        config : ConfigLoader
            Configuration object containing:
            - data_sources.gfs_base_url: S3 base URL
            - dates.run_date: Model run date (YYYYMMDD)
            - dates.cycle: Model cycle (00, 06, 12, 18)
            - data_sources.forecast_hours: Dict with start, end, step
            
        Raises
        ------
        KeyError
            If required configuration keys are missing
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Extract GFS configuration parameters
        self.base_url = config.get('data_sources.gfs_base_url')
        self.run_date = config.get('dates.run_date')
        self.cycle = config.get('dates.cycle')
        
        # Generate forecast hour range
        forecast_config = config.get('data_sources.forecast_hours')
        self.forecast_hours = range(
            forecast_config['start'],
            forecast_config['end'] + 1,
            forecast_config['step']
        )
        
        # Setup HTTP session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.logger.info(f"[INFO] GFS Downloader initialized for {self.run_date}/{self.cycle}Z")
        self.logger.info(f"[INFO] Forecast range: {min(self.forecast_hours)}-{max(self.forecast_hours)}h "
                        f"(step: {self.forecast_hours.step}h)")
    
    def _generate_file_urls(self) -> List[Tuple[str, str]]:
        """
        Generate list of URLs and filenames for GFS data download.
        
        The GFS file naming convention follows the pattern:
        gfs.t{cycle}z.pgrb2.0p25.f{forecast_hour:03d}
        
        Where:
        - cycle: Model initialization time (00, 06, 12, 18)
        - pgrb2: GRIB2 format identifier
        - 0p25: 0.25 degree resolution
        - f{forecast_hour}: Forecast hour zero-padded to 3 digits
        
        Returns
        -------
        List[Tuple[str, str]]
            List of (url, filename) tuples for each forecast hour
            
        Examples
        --------
        For run_date=20250727, cycle=00, forecast_hour=000:
        URL: https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.20250727/00/atmos/gfs.t00z.pgrb2.0p25.f000
        Filename: gfs.t00z.pgrb2.0p25.f000
        """
        urls_and_files = []
        
        for fh in self.forecast_hours:
            fh_str = f"{fh:03d}"
            filename = f"gfs.t{self.cycle}z.pgrb2.0p25.f{fh_str}"
            url = f"{self.base_url}/gfs.{self.run_date}/{self.cycle}/atmos/{filename}"
            
            urls_and_files.append((url, filename))
        
        return urls_and_files
    
    def _download_file(self, url: str, filepath: Path, max_retries: int = 3) -> bool:
        """
        Download a single GFS file with retry logic and progress tracking.
        
        Parameters
        ----------
        url : str
            Remote URL of the file to download
        filepath : Path
            Local path where file will be saved
        max_retries : int, optional
            Maximum number of retry attempts (default: 3)
            
        Returns
        -------
        bool
            True if download succeeded, False otherwise
            
        Notes
        -----
        - Uses streaming download with 8KB chunks for memory efficiency
        - Shows progress bar during download
        - Automatically removes partial files on failure
        - Implements exponential backoff on retries
        - Timeout set to 300 seconds per request
        """
        for attempt in range(max_retries + 1):
            try:
                self.logger.info(f"[INFO] Downloading: {filepath.name} (attempt {attempt + 1})")
                
                # Start download with streaming
                response = self.session.get(url, stream=True, timeout=300)
                response.raise_for_status()
                
                # Get file size for progress tracking
                total_size = int(response.headers.get('content-length', 0))
                size_mb = total_size / (1024 * 1024)
                
                # Download with progress bar
                with open(filepath, 'wb') as f:
                    with tqdm(
                        total=total_size,
                        unit='B',
                        unit_scale=True,
                        desc=f"{filepath.name} ({size_mb:.1f}MB)",
                        leave=False
                    ) as pbar:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                
                # Verify download completed successfully
                if filepath.stat().st_size == total_size:
                    self.logger.info(f"SUCCESS: Downloaded {filepath.name} ({size_mb:.1f}MB)")
                    return True
                else:
                    raise Exception(f"File size mismatch: expected {total_size}, got {filepath.stat().st_size}")
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"FAILED: Network error downloading {url}: {e}")
                
                # Remove partial file
                if filepath.exists():
                    filepath.unlink()
                    
                # Wait before retry (exponential backoff)
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    self.logger.info(f"[INFO] Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                
            except Exception as e:
                self.logger.error(f"FAILED: Unexpected error downloading {url}: {e}")
                
                # Remove partial file
                if filepath.exists():
                    filepath.unlink()
                    
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    self.logger.info(f"[INFO] Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
        
        return False
    
    def download_gfs_data(self, output_dir: Path) -> bool:
        """
        Download all required GFS files for the configured forecast period.
        
        This method orchestrates the complete download process:
        1. Creates output directory if needed
        2. Generates list of required files
        3. Downloads each file (skipping existing valid files)
        4. Provides detailed progress reporting
        5. Returns success/failure status
        
        Parameters
        ----------
        output_dir : Path
            Directory where GFS files will be stored
            
        Returns
        -------
        bool
            True if all files downloaded successfully, False otherwise
            
        Notes
        -----
        - Skips files that already exist and have size > 0
        - Provides detailed logging of progress and failures
        - Total download time typically 30-60 minutes for 10-day forecast
        - Requires ~35-40 GB of free disk space
        """
        self.logger.info(f"[INFO] Starting GFS data download for {self.run_date}/{self.cycle}Z")
        self.logger.info(f"[INFO] Output directory: {output_dir}")
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate file list
        urls_and_files = self._generate_file_urls()
        total_files = len(urls_and_files)
        
        self.logger.info(f"[INFO] Total files to download: {total_files}")
        self.logger.info(f"[INFO] Forecast hours: {min(self.forecast_hours)}h to {max(self.forecast_hours)}h "
                        f"(interval: {self.forecast_hours.step}h)")
        
        # Track progress
        success_count = 0
        failed_files = []
        skipped_count = 0
        
        start_time = time.time()
        
        # Download each file
        for i, (url, filename) in enumerate(urls_and_files, 1):
            filepath = output_dir / filename
            
            # Skip existing valid files
            if filepath.exists() and filepath.stat().st_size > 0:
                size_mb = filepath.stat().st_size / (1024 * 1024)
                self.logger.info(f"[{i}/{total_files}] SKIPPED: {filename} already exists ({size_mb:.1f}MB)")
                success_count += 1
                skipped_count += 1
                continue
            
            self.logger.info(f"[{i}/{total_files}] DOWNLOADING: {filename}")
            
            if self._download_file(url, filepath):
                success_count += 1
            else:
                failed_files.append(filename)
        
        # Calculate summary statistics
        elapsed_time = time.time() - start_time
        download_success = len(failed_files) == 0
        
        # Report final results
        self.logger.info("=" * 60)
        self.logger.info("GFS DOWNLOAD REPORT")
        self.logger.info("=" * 60)
        self.logger.info(f"Total files: {total_files}")
        self.logger.info(f"Successfully downloaded: {success_count - skipped_count}")
        self.logger.info(f"Already existed: {skipped_count}")
        self.logger.info(f"Failed downloads: {len(failed_files)}")
        self.logger.info(f"Total time: {elapsed_time/60:.1f} minutes")
        
        if failed_files:
            self.logger.warning("FAILED: The following files failed to download:")
            for filename in failed_files:
                self.logger.warning(f"  - {filename}")
        
        if download_success:
            self.logger.info("SUCCESS: All GFS files downloaded successfully")
        else:
            self.logger.error("FAILED: Some files could not be downloaded")
        
        return download_success
    
    def verify_downloads(self, data_dir: Path) -> List[str]:
        """
        Verify that all required GFS files are present and valid.
        
        Parameters
        ----------
        data_dir : Path
            Directory containing downloaded GFS files
            
        Returns
        -------
        List[str]
            List of missing or corrupted filenames
            
        Notes
        -----
        - Checks file existence and non-zero size
        - Does not validate GRIB2 format (use external tools for that)
        - Should be called after download_gfs_data()
        """
        urls_and_files = self._generate_file_urls()
        missing_files = []
        
        self.logger.info("[INFO] Verifying GFS file downloads...")
        
        for url, filename in urls_and_files:
            filepath = data_dir / filename
            
            if not filepath.exists():
                missing_files.append(filename)
                self.logger.warning(f"WARNING: Missing file: {filename}")
                continue
            
            # Check if file is empty
            if filepath.stat().st_size == 0:
                missing_files.append(filename)
                self.logger.warning(f"WARNING: Empty file: {filename}")
                continue
                
            # Log file info for valid files
            size_mb = filepath.stat().st_size / (1024 * 1024)
            self.logger.debug(f"[DEBUG] Valid file: {filename} ({size_mb:.1f}MB)")
        
        if missing_files:
            self.logger.warning(f"WARNING: {len(missing_files)} files are missing or corrupted")
            for filename in missing_files:
                self.logger.warning(f"  - {filename}")
        else:
            self.logger.info("SUCCESS: All GFS files are present and valid")
        
        return missing_files
    
    def get_file_list(self) -> List[str]:
        """
        Get list of expected GFS filenames for current configuration.
        
        Returns
        -------
        List[str]
            List of expected GFS filenames
            
        Examples
        --------
        >>> downloader = GFSDownloader(config)
        >>> files = downloader.get_file_list()
        >>> print(files[:3])
        ['gfs.t00z.pgrb2.0p25.f000', 'gfs.t00z.pgrb2.0p25.f003', 'gfs.t00z.pgrb2.0p25.f006']
        """
        urls_and_files = self._generate_file_urls()
        return [filename for url, filename in urls_and_files]
    
    def get_total_size_estimate(self) -> float:
        """
        Estimate total download size in GB for current configuration.
        
        Returns
        -------
        float
            Estimated total size in gigabytes
            
        Notes
        -----
        Based on typical GFS file sizes of ~400MB per file
        """
        num_files = len(self.forecast_hours)
        avg_file_size_gb = 0.4  # ~400MB per file
        return num_files * avg_file_size_gb