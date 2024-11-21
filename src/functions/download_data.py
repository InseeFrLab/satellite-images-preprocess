import concurrent.futures
import os
import subprocess
from pathlib import Path
from typing import List, Optional

import geopandas as gpd
import pandas as pd
from s3fs import S3FileSystem
from tqdm import tqdm


def get_file_system() -> S3FileSystem:
    """
    Return the s3 file system.
    """
    return S3FileSystem(
        client_kwargs={"endpoint_url": f"https://{os.environ['AWS_S3_ENDPOINT']}"},
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


def load_bdtopo(
    year: str,
    dep: str,
) -> gpd.GeoDataFrame:
    """
    Load BDTOPO for a given datetime.

    Args:
        year (Literal): Year.
        dep (Literal): Departement.

    Returns:
        gpd.GeoDataFrame: BDTOPO GeoDataFrame.
    """

    if int(year) >= 2019:
        couche, ext = ("BATIMENT", "shp")
    elif int(year) < 2019:
        couche, ext = ("BATI_INDIFFERENCIE", "SHP")

    fs = get_file_system()

    s3_path = f"projet-slums-detection/data-label/BDTOPO/{dep}/{year}/{couche}.*"
    local_path = f"data/data-label/BDTOPO/{dep}/{year}/"

    fs.download(
        rpath=s3_path,
        lpath=local_path,
        recursive=True,
    )

    df = gpd.read_file(f"{local_path}{couche}.{ext}")

    return df


def get_raw_images(
    from_s3: bool,
    source: str,
    dep: str,
    year: str,
):
    if int(from_s3):
        fs = get_file_system()

        images = fs.ls((f"projet-slums-detection/data-raw/" f"{source}/{dep}/{year}"))
    else:
        images_path = f"data/data-raw/{source}/{dep}/{year}"
        download_data(images_path, source, dep, year)
        images = [f"{images_path}/{filename}" for filename in os.listdir(images_path)]

    return images


def get_roi(
    dep: str,
):
    fs = get_file_system()
    roi = gpd.read_file(fs.open(f"projet-slums-detection/data-roi/{dep}.geojson", "rb"))

    return roi


def download_data(
    images_path: str,
    source: str,
    dep: str,
    year: str,
):
    """
    Download data from a specified source, department, and year.
    Parameters:
        - source (str): The data source identifier.
        - dep (str): The department identifier.
        - year (str): The year for which data should be downloaded.
    """
    all_exist = os.path.exists(f"{images_path}")

    if all_exist:
        return None

    image_cmd = [
        "mc",
        "cp",
        "-r",
        f"s3/projet-slums-detection/data-raw/{source}/{dep}/{year}/",
        f"{images_path}",
    ]

    # download raw images
    with open("/dev/null", "w") as devnull:
        subprocess.run(image_cmd, check=True, stdout=devnull, stderr=devnull)


def load_cosia(
    year: str,
    dep: str,
):
    def list_gpkg_files(base_path: str, filesystem: S3FileSystem) -> List[str]:
        """
        List all GPKG files in the specified path.

        Returns:
            List[str]: List of GPKG file paths
        """
        try:
            pattern = f"{base_path}/**/*.gpkg"
            files = filesystem.glob(pattern)
            print(f"Found {len(files)} GPKG files")
            return files
        except Exception as e:
            print(f"Error listing GPKG files: {str(e)}")
            raise

    def read_single_file(file_path: str, filesystem: S3FileSystem) -> Optional[gpd.GeoDataFrame]:
        """
        Read a single GPKG file into a GeoDataFrame.

        Args:
            file_path (str): Path to the GPKG file

        Returns:
            Optional[gpd.GeoDataFrame]: The loaded GeoDataFrame or None if there's an error
        """

        try:
            with filesystem.open(file_path) as f:
                df = gpd.read_file(f, layer=Path(file_path).stem)

            # Add source file information
            df["source_file"] = Path(file_path).name
            return df
        except Exception as e:
            print(f"Error reading file {file_path}: {str(e)}")
            return None

    def process_files(
        base_path: str, filesystem: S3FileSystem, max_workers: int = 4
    ) -> gpd.GeoDataFrame:
        """
        Process all GPKG files and concatenate them into a single GeoDataFrame.

        Args:
            max_workers (int): Maximum number of concurrent workers for parallel processing

        Returns:
            gpd.GeoDataFrame: Concatenated GeoDataFrame
        """
        files = list_gpkg_files(base_path, filesystem)
        dataframes = []

        # Process files in parallel with progress bar
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(read_single_file, file, filesystem): file for file in files}

            with tqdm(total=len(files), desc="Processing GPKG files") as pbar:
                for future in concurrent.futures.as_completed(futures):
                    df = future.result()
                    if df is not None:
                        dataframes.append(df)
                    pbar.update(1)

        if not dataframes:
            raise ValueError("No valid data frames were created from the input files")

        # Concatenate all dataframes
        result = pd.concat(dataframes, ignore_index=True)
        print(f"Successfully concatenated {len(dataframes)} files into a single DataFrame")
        print(f"Final DataFrame shape: {result.shape}")

        return result

    fs = get_file_system()

    gdf = process_files(
        f"projet-slums-detection/data-label/COSIA/{dep}/{year}",
        filesystem=fs,
    )

    return gdf
