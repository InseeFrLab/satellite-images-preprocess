import os
from typing import List, Optional

import s3fs
from osgeo import gdal
from tqdm import tqdm
import argparse


def get_file_system(
    endpoint: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
) -> s3fs.S3FileSystem:
    """
    Initialize and return an S3 file system.

    Args:
        endpoint (str, optional): S3 endpoint URL.
        access_key (str, optional): AWS access key.
        secret_key (str, optional): AWS secret key.

    Returns:
        s3fs.S3FileSystem: S3 file system instance.
    """
    return s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": endpoint or f"https://{os.environ['AWS_S3_ENDPOINT']}"},
        key=access_key or os.environ.get("AWS_ACCESS_KEY_ID"),
        secret=secret_key or os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def list_tif_files(
    fs: s3fs.S3FileSystem,
    bucket: str,
    department: str,
    year: int,
    base_path: str = "data-unstructured",
    sensor: str = "PLEIADES",
    file_extension: str = "*.tif",
) -> List[str]:
    """
    List all TIFF files with configurable path structure.

    Args:
        fs (s3fs.S3FileSystem): S3 file system.
        bucket (str): S3 bucket name.
        department (str): Department (e.g., 'GUYANE').
        year (int): Capture year.
        base_path (str, optional): Base directory path.
        sensor (str, optional): Sensor or data source.
        file_extension (str, optional): File extension filter.

    Returns:
        List[str]: Complete list of TIFF file paths.
    """
    path_pattern = f"{bucket}/{base_path}/{sensor}/{department}_brut/{year}/{file_extension}"
    tif_files = fs.glob(path_pattern)
    return [f"/vsis3/{f}" for f in tif_files]


def create_vrt(
    tif_files: List[str], vrt_path: str = "merged.vrt", resample_algo: str = "nearest"
) -> gdal.Dataset:
    """
    Create a VRT (Virtual Raster) from TIFF files with progress bar.

    Args:
        tif_files (List[str]): TIFF file paths.
        vrt_path (str, optional): VRT output path.
        resample_algo (str, optional): Resampling algorithm.

    Returns:
        gdal.Dataset: VRT dataset.
    """
    with tqdm(total=len(tif_files), desc="VRT Creation", unit="file", ncols=80) as pbar:

        def progress_callback(pct, message, data):
            if pct < 100:
                pbar.update(1)
            return 1

        vrt_options = gdal.BuildVRTOptions(
            resampleAlg=resample_algo,
            addAlpha=False,
            srcNodata=0,
            VRTNodata=0,
            callback=progress_callback,
        )

        vrt = gdal.BuildVRT(vrt_path, tif_files, options=vrt_options)

    return vrt


def tile_raster(
    ds: gdal.Dataset, tile_size: int = 2000, output_dir: str = "/vsis3/projet-slums-detection/tmp"
) -> None:
    """
    Cut raster dataset into tiles.

    Args:
        ds (gdal.Dataset): Source dataset.
        tile_size (int, optional): Tile size in pixels.
        output_dir (str, optional): Output directory for tiles.
    """
    gt = ds.GetGeoTransform()
    x_res, y_res = gt[1], gt[5]

    x_size, y_size = ds.RasterXSize, ds.RasterYSize
    adjusted_x_size = ((x_size + tile_size - 1) // tile_size) * tile_size
    adjusted_y_size = ((y_size + tile_size - 1) // tile_size) * tile_size

    minx, maxy = gt[0], gt[3]
    maxx = minx + adjusted_x_size * x_res
    miny = maxy + adjusted_y_size * y_res
    
    for i in range(0, adjusted_x_size, tile_size):
        for j in range(0, adjusted_y_size, tile_size):
            tile_minx = minx + i * x_res
            tile_maxx = tile_minx + tile_size * x_res
            tile_maxy = maxy + j * y_res
            tile_miny = tile_maxy + tile_size * y_res

            # controle no data

            output_tile_path = os.path.join(output_dir, f"tile_{i}_{j}.tif")

            gdal.Translate(
                output_tile_path,
                ds,
                projWin=[tile_minx, tile_maxy, tile_maxx, tile_miny],
                width=tile_size,
                height=tile_size,
                creationOptions=["COMPRESS=LZW"],
                noData=0,
            )
            print(f"Tile generated: {output_tile_path}")


def main():
    parser = argparse.ArgumentParser(description="Raster tiling pipeline")
    parser.add_argument("--department", type=str, required=True, help="Department (e.g., 'GUYANE')")
    parser.add_argument("--year", type=int, required=True, help="Year (e.g., 2024)")
    parser.add_argument("--tile_size", type=int, default=2000, help="Tile size in pixels")
    parser.add_argument("--output_dir", type=str, required=True, help="Output directory for tiles")
    parser.add_argument("--bucket", type=str, default="projet-slums-detection", help="S3 bucket name")
    parser.add_argument("--base_path", type=str, default="data-raw", help="Base path for source TIFF files")
    parser.add_argument("--sensor", type=str, default="PLEIADES", help="Sensor name")
    args = parser.parse_args()

    # Set S3 configuration
    os.environ["CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE"] = "YES"

    # Initialize S3 file system
    fs = get_file_system()

    # List TIFF files
    tif_files = list_tif_files(
        fs, args.bucket, args.department, args.year,
        base_path=args.base_path, sensor=args.sensor
    )

    if not tif_files:
        print("No TIFF files found. Exiting.")
        return

    # Create VRT
    vrt = create_vrt(tif_files)

    # Tile cutting
    tile_raster(vrt, args.tile_size, args.output_dir)

    # Cleanup
    vrt = None
    print("Processing completed.")

if __name__ == "__main__":
    main()
