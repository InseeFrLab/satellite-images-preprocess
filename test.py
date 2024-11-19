import os
from osgeo import gdal, osr
import numpy as np
import matplotlib.pyplot as plt
import s3fs
import os
import astrovision
from astrovision.data.satellite_image import (
    SatelliteImage,
)

# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C1.tif donnees_brutes/exemple_1.tif
# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C2.tif donnees_brutes/exemple_2.tif
# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C3.tif donnees_brutes/exemple_3.tif
# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C3.tif donnees_brutes/exemple_4.tif
# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C4.tif donnees_brutes/exemple_5.tif
# Chemin vers le fichier .tiff

file_path = 'donnees_brutes/exemple_5.tif'
satellite_image = SatelliteImage.from_raster(file_path)
machin = satellite_image.plot([0,1,2])
output_path  = "image.png"
machin.savefig(output_path)

# np.unique(satellite_image.array) , O0 no data

def get_file_system() -> s3fs.S3FileSystem:
    """
    Return the s3 file system.
    """
    return s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": f"https://{os.environ['AWS_S3_ENDPOINT']}"},
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

# List all your TIFF files
tif_files = fs.glob("projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/*.tif")
tif_files = [f"/vsis3/{f}" for f in tif_files]

# Build VRT options
vrt_options = gdal.BuildVRTOptions(resampleAlg='nearest', addAlpha=False, srcNodata=0, VRTNodata=0)


# Create the VRT
vrt_path = 'merged.vrt'
vrt = gdal.BuildVRT(vrt_path, tif_files[1:100], options=vrt_options)

ds = gdal.Open(vrt_path)
gt = ds.GetGeoTransform()
x_res = gt[1]
y_res = gt[5]

# Get the original raster size
x_size = ds.RasterXSize
y_size = ds.RasterYSize

# Calculate the adjusted raster size
tile_size = 2000  # pixels

# Adjust the raster size to be divisible by tile_size
adjusted_x_size = ((x_size + tile_size - 1) // tile_size) * tile_size
adjusted_y_size = ((y_size + tile_size - 1) // tile_size) * tile_size

# Calculate the adjusted bounding box
minx = gt[0]
maxy = gt[3]
maxx = minx + adjusted_x_size * x_res
miny = maxy + adjusted_y_size * y_res