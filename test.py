import os
from osgeo import gdal, osr
import numpy as np
import matplotlib.pyplot as plt

# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C1.tif donnees_brutes/exemple_1.tif
# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C2.tif donnees_brutes/exemple_2.tif
# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C3.tif donnees_brutes/exemple_3.tif
# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C3.tif donnees_brutes/exemple_4.tif
# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C4.tif donnees_brutes/exemple_5.tif
# Chemin vers le fichier .tiff

import astrovision
from astrovision.data.satellite_image import (
    SatelliteImage,
)

file_path = 'donnees_brutes/exemple_5.tif'
satellite_image = SatelliteImage.from_raster(file_path)
machin = satellite_image.plot([0,1,2])
machin.savefig(output_path)

import s3fs
import os
def get_file_system() -> s3fs.S3FileSystem:
    """
    Return the s3 file system.
    """
    return s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": f"https://{os.environ['AWS_S3_ENDPOINT']}"},
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
    )
fs = get_file_system()
images = fs.ls("projet-slums-detection/data-raw/PLEIADES/GUYANE/2024")
len(images)
images[0]


# List all your TIFF files
tif_files = fs.glob("projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/*.tif")

# Build VRT options
vrt_options = gdal.BuildVRTOptions(resampleAlg='nearest', addAlpha=False, srcNodata=-9999, VRTNodata=-9999)

# Create the VRT
vrt_path = 'merged.vrt'
vrt = gdal.BuildVRT(vrt_path, tif_files, options=vrt_options)