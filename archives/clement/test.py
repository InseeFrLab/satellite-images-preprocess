import os

import s3fs
from astrovision.data.satellite_image import (
    SatelliteImage,
)
from osgeo import gdal


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


fs = get_file_system()
# List all your TIFF files
tif_files = fs.glob("projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/*.tif")
tif_files = [f"/vsis3/{f}" for f in tif_files]

# Build VRT options
vrt_options = gdal.BuildVRTOptions(resampleAlg="nearest", addAlpha=False, srcNodata=0, VRTNodata=0)

# Create the VRT
vrt_path = "merged.vrt"
vrt = gdal.BuildVRT(vrt_path, tif_files[1:10], options=vrt_options)
# vrt.FlushCache() pour écrire le fichier

ds = vrt
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

output_dir = "/vsis3/projet-slums-detection/tmp"
# os.makedirs(output_dir, exist_ok=True)

for i in range(0, adjusted_x_size, tile_size):
    for j in range(0, adjusted_y_size, tile_size):
        # Calcul des coordonnées géographiques pour chaque tuile
        tile_minx = minx + i * x_res
        tile_maxx = tile_minx + tile_size * x_res
        tile_maxy = maxy + j * y_res
        tile_miny = tile_maxy + tile_size * y_res

        # Nom de fichier de sortie
        output_tile_path = os.path.join(output_dir, f"tile_{i}_{j}.tif")

        # Découper une tuile à l'aide de gdal.Translate
        gdal.Translate(
            output_tile_path,
            ds,
            projWin=[tile_minx, tile_maxy, tile_maxx, tile_miny],
            width=tile_size,
            height=tile_size,
            creationOptions=["COMPRESS=LZW"],  # Compression pour réduire la taille des fichiers
            noData=0,  # Définit la valeur NoData
        )

        print(f"Tuile générée : {output_tile_path}")

# Étape 4 : Nettoyer les ressources
vrt_ds = None

file_path = "tiles/tile_6000_22000.tif"
satellite_image = SatelliteImage.from_raster(file_path)
machin = satellite_image.plot([0, 1, 2])
output_path = "image.png"
machin.savefig(output_path)
