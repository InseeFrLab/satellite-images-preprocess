from astrovision.data.satellite_image import (
    SatelliteImage,
)
import numpy as np
import geopandas as gpd
import rasterio
from rasterio.features import shapes
import tempfile
from contextlib import contextmanager
from tqdm import tqdm
from shapely import wkt
import pandas as pd
import s3fs
import os
import boto3

@contextmanager
def temporary_raster():
    """Context manager for handling temporary raster files safely."""
    temp = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
    try:
        temp.close()
        yield temp.name
    finally:
        try:
            os.unlink(temp.name)
        except OSError:
            pass


def create_geojson_from_image(image: SatelliteImage) -> str:
    """
    Creates a Geopandas from an image.
    Args:
        image: A SatelliteImage.
    Returns:
        A Geopandas representing the clusters with their respective labels.
    """
    # Convert label to uint8
    sum_bands = np.sum(image.array, axis=0)
    binary_arr = np.where(sum_bands == 0, 0, 1)
    binary_arr = binary_arr.astype("uint8")

    # Define the metadata for the raster image
    metadata = {
        "driver": "GTiff",
        "dtype": "uint8",
        "count": 1,
        "width": binary_arr.shape[1],
        "height": binary_arr.shape[0],
        "crs": image.crs,
        "transform": rasterio.transform.from_origin(
            image.bounds[0], image.bounds[3], 0.5, 0.5
        ),  # pixel size is 0.5m
    }

    # Use the context manager for temporary file handling
    with temporary_raster() as temp_tif:
        with rasterio.open(temp_tif, "w+", **metadata) as dst:
            dst.write(binary_arr, 1)

            # Process shapes within the same rasterio context
            results = [
                {"properties": {"label": int(v)}, "geometry": s}
                for i, (s, v) in enumerate(shapes(binary_arr, mask=None, transform=dst.transform))
                if v != 0  # Keep only the labels which are not 0
            ]

    # Create and return GeoDataFrame
    if results:
        return gpd.GeoDataFrame.from_features(results)
    else:
        return gpd.GeoDataFrame(columns=["geometry", "label"])


if __name__ == "__main__":
    fs = s3fs.S3FileSystem(
        client_kwargs={'endpoint_url': 'https://'+'minio.lab.sspcloud.fr'},
        key=os.environ["AWS_ACCESS_KEY_ID"],
        secret=os.environ["AWS_SECRET_ACCESS_KEY"],
        token=os.environ["AWS_SESSION_TOKEN"])

    s3 = boto3.client(
        "s3", endpoint_url='https://'+'minio.lab.sspcloud.fr',
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=os.environ["AWS_SESSION_TOKEN"])

    # Définition du bucket et du dossier à lister
    bucket_name = "projet-slums-detection"
    s3_directory = "data-raw/PLEIADES/GUYANE_brut/2024/"

    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_directory)

    file_keys = []
    for page in pages:
        if "Contents" in page:
            file_keys.extend(obj["Key"] for obj in page["Contents"])

    print('Récupérer les métadonnées')
    bounds = {}
    crs = {}
    polygon_images = {}
    df = pd.DataFrame(columns=['Filepath', 'Polygon'])
    error_files = []
    meta_files = []

    for file_key in tqdm(file_keys):
        try:
            image = SatelliteImage.from_raster(
                                file_path=f"/vsis3/{bucket_name}/{file_key}",
                            )
        except:
            error_files.append(file_key)
        else:
            bounds[file_key] = str(image.bounds)
            crs[file_key] = str(image.crs)
            polygon_image = create_geojson_from_image(image)
            polygon_wkt = polygon_image.geometry[0].wkt
            polygon_images[file_key] = polygon_wkt
            df = pd.concat([df, pd.DataFrame([{'Filepath': file_key, 'Polygon': polygon_wkt}])], ignore_index=True)
            meta_files.append(file_key)

    # Enregistrer le fichier parquet avec les polygones
    s3_path = f"""s3://{bucket_name}/data-raw/PLEIADES/GUYANE_brut/polygones_images_brutes_2024.parquet"""
    with fs.open(s3_path, 'wb') as f:
        df.to_parquet(f, index=False)

    print('Créer les images contenant les métadonnées')
    s3_directory_meta = s3_directory[:-1] + "_metapoly/"

    for file_key in tqdm(meta_files):
        new_file = s3_directory_meta + file_key.split('/')[-1]

        # Nouvelles métadonnées à ajouter
        dt = s3.head_object(Bucket=bucket_name, Key=file_key).get('LastModified')
        formatted_date = str(dt.replace(microsecond=300000).isoformat())

        poly = wkt.loads(polygon_images[file_key])
        simplified_polygon = poly.simplify(tolerance=5)
        polygon_image_wkt = wkt.dumps(simplified_polygon, rounding_precision=2)

        new_metadata = {
            "x-amz-meta-bounding_box": bounds[file_key],
            "x-amz-meta-date": formatted_date,
            "x-amz-meta-crs": crs[file_key],
            "x-amz-meta-polygon": polygon_image_wkt
        }

        # Étape 1 : Récupérer les métadonnées existantes
        existing_metadata = s3.head_object(Bucket=bucket_name, Key=file_key).get('Metadata', {})

        # Étape 2 : Ajouter les nouvelles métadonnées aux existantes
        combined_metadata = existing_metadata.copy()
        # Convertir les valeurs des tuples en chaînes
        combined_metadata["x-amz-meta-bounding_box"] = str(new_metadata["x-amz-meta-bounding_box"])
        combined_metadata["x-amz-meta-date"] = str(new_metadata["x-amz-meta-date"])
        combined_metadata["x-amz-meta-crs"] = str(new_metadata["x-amz-meta-crs"])
        combined_metadata["x-amz-meta-polygon"] = str(new_metadata["x-amz-meta-polygon"])

        # Étape 3 : Copier le fichier avec les nouvelles métadonnées
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': file_key},
            Key=new_file,  # Ou remplacer avec le même nom pour écraser l'objet
            Metadata=combined_metadata,
            MetadataDirective='REPLACE'  # Remplacer les métadonnées existantes
        )

    print(f"""Images avec métadonnées créées ici {s3_directory_meta}""")
