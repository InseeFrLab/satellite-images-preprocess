from functions.download_data import get_file_system
import geopandas as gpd
import pandas as pd
from shapely import wkt
import argparse
import fsspec

from utils.mappings import name_dep_to_num_dep


def main(folder_zip_path: str):
    fs = get_file_system()

    with fs.open(folder_zip_path, 'rb') as f:
        gdf = gpd.read_file(f)

    gdf = gdf.rename(columns={
            'id_zone': 'code',
            'depcom': 'depcom_2018'
        }
    )

    gdf["ident_ilot"] = gdf["depcom_2018"].astype(str) + gdf["code"].astype(str)
    gdf["dep"] = gdf["depcom_2018"].astype(str).str[:3]
    gdfs_per_dep = {dep: gdf[gdf["dep"] == dep] for dep in gdf["dep"].unique()}

    for name_dep, num_dep in name_dep_to_num_dep.items():
        if num_dep in gdfs_per_dep.keys():
            gdf_to_save = gdfs_per_dep[num_dep]
            gdf_to_save = gdf_to_save.drop(columns=['dep'])
            filename_to_save = f"s3://projet-slums-detection/data-clusters/dep={name_dep}/part-0.parquet"
            gdf_to_save.to_parquet(filename_to_save, index=False, filesystem=fs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Make clusters geom file from SERN csv file")
    parser.add_argument("--folder_zip_path", type=str, required=True, help="Folder zip path (e.g., 's3://projet-slums-detection/cluster-geom-raw/couche_ilots_mars_2025.zip')")
    args = parser.parse_args()

    folder_zip_path = args.folder_zip_path
    main(folder_zip_path)
