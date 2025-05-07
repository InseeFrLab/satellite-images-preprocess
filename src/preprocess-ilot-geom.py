from functions.download_data import get_file_system
import geopandas as gpd
import pandas as pd
from shapely import wkt
import argparse

from utils.mappings import name_dep_to_num_dep


def main(filename: str):
    fs = get_file_system()
    with fs.open(filename, mode='r') as f:
        df = pd.read_csv(f)

    df = df.rename(columns={
            'geom': 'geometry',
            'id_zone': 'code',
            'depcom': 'depcom_2018'
        }
    )

    df["geometry"] = df["geometry"].apply(wkt.loads)
    df["ident_ilot"] = df["depcom_2018"].astype(str) + df["code"].astype(str)
    df["dep"] = df["depcom_2018"].astype(str).str[:3]
    gdf = gpd.GeoDataFrame(df, geometry="geometry")
    gdfs_per_dep = {dep: gdf[gdf["dep"] == dep] for dep in gdf["dep"].unique()}

    for name_dep, num_dep in name_dep_to_num_dep.items():
        if num_dep in gdfs_per_dep.keys():
            gdf_to_save = gdfs_per_dep[num_dep]
            filename_to_save = f"s3://projet-slums-detection/data-clusters/dep={name_dep}/part-0.parquet"
            gdf_to_save.to_parquet(filename_to_save, filesystem=fs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Make clusters geom file from SERN csv file")
    parser.add_argument("--filename", type=str, required=True, help="Filename (e.g.,  's3://projet-slums-detection/cluster-geom-raw/geometry_ilots_mars_2025.csv')")
    args = parser.parse_args()

    filename = args.filename
    main(filename)
