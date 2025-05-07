from functions.download_data import get_file_system
import geopandas as gpd
import argparse
import pyarrow as pa
import pyarrow.dataset as ds

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
    num_dep_to_name_dep = {v: k for k, v in name_dep_to_num_dep.items()}
    gdf["dep"] = gdf["depcom_2018"].astype(str).str[:3].map(num_dep_to_name_dep)

    gdf = gdf[~gdf.geometry.is_empty].reset_index(drop=True)
    gdf = gdf[gdf.is_valid].reset_index(drop=True)
    gdf = gdf[gdf.notna()].reset_index(drop=True)

    arrow_table = gdf.to_arrow(index=None, geometry_encoding='WKB')

    # Convertir en table pyarrow
    table = pa.table(arrow_table)

    # Écriture du dataset partitionné
    ds.write_dataset(
        table,
        base_dir="s3://projet-slums-detection/data-clusters/",
        format="parquet",
        partitioning=["dep"],
        filesystem=fs,
        existing_data_behavior="overwrite_or_ignore"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Make clusters geom file from SERN csv file")
    parser.add_argument("--folder_zip_path", type=str, required=True, help="Folder zip path (e.g., 's3://projet-slums-detection/cluster-geom-raw/couche_ilots_mars_2025.zip')")
    args = parser.parse_args()

    folder_zip_path = args.folder_zip_path
    folder_zip_path = 's3://projet-slums-detection/cluster-geom-raw/couche_ilots_mars_2025.zip'
    main(folder_zip_path)
