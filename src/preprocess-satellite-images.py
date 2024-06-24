import os
import sys
import yaml

import numpy as np
from astrovision.data import SatelliteImage, SegmentationLabeledSatelliteImage
from tqdm import tqdm

from classes.filters.filter import Filter
from functions.download_data import get_raw_images, get_roi
from functions.labelling import get_labeler
from utils.mappings import name_dep_to_crs
from osgeo import gdal

gdal.UseExceptions()


def main(
    source: str,
    dep: str,
    year: str,
    n_bands: int,
    type_labeler: str,
    task: str,
    tiles_size: int,
    from_s3: bool,
):
    """
    Main method.
    """

    print("\n*** 1- Téléchargement de la base d'annotation...\n")
    labeler = get_labeler(type_labeler, year, dep, task)

    print("\n*** 2- Récupération des données...\n")
    images = get_raw_images(from_s3, source, dep, year)
    prepro_test_path = f"data/data-preprocessed/labels/{type_labeler}/{task}/{source}/{dep}/{year}/{tiles_size}/test/"
    prepro_train_path = f"data/data-preprocessed/labels/{type_labeler}/{task}/{source}/{dep}/{year}/{tiles_size}/train/"
    # Creating empty directories for train and test data
    os.makedirs(
        prepro_test_path,
        exist_ok=True,
    )
    os.makedirs(
        prepro_train_path,
        exist_ok=True,
    )
    os.makedirs(
        f"{prepro_test_path.replace('labels', 'patchs')}",
        exist_ok=True,
    )
    os.makedirs(
        f"{prepro_train_path.replace('labels', 'patchs')}",
        exist_ok=True,
    )
    print("\n*** 3- Annotation, découpage et filtrage des images...\n")

    # Instanciate a dict of metrics for normalization
    metrics = {
        "mean": [],
        "std": [],
    }

    for im in tqdm(images):
        # 1- Ouvrir avec SatelliteImage
        if int(from_s3):
            si = SatelliteImage.from_raster(
                file_path=f"/vsis3/{im}",
                n_bands=int(n_bands),
            )

        else:
            si = SatelliteImage.from_raster(
                file_path=im,
                n_bands=int(n_bands),
            )

        # 2- Labeliser avec labeler (labeler/tache)
        label = labeler.create_label(si)
        lsi = SegmentationLabeledSatelliteImage(si, label)

        # 3- Split les tuiles (param tiles_size)
        splitted_lsi = lsi.split(int(tiles_size))

        # 4- Import ROI borders
        roi = get_roi(dep)

        # 5- Filtre too black, clouds and ROI

        filter_ = Filter()

        if source == "PLEIADES":
            is_cloud = filter_.is_cloud(
                lsi.satellite_image,
                tiles_size=int(tiles_size),
                threshold_center=0.7,
                threshold_full=0.4,
                min_relative_size=0.0125,
            )
        else:
            is_cloud = [0] * len(splitted_lsi.satellite_image)

        splitted_lsi_filtered = [
            lsi
            for lsi, cloud in zip(splitted_lsi, is_cloud)
            if not (
                filter_.is_too_black(
                    lsi.satellite_image, black_value_threshold=25, black_area_threshold=0.5
                )
                or cloud
            )
            and (
                lsi.satellite_image.intersects_polygon(
                    roi.loc[0, "geometry"], crs=lsi.satellite_image.crs
                )
            )
        ]

        with open("src/config/bb_test.yaml", "r") as file:
            bbox_test = yaml.load(file, Loader=yaml.FullLoader)

        # 6- save dans data-prepro
        for i, lsi in enumerate(splitted_lsi_filtered):
            filename, ext = os.path.splitext(os.path.basename(im))
            is_test = any(
                [
                    lsi.satellite_image.intersects_box(tuple(bbox), crs=name_dep_to_crs[dep])
                    for bbox in bbox_test[dep]
                ]
            )

            if is_test:
                lsi.satellite_image.to_raster(
                    f"{prepro_test_path.replace('labels', 'patchs')}{filename}_{i:04d}{ext}"
                )
                np.save(
                    f"{prepro_test_path}{filename}_{i:04d}.npy",
                    lsi.label,
                )
            else:
                lsi.satellite_image.to_raster(
                    f"{prepro_train_path.replace('labels', 'patchs')}{filename}_{i:04d}{ext}"
                )
                np.save(
                    f"{prepro_train_path}{filename}_{i:04d}.npy",
                    lsi.label,
                )
                # get mean and std of an image
                metrics["mean"].append(np.mean(lsi.satellite_image.array, axis=(1, 2)))
                metrics["std"].append(np.std(lsi.satellite_image.array, axis=(1, 2)))

    metrics["mean"] = np.vstack(metrics["mean"]).mean(axis=0).tolist()
    metrics["std"] = np.vstack(metrics["std"]).mean(axis=0).tolist()

    with open(
        f"{prepro_train_path.replace('labels', 'patchs')}metrics-normalization.yaml", "w"
    ) as f:
        yaml.dump(metrics, f, default_flow_style=False)

    print("\n*** 4- Preprocessing terminé !\n")


if __name__ == "__main__":
    main(
        str(sys.argv[1]),
        str(sys.argv[2]),
        str(sys.argv[3]),
        str(sys.argv[4]),
        str(sys.argv[5]),
        str(sys.argv[6]),
        str(sys.argv[7]),
        str(sys.argv[8]),
    )
