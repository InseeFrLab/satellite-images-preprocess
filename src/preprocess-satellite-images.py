import os
import sys

import numpy as np
import yaml
from osgeo import gdal
from pqdm.processes import pqdm
os.chdir("satellite-images-preprocess/src")
from functions.download_data import get_raw_images, get_roi
from functions.labelling import get_labeler
from functions.process_images import process_single_image
from utils.mappings import name_dep_to_crs

gdal.UseExceptions()

source="PLEIADES"
dep="MAYOTTE"
year="2023"
n_bands=3
type_labeler="COSIA"
task="segmentation"
tiles_size=250
from_s3=0


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

    print("\n*** 3- Annotation, découpage et filtrage des images...\n")

    # Import ROI borders
    roi = get_roi(dep)

    # Instanciate a dict of metrics for normalization
    metrics = {
        "mean": [],
        "std": [],
    }

    # Load bbox_test configuration
    with open("src/config/bb_test.yaml", "r") as file:
        bbox_test = yaml.load(file, Loader=yaml.FullLoader)

    max_workers = 30
    # Use pqdm for parallelization
    args = [
        [
            im,
            from_s3,
            n_bands,
            labeler,
            tiles_size,
            source,
            roi,
            bbox_test,
            name_dep_to_crs,
            dep,
            prepro_test_path,
            prepro_train_path,
        ]
        for im in images
    ]
    result = pqdm(args, process_single_image, n_jobs=max_workers, argument_type="args")

    metrics = {
        key: np.mean(
            np.stack([array for entry in result if entry[key] for array in entry[key]]), axis=0
        ).tolist()
        if any(entry[key] for entry in result)
        else None
        for key in ["mean", "std"]
    }

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
