import sys
import os
from astrovision.data import SatelliteImage, SegmentationLabeledSatelliteImage
from functions import download_data, labelling
import numpy as np
import matplotlib.pyplot as plt


def main(
    source: str,
    dep: str,
    year: str,
    n_bands: int,
    type_labeler: str,
    task: str,
    tiles_size: int,
    test: bool = True
):
    """
    Main method.
    """
    test = int(test)
    if test:
        dataset = "test"
    else:
        dataset = "train"
    fs = download_data.get_file_system()
    test_image_filepaths = fs.ls(f"projet-slums-detection/data-preprocessed/patchs/{type_labeler}/{task}/{source}/{dep}/{year}/{tiles_size}/{dataset}/")
    test_label_filepaths = fs.ls(f"projet-slums-detection/data-preprocessed/labels/{type_labeler}/{task}/{source}/{dep}/{year}/{tiles_size}/{dataset}/")
    test_image_filepaths = list(test_image_filepaths)
    test_label_filepaths = list(test_label_filepaths)
    test_image_filepaths.sort()
    test_label_filepaths.sort()

    os.makedirs(
        f"../data/labeled_patches/{type_labeler}/{task}/{source}/{dep}/{year}/{tiles_size}/{dataset}/",  # noqa
        exist_ok=True,
    )

    for image_path, label_path in zip(test_image_filepaths, test_label_filepaths):
        image = SatelliteImage.from_raster(
            file_path=f"/vsis3/{image_path}",
            n_bands=n_bands,
        )
        label = np.load(fs.open(f"s3://{label_path}"))
        lsi = SegmentationLabeledSatelliteImage(image, label)

        image_mask = lsi.plot(bands_indices=[0, 1, 2])
        filename = image_path.split("/")[-1]
        filename = filename.split(".")[0]
        image_mask.savefig(
            f"../data/labeled_patches/{type_labeler}/{task}/{source}/{dep}/{year}/{tiles_size}/{dataset}/{filename}.png"
        )
        plt.close()


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
