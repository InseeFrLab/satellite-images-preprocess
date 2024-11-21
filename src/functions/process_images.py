import os

import numpy as np
from astrovision.data import SatelliteImage, SegmentationLabeledSatelliteImage

from classes.filters.filter import Filter


def process_single_image(
    im,
    from_s3: bool,
    n_bands: int,
    labeler,
    tiles_size: int,
    source,
    roi,
    bbox_test,
    name_dep_to_crs,
    dep,
    prepro_test_path,
    prepro_train_path,
):
    # 1- Open with SatelliteImage
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

    # 2- Label with labeler
    label = labeler.create_label(si)
    lsi = SegmentationLabeledSatelliteImage(si, label)

    # 3- Split tiles
    splitted_lsi = lsi.split(int(tiles_size))  # TODO int(tiles_size) is redundant

    filter_ = Filter()

    # 4- Filter: too black, clouds, and ROI
    if source == "PLEIADES":
        is_cloud = filter_.is_cloud(
            lsi.satellite_image,
            tiles_size=int(tiles_size),  # TODO int(tiles_size) is redundant
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

    # 5- Save filtered tiles to data-prepro
    metrics = {"mean": [], "std": []}
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
            # Get mean and std of an image
            metrics["mean"].append(np.mean(lsi.satellite_image.array, axis=(1, 2)))
            metrics["std"].append(np.std(lsi.satellite_image.array, axis=(1, 2)))

    return metrics
