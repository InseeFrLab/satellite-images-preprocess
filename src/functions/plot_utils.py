import math

import matplotlib.pyplot as plt
from astrovision.data import SatelliteImage, SegmentationLabeledSatelliteImage
from tqdm import tqdm
import re
import s3fs

from utils.mappings import name_dep_to_crs
from classes.filters.filter import Filter
from classes.labelers.labeler import Labeler
from functions.image_utils import gps_to_crs_point, find_image_of_point


def plot_list_path_square_cloud(
    list_filepaths: list,
    filter_: Filter,
    dep: str,
    year: str,
):
    list_filepaths = sorted(list_filepaths)
    size = int(math.sqrt(len(list_filepaths)))
    bands_indices = [0, 1, 2]

    list_labeled_image = []

    for im in tqdm(list_filepaths):
        # 1- Ouvrir avec SatelliteImage
        image = SatelliteImage.from_raster(
            file_path=f"../data/data-raw/{dep}/{year}/{im}",
            n_bands=len(bands_indices),
        )
        image.normalize()

        mask_cloud = filter_.create_mask_cloud(image, 0.7, 0.4, 0.0125)
        lsi = SegmentationLabeledSatelliteImage(image, mask_cloud)
        list_labeled_image.append(lsi)

        # plt.imshow(mask_cloud, cmap = 'gray')

    list_images1 = [iml.satellite_image for iml in list_labeled_image]
    list_labels1 = [iml.label for iml in list_labeled_image]

    list_bounding_box = [[im.bounds[3], im.bounds[0]] for im in list_images1]

    # Utiliser zip pour combiner les trois listes
    combined = zip(list_bounding_box, list_images1, list_labels1)

    # Trier les éléments combinés en fonction de la troisième liste
    sorted_combined = sorted(combined, key=lambda x: (-x[0][0], x[0][1]))

    # Diviser les listes triées en fonction de l'ordre des éléments
    __, list_images, list_labels = zip(*sorted_combined)

    size = int(math.sqrt(len(list_images)))

    # Create a figure and axes
    fig, axs = plt.subplots(nrows=size, ncols=2 * size, figsize=(20, 10))

    # Iterate over the grid of masks and plot them
    for i in range(size):
        for j in range(size):
            axs[i, j].imshow(
                list_images[i * size + j].array.transpose(1, 2, 0)[:, :, bands_indices]
            )

    for i in range(size):
        for j in range(size):
            axs[i, j + size].imshow(list_labels[i * size + j], cmap="gray")

    # Remove any unused axes
    for i in range(size):
        for j in range(2 * size):
            axs[i, j].set_axis_off()

    # Show the plot
    return plt.gcf()


def plot_square_nb_images_folder_cloud(
    dep: str, year: str, filter_: Filter, debut: int, fin: int, n_bands: int, fs: s3fs
):
    bands_indices = [i for i in range(int(n_bands))]

    list_labeled_image = []

    list_images = fs.ls(f"projet-slums-detection/data-raw/PLEIADES/{dep}/{year}/")
    list_images = sorted(list_images)[debut:fin]
    size = int(math.sqrt(len(list_images)))

    for im_path in tqdm(list_images):
        # 1- Ouvrir avec SatelliteImage
        image = SatelliteImage.from_raster(
            file_path=f"/vsis3/{im_path}",
            n_bands=int(n_bands),
        )
        image.normalize()

        mask_cloud = filter_.create_mask_cloud(image, 0.7, 0.4, 0.0125)
        lsi = SegmentationLabeledSatelliteImage(image, mask_cloud)
        list_labeled_image.append(lsi)

        # plt.imshow(mask_cloud, cmap = 'gray')

    list_images1 = [iml.satellite_image for iml in list_labeled_image]
    list_labels1 = [iml.label for iml in list_labeled_image]

    list_bounding_box = [[im.bounds[3], im.bounds[0]] for im in list_images1]

    # Utiliser zip pour combiner les trois listes
    combined = zip(list_bounding_box, list_images1, list_labels1)

    # Trier les éléments combinés en fonction de la troisième liste
    sorted_combined = sorted(combined, key=lambda x: (-x[0][0], x[0][1]))

    # Diviser les listes triées en fonction de l'ordre des éléments
    __, list_images, list_labels = zip(*sorted_combined)

    size = int(math.sqrt(len(list_images)))

    # Create a figure and axes
    fig, axs = plt.subplots(nrows=size, ncols=2 * size, figsize=(20, 10))

    # Iterate over the grid of masks and plot them
    for i in range(size):
        for j in range(size):
            axs[i, j].imshow(
                list_images[i * size + j].array.transpose(1, 2, 0)[:, :, bands_indices]
            )

    for i in range(size):
        for j in range(size):
            axs[i, j + size].imshow(list_labels[i * size + j], cmap="gray")

    # Remove any unused axes
    for i in range(size):
        for j in range(2 * size):
            axs[i, j].set_axis_off()

    # Show the plot
    return plt.gcf()


def plot_images_mask_around_point(
    point_gps: list,
    source: str,
    dep: str,
    year: str,
    labeler: Labeler,
    n_bands: int,
    fs: s3fs,
    nb_dist: int = 1,
):
    gps_image = find_image_of_point(point_gps, dep, year, fs)
    if gps_image:
        bands_indices = [i for i in range(int(n_bands))]

        list_labeled_image = []

        list_images = fs.ls(f"projet-slums-detection/data-raw/{source}/{dep}/{year}/")

        delimiters = ["-", "_"]

        pattern = "|".join(delimiters)
        images_bb = {}

        if year == "2022" and dep in ["GUADELOUPE", "MAYOTTE"]:
            top_bound_index = 4
            left_bound_index = 3
        else:
            top_bound_index = 3
            left_bound_index = 2

        for filename in list_images:
            split_filename = filename.split("/")[-1]
            split_filename = re.split(pattern, split_filename)
            images_bb[filename] = [
                int(split_filename[top_bound_index]),
                int(split_filename[left_bound_index]),
            ]

        crs = name_dep_to_crs[dep]
        point_crs = gps_to_crs_point(point_gps[0], point_gps[1], crs)
        bounds = [int(point_crs[1] / 1000), int(point_crs[0] / 1000)]

        list_images = [
            cle
            for cle, valeur in images_bb.items()
            if (
                bounds[0] - nb_dist <= valeur[0] <= bounds[0] + nb_dist
                and bounds[1] - nb_dist <= valeur[1] <= bounds[1] + nb_dist
            )
        ]

        size = int(math.sqrt(len(list_images)))

        for im_path in tqdm(list_images):
            # 1- Ouvrir avec SatelliteImage
            image = SatelliteImage.from_raster(
                file_path=f"/vsis3/{im_path}",
                n_bands=int(n_bands),
            )
            image.normalize()

            mask = labeler.create_segmentation_label(image)
            lsi = SegmentationLabeledSatelliteImage(image, mask)
            list_labeled_image.append(lsi)

            # plt.imshow(mask_cloud, cmap = 'gray')

        list_images1 = [iml.satellite_image for iml in list_labeled_image]
        list_labels1 = [iml.label for iml in list_labeled_image]

        list_bounding_box = [[im.bounds[3], im.bounds[0]] for im in list_images1]

        # Utiliser zip pour combiner les trois listes
        combined = zip(list_bounding_box, list_images1, list_labels1)

        # Trier les éléments combinés en fonction de la troisième liste
        sorted_combined = sorted(combined, key=lambda x: (-x[0][0], x[0][1]))

        # Diviser les listes triées en fonction de l'ordre des éléments
        __, list_images, list_labels = zip(*sorted_combined)

        size = int(math.sqrt(len(list_images)))

        # Create a figure and axes
        fig, axs = plt.subplots(nrows=size, ncols=2 * size, figsize=(20, 10))

        bb_images = []
        # Iterate over the grid of masks and plot them
        for i in range(size):
            for j in range(size):
                axs[i, j].imshow(
                    list_images[i * size + j].array.transpose(1, 2, 0)[:, :, bands_indices]
                )
                bounds = list(list_images[i * size + j].bounds)
                bb_images.append(bounds)
                axs[i, j].set_title(f"{len(bb_images)-1}")

        for i in range(size):
            for j in range(size):
                axs[i, j + size].imshow(list_labels[i * size + j], cmap="gray")

        # Remove any unused axes
        for i in range(size):
            for j in range(2 * size):
                axs[i, j].set_axis_off()

        # Show the plot
        return plt.gcf(), bb_images

    else:
        return gps_image
