"""
Labeler classes.
"""
from abc import ABC, abstractmethod
from typing import List, Tuple

import numpy as np
import pandas as pd
from astrovision.data import SatelliteImage
from rasterio.features import rasterize

from functions import download_data


class Labeler(ABC):
    """
    Labeler abstract base class.
    """

    def __init__(
        self,
        year: str,
        dep: str,
        task: str,
    ):
        """
        Constructor.

        Args:
            year (Literal): Year.
            dep (Literal): Departement.
            task (Literal): task
        """
        self.year = year
        self.dep = dep
        self.task = task
        if task not in ["segmentation", "detection"]:
            raise NotImplementedError("Task must be 'segmentation'" "or 'detection'.")

    @abstractmethod
    def create_segmentation_label(self, satellite_image: SatelliteImage) -> np.array:
        """
        Create a segmentation label (mask) for a SatelliteImage.

        Args:
            satellite_image (SatelliteImage): Satellite image.

        Returns:
            np.array: Segmentation mask.
        """
        raise NotImplementedError()

    def create_label(self, satellite_image: SatelliteImage):
        """
        Create a label for a SatelliteImage.

        Args:
            satellite_image (SatelliteImage): Satellite image.
            task (str): Task.
        """

        if self.task == "segmentation":
            return self.create_segmentation_label(satellite_image)
        elif self.task == "detection":
            return self.create_detection_label(satellite_image)


class BDTOPOLabeler(Labeler):
    """ """

    def __init__(
        self,
        year: str,
        dep: str,
        task: str,
    ):
        """
        Constructor.

        Args:
            labeling_date (datetime): Date of labeling data.
            dep (Literal): Departement.
        """
        super(BDTOPOLabeler, self).__init__(year, dep, task)
        self.labeling_data = download_data.load_bdtopo(year=self.year, dep=self.dep)
        self.labeling_data["bbox"] = self.labeling_data.geometry.apply(lambda geom: geom.bounds)

    def create_segmentation_label(self, satellite_image: SatelliteImage) -> np.array:
        """
        Create a segmentation label (mask) from BDTOPO data for a
        SatelliteImage.

        Args:
            satellite_image (SatelliteImage): Satellite image.

        Returns:
            np.array: Segmentation mask.
        """
        if self.labeling_data.crs != satellite_image.crs:
            self.labeling_data.geometry = self.labeling_data.geometry.to_crs(satellite_image.crs)

        # Filtering geometries from BDTOPO
        xmin, ymin, xmax, ymax = satellite_image.bounds
        patch = self.labeling_data.cx[xmin:xmax, ymin:ymax].copy()

        if patch.empty:
            rasterized = np.zeros(satellite_image.array.shape[1:], dtype=np.uint8)
        else:
            rasterized = rasterize(
                patch.geometry,
                out_shape=satellite_image.array.shape[1:],
                fill=0,
                out=None,
                transform=satellite_image.transform,
                all_touched=True,
                default_value=1,
                dtype=None,
            )

        return rasterized

    def create_segmentation_label_filtered(self, satellite_image: SatelliteImage) -> np.array:
        """
        Create a filtered segmentation label (mask) from BDTOPO
        data for a SatelliteImage. It keeps the buildings labelled as
        habitations or undefined.

        Args:
            satellite_image (SatelliteImage): Satellite image.

        Returns:
            np.array: Segmentation mask.
        """
        if self.labeling_data.crs != satellite_image.crs:
            self.labeling_data.geometry = self.labeling_data.geometry.to_crs(satellite_image.crs)

        # Filtering geometries from BDTOPO
        xmin, ymin, xmax, ymax = satellite_image.bounds
        patch = self.labeling_data.cx[xmin:xmax, ymin:ymax].copy()

        patch11 = patch[patch["USAGE1"] == "Indifférencié"]
        patch12 = patch[patch["USAGE1"] == "Résidentiel"]

        patch2 = pd.concat([patch11, patch12], ignore_index=True)

        # threshold
        patch_petite_hab = patch2[patch2["HAUTEUR"] <= 7.0]

        if patch_petite_hab.empty:
            rasterized = np.zeros(satellite_image.array.shape[1:], dtype=np.uint8)
        else:
            rasterized = rasterize(
                patch_petite_hab.geometry,
                out_shape=satellite_image.array.shape[1:],
                fill=0,
                out=None,
                transform=satellite_image.transform,
                all_touched=True,
                default_value=1,
                dtype=None,
            )

        return rasterized

    @staticmethod
    def get_object_coordinates_in_image(row, bounds, image_width, image_height):
        xmin, ymin, xmax, ymax = row["bbox"]

        xmin_in_image = (xmin - bounds[0]) * (image_width) / (bounds[2] - bounds[0])
        xmin_in_image = np.maximum(xmin_in_image, 0)
        xmin_in_image = round(xmin_in_image)

        xmax_in_image = (xmax - bounds[0]) * (image_width) / (bounds[2] - bounds[0])
        xmax_in_image = np.minimum(xmax_in_image, image_width)
        xmax_in_image = round(xmax_in_image)

        ymin_in_image = (ymin - bounds[1]) * (image_height) / (bounds[3] - bounds[1])
        ymin_in_image = np.maximum(ymin_in_image, 0)
        ymin_in_image = round(ymin_in_image)

        ymax_in_image = (ymax - bounds[1]) * (image_height) / (bounds[3] - bounds[1])
        ymax_in_image = np.minimum(ymax_in_image, image_height)
        ymax_in_image = round(ymax_in_image)

        return xmin_in_image, ymin_in_image, xmax_in_image, ymax_in_image

    @staticmethod
    def geometry_to_pixel_bounds(geom, transform):
        minx, miny, maxx, maxy = geom.bounds
        x_pixel_min, y_pixel_max = ~transform * (minx, miny)
        x_pixel_max, y_pixel_min = ~transform * (maxx, maxy)
        return (x_pixel_min, y_pixel_min, x_pixel_max, y_pixel_max)

    def create_detection_label(self, satellite_image: SatelliteImage) -> List[Tuple[int]]:
        """
        Create an object detection label for a SatelliteImage.

        Args:
            satellite_image (SatelliteImage): Satellite image.

        Returns:
            List[Tuple[int]]: Object detection label.
        """

        if self.labeling_data.crs != satellite_image.crs:
            self.labeling_data.geometry = self.labeling_data.geometry.to_crs(satellite_image.crs)

        image_height = satellite_image.array.shape[1]

        # Filtering geometries from BDTOPO
        xmin, ymin, xmax, ymax = satellite_image.bounds
        patch = self.labeling_data.cx[xmin:xmax, ymin:ymax].copy()

        if patch.empty:
            label = np.array([], dtype=np.uint8)
        else:
            label = patch.geometry.apply(
                self.geometry_to_pixel_bounds, transform=satellite_image.transform
            ).tolist()
            label = [
                tuple(max(min(round(coord), image_height), 0) for coord in bbox) for bbox in label
            ]
            label = [bbox for bbox in label if ((bbox[0] != bbox[2]) and (bbox[1] != bbox[3]))]
        return label
