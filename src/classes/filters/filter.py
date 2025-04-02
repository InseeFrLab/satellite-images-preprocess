from typing import List

import geopandas as gpd
import numpy as np
from astrovision.data import SatelliteImage
from astrovision.data.utils import generate_tiles_borders
from rasterio.features import rasterize, shapes
from scipy.ndimage import label
from shapely.geometry import Polygon


class Filter:
    """
    Filter class.
    """

    def __init__(self):
        return

    def is_too_black(
        self,
        image: SatelliteImage,
        black_value_threshold: int = 100,
        black_area_threshold: float = 0.5,
    ) -> bool:
        """
        Determine if an image has a significant proportion of black pixels.

        Parameters:
        - image (SatelliteImage): The input satellite image.
        - black_value_threshold (int, optional): The intensity threshold to consider a pixel as black.
          Pixels with intensity values less than this threshold are considered black. Default is 100.
        - black_area_threshold (float, optional): The threshold for the proportion of black pixels in the image.
          If the ratio of black pixels exceeds this threshold, the function returns True. Default is 0.5.

        Returns:
        - bool: True if the proportion of black pixels is greater than or equal to the threshold, False otherwise.
        """
        # Convert the RGB image to grayscale
        gray_image = 0.2989 * image.array[0] + 0.5870 * image.array[1] + 0.1140 * image.array[2]

        # Count the number of black pixels
        nb_black_pixels = np.sum(gray_image < black_value_threshold)

        # Calculate the proportion of black pixels
        black_pixel_ratio = nb_black_pixels / np.prod(gray_image.shape)

        # Check if the proportion exceeds the threshold
        return black_pixel_ratio >= black_area_threshold

    def mask_cloud(self, image: SatelliteImage, threshold: float = 0.7, min_relative_size: float = 0.0125) -> np.ndarray:
        """
        Generate a cloud mask based on pixel intensity and cluster size.

        Parameters:
        - image (SatelliteImage): The input satellite image.
        - threshold (float, optional): The relative intensity threshold to classify pixels as cloud.
          Default is 0.7.
        - min_relative_size (float, optional): The minimum relative size of a cluster to be considered as a cloud.
          Default is 0.0125.

        Returns:
        - np.ndarray: A binary mask indicating the cloud regions in the image.

        Example:
            >>> filename_1 = '../data/PLEIADES/2020/MAYOTTE/
            ORT_2020052526656219_0508_8599_U38S_8Bits.jp2'
            >>> date_1 = date.fromisoformat('2020-01-01')
            >>> si = SatelliteImage.from_raster(
                                        filename_1,
                                        date = date_1,
                                        n_bands = 3,
                                        dep = "976"
                                    )
            >>> mask = mask_cloud(si)
            >>> fig, ax = plt.subplots(figsize=(10, 10))
            >>> ax.imshow(np.transpose(si.array, (1, 2, 0))[:,:,:3])
            >>> ax.imshow(mask, alpha=0.3)
        """
        # Convert the RGB image to grayscale
        weights = np.array([0.2989, 0.5870, 0.1140])[:, np.newaxis, np.newaxis]
        grayscale = np.sum(weights * image.array, axis=0)
        grayscale = grayscale.astype(image.array.dtype)

        # Compute absolute threshold based on image dtype
        if grayscale.dtype == np.uint8:
            absolute_threshold = threshold * (2**8 - 1)
        elif grayscale.dtype == np.uint16:
            absolute_threshold = threshold * (2**16 - 1)
        else:
            raise ValueError(f"Unsupported dtype: {grayscale.dtype}. Expected np.uint8 or np.uint16.")

        # Find clusters of white pixels
        labeled, num_features = label(grayscale > absolute_threshold)
        region_sizes = np.bincount(labeled.flat)

        # Sort region labels by decreasing size
        sorted_labels = np.argsort(-region_sizes)

        mask = np.zeros_like(labeled)
        if num_features >= 1:
            for i in range(1, num_features + 1):
                # Minimum size of the cluster
                if region_sizes[sorted_labels[i]] >= min_relative_size * np.prod(grayscale.shape):
                    mask[labeled == sorted_labels[i]] = 1
                else:
                    break

        # Return the cloud mask
        return mask

    def create_mask_cloud(
        self,
        image: SatelliteImage,
        threshold_center: float = 0.7,
        threshold_full: float = 0.4,
        min_relative_size: float = 0.0125,
    ) -> np.ndarray:
        """
        Create a binary mask indicating cloud regions in the input satellite image.

        Parameters:
        - image (SatelliteImage): The input satellite image.
        - threshold_center (float, optional): The intensity threshold for center clouds.
          Default is 0.7.
        - threshold_full (float, optional): The intensity threshold for full clouds.
          Default is 0.4.
        - min_relative_size (float, optional): The minimum relative size of a cloud cluster.
          Default is 0.0125.

        Returns:
        - np.ndarray: A binary mask indicating the cloud regions in the image.

        Example:
            >>> filename_1 = '../data/PLEIADES/2020/MAYOTTE/
            ORT_2020052526656219_0508_8599_U38S_8Bits.jp2'
            >>> date_1 = date.fromisoformat('2020-01-01')
            >>> image_1 = SatelliteImage.from_raster(
                                        filename_1,
                                        date = date_1,
                                        n_bands = 3,
                                        dep = "976"
                                    )
            >>> mask_full = create_mask_cloud(image_1)
            >>> fig, ax = plt.subplots(figsize=(10, 10))
            >>> ax.imshow(np.transpose(image_1.array, (1, 2, 0))[:,:,:3])
            >>> ax.imshow(mask_full, alpha=0.3)
        """

        # Mask out center clouds from the image using the specified threshold
        cloud_center = self.mask_cloud(image, threshold_center, min_relative_size)

        # Mask out full clouds from the image using the specified threshold
        cloud_full = self.mask_cloud(image, threshold_full, min_relative_size)

        # Create GeoDataFrames from the masked center and full clouds
        g_center = self.mask_to_gdf(cloud_center)
        g_full = self.mask_to_gdf(cloud_full)

        # Spatial join on the GeoDataFrames for the masked full and center clouds
        result = gpd.sjoin(g_full, g_center, how="inner", predicate="intersects")

        # Remove any duplicate geometries
        result = result.drop_duplicates(subset="geometry")

        # Rasterize the geometries into a numpy array
        if result.empty:
            rasterized = np.zeros(image.array.shape[1:])
        else:
            rasterized = rasterize(
                result.geometry,
                out_shape=image.array.shape[1:],
                fill=0,
                out=None,
                all_touched=True,
                default_value=1,
                dtype=None,
            )

        return rasterized

    def mask_to_gdf(self, mask: np.array) -> gpd.GeoDataFrame:
        """
        Convert a binary mask to a GeoDataFrame containing polygons.

        Parameters:
        - mask (np.ndarray): Binary mask indicating regions of interest.

        Returns:
        - gpd.GeoDataFrame: GeoDataFrame containing polygons derived from the input mask.
        """

        polygon_list = []

        for shape in shapes(mask):
            polygon = Polygon(shape[0]["coordinates"][0])

            # Skip polygons with area greater than 85% of the mask area
            if polygon.area > 0.85 * mask.shape[0] * mask.shape[1]:
                continue

            polygon_list.append(polygon)

        gdf = gpd.GeoDataFrame(geometry=polygon_list)
        return gdf

    def is_cloud(
        self,
        si: SatelliteImage,
        tiles_size: int,
        threshold_center: float,
        threshold_full: float,
        min_relative_size: float,
    ) -> List[int]:
        """
        Determine cloud presence in tiles within a satellite image.

        Parameters:
        - si (SatelliteImage): The input satellite image.
        - tiles_size (int): The size of tiles for analysis.
        - threshold_center (float): The intensity threshold for center clouds.
        - threshold_full (float): The intensity threshold for full clouds.
        - min_relative_size (float): The minimum relative size of a cloud cluster.

        Returns:
        - List[int]: A list of binary values (1 for cloud, 0 for non-cloud) for each tile.
        """

        # Create a cloud mask for the entire image
        mask = self.create_mask_cloud(
            si,
            threshold_center,
            threshold_full,
            min_relative_size,
        )

        # Generate tile indices
        indices = generate_tiles_borders(mask.shape[0], mask.shape[1], tiles_size)

        # Extract masks for each tile
        masks = [mask[rows[0] : rows[1], cols[0] : cols[1]] for rows, cols in indices]

        # Return a list of binary values indicating cloud presence in each tile
        return [1 if np.sum(tile_mask) > np.prod(tile_mask.shape) * 0.5 else 0 for tile_mask in masks]
