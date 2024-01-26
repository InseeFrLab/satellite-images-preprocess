import re
import s3fs
from typing import List
from pyproj import Transformer
from utils.mappings import name_dep_to_crs
from astrovision.data import SatelliteImage


def crs_to_gps_image(
    filepath: str,
) -> (float, float):
    """
    Gives the gps point of the left-top boundingbox of the image.
    These bounds are found in the filename (quicker than if we have
    to open all the images). So this method is based on the filenames
    of the pleiades images. Argument is either a SatelliteImage or a filepath.

    Args:
        filepath (str):
            The full filepath.

    Returns:
        GPS coordinate (float, float):
            Latitude and longitutude.

    Example:
        >>> filename_1 = 'projet-slums-detection/data-raw/PLEIADES/MARTINIQUE/2022/
        ORT_2022_0712_1606_U20N_8Bits.jp2'
        >>> crs_to_gps_image(filename_1)
        (14.518646888444412, -61.032716786523345)
    """
    delimiters = ["-", "_"]

    pattern = "|".join(delimiters)

    split_filepath = filepath.split("/")
    split_filename = re.split(pattern, split_filepath[-1])
    dep = split_filepath[3]
    year = split_filepath[4]

    if year == "2022" and dep in ["GUADELOUPE", "MAYOTTE"]:
        top_bound_index = 4
        left_bound_index = 3
    else:
        top_bound_index = 3
        left_bound_index = 2

    x = float(split_filename[left_bound_index]) * 1000.0  # left
    y = float(split_filename[top_bound_index]) * 1000.0  # top

    str_crs = name_dep_to_crs[split_filepath[3]]

    transformer = Transformer.from_crs(f"EPSG:{str_crs}", "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(x, y)

    # Return GPS coordinates (latitude, longitude)
    return lat, lon


def gps_to_crs_point(
    lat: float,
    lon: float,
    crs: str,
) -> (float, float):
    """
    Gives the CRS point of a GPS point.

    Args:
        lat (float):
            Latitude
        lon (float):
            Longitude
        crs (str):
            The coordinate system of the point.

    Returns:
        CRS coordinate (float, float):

    Example:
        >>> gps_to_crs_point(-12.774895, 45.218719, "4471")
        (523739.43307804153, 8587747.558612097)
    """
    # Convert GPS coordinates to coordinates in destination coordinate system
    # (CRS)
    transformer = Transformer.from_crs(
        "EPSG:4326", f"EPSG:{crs}", always_xy=True
    )  # in case the input CRS is of integer type
    x, y = transformer.transform(lon, lat)
    # because y=lat and x=lon, the gps coordinates are in (lat,lon)

    # Return coordinates in the specified CRS
    return x, y


def find_image_of_point(
    coordinates: list,
    dep: str,
    year: str,
    fs: s3fs,
    coord_gps: bool = True,
) -> str:
    """
    Gives the image in the folder which contains the point (gps or crs).
    This method is based on the filenames of the pleiades images.
    Returns a message if the image is not in the folder.

    Args:
        coordinates (list):
            [x,y] CRS coordinate or [lat, lon] gps coordinate
        dep (str):
            The department in which we search the image containing
            the point.
        year(str):
            The year when the image was photographed.
        fs (s3fs)
        coord_gps (boolean):
            True if the coordinate is a gps coordinate,
            False if the coordinate is a crs coordinate.
            By default True.

    Returns:
        str:
            The path of the image containing the point.

    Examples:
        >>> from functions import download_data
        >>> fs = download_data.get_file_system()
        >>> find_image_of_point([713000.0, 1606000.0], "MARTINIQUE", "2022", fs, False)
        'projet-slums-detection/data-raw/PLEIADES/MARTINIQUE/2022/ORT_2022_0712_1606_U20N_8Bits.jp2'

        >>> from functions import download_data
        >>> fs = download_data.get_file_system()
        >>> folder = "projet-slums-detection/data-raw/PLEIADES/MARTINIQUE/2022/"
        >>> find_image_of_point([14.635338, -61.038345], "MARTINIQUE", "2022", fs)
        'projet-slums-detection/data-raw/PLEIADES/MARTINIQUE/2022/ORT_2022_0711_1619_U20N_8Bits.jp2'
    """
    folder_path = f"projet-slums-detection/data-raw/PLEIADES/{dep}/{year}/"

    if coord_gps:
        # Retrieve the crs via the department
        crs = name_dep_to_crs[dep]

        lat, lon = coordinates
        x, y = gps_to_crs_point(lat, lon, crs)

    else:
        x, y = coordinates

    # Retrieve left-top coordinates
    delimiters = ["-", "_"]

    pattern = "|".join(delimiters)

    if year == "2022" and dep in ["GUADELOUPE", "MAYOTTE"]:
        top_bound_index = 4
        left_bound_index = 3
    else:
        top_bound_index = 3
        left_bound_index = 2

    for filename in fs.ls(folder_path):
        split_filename = filename.split("/")[-1]
        split_filename = re.split(pattern, split_filename)
        left = float(split_filename[left_bound_index]) * 1000
        top = float(split_filename[top_bound_index]) * 1000
        right = left + 1000.0
        bottom = top - 1000.0

        if left <= x <= right:
            if bottom <= y <= top:
                return filename
    else:
        print("The point is not find in the folder.")
        return None


def find_image_different_years(
    filepath: str,
    different_year: int,
    fs: s3fs,
) -> str:
    """
    Finds the image which represents the same place but in a different year.
    The arguments can be either a SatelliteImage or the filepath of an image.
    This method is based on the filenames of the pleiades images.

    Args:
        filepath (str):
            The filepath of the image.
        different_year (int):
            The year we are interested in.
        fs (s3fs)

    Returns:
        str:
            The path of the image representing the same place but in a
            different period of time.

    Example:
        >>> from functions import download_data
        >>> fs = download_data.get_file_system()
        >>> filename_1 = 'projet-slums-detection/data-raw/PLEIADES/MARTINIQUE/2022/
        ORT_2022_0711_1619_U20N_8Bits.jp2'
        >>> find_image_different_years(filename_1, 2018, fs)
        'projet-slums-detection/data-raw/PLEIADES/MARTINIQUE/2018/972-2017-0711-1619-U20N-0M50-RVB-E100.jp2'
    """
    # Retrieve base department
    split_folder = filepath.split("/")[:-1]

    dep = split_folder[3]
    year = different_year

    folder_path = f"projet-slums-detection/data-raw/PLEIADES/{dep}/{year}/"

    # Retrieve left-top coordinates
    if filepath.find("_") != -1:
        pattern = "_"

    elif filepath.find("-") != -1:
        pattern = "-"

    split_filepath = filepath.split("/")[-1]
    split_filepath = split_filepath.split(pattern)

    if len(fs.ls(folder_path)) == 0:
        return f"Il n'existe pas de dossier d'images du département {dep} pour \
l'année {different_year}"

    else:
        filename = fs.ls(folder_path)[0]

        if filename.find("_") != -1:
            pattern = "_"

        elif filename.find("-") != -1:
            pattern = "-"

        split_filename = filename.split("/")[-1]
        split_filename = split_filename.split(pattern)

        if year == "2022" and dep in ["GUADELOUPE", "MAYOTTE"]:
            top_bound_index = 4
            left_bound_index = 3
        else:
            top_bound_index = 3
            left_bound_index = 2

        split_filename[left_bound_index] = split_filepath[left_bound_index]
        split_filename[top_bound_index] = split_filepath[top_bound_index]

        new_filename = pattern.join(split_filename)
        new_filename = f"{folder_path}{new_filename}"

        if new_filename in fs.ls(folder_path):
            return new_filename
        else:
            return "There is no image of this place in the requested year in the database Pléiades."


def point_is_in_image(
    image: SatelliteImage,
    coordinates: list,
    coord_gps: bool = True,
) -> bool:
    """
    Return True if the SatelliteImage contains the point (gps or crs).

    Args:
        image (SatelliteImage)
        coordinates (list):
            [x,y] CRS coordinate or [lat, lon] gps coordinate
        coord_gps (boolean):
            True if the coordinate is a gps coordinate,
            False if the coordinate is a crs coordinate.
            By default True.

    Returns:
        bool

    Examples:
    >>> from astrovision.data import SatelliteImage
    >>> filepath = 'projet-slums-detection/data-raw/PLEIADES/MARTINIQUE/2022/
    ORT_2022_0711_1619_U20N_8Bits.jp2'
    >>> image = SatelliteImage.from_raster(
                file_path=f"/vsis3/{filepath}",
                n_bands=3,
            )
    >>> point_is_in_image(image,[14.635338, -61.038345])
    True
    """
    if coord_gps:
        # Retrieve the crs via the department
        crs = image.crs[5:]

        lat, lon = coordinates
        x, y = gps_to_crs_point(lat, lon, crs)

    else:
        x, y = coordinates

    # Retrieve left-top coordinates
    left, bottom, right, top = image.bounds

    if left <= x <= right:
        if bottom <= y <= top:
            return True
    else:
        return False


def image_is_in_bb(
    image: SatelliteImage,
    bounding_box: List[float],
) -> bool:
    """
    Return True if the SatelliteImage is in the bouding box.

    Args:
        image (SatelliteImage)
        bounding_box (List[float]):
            [left, bottom, right, top]

    Returns:
        bool

    Examples:
    >>> from astrovision.data import SatelliteImage
    >>> filepath = 'projet-slums-detection/data-raw/PLEIADES/MARTINIQUE/2022/
    ORT_2022_0711_1619_U20N_8Bits.jp2'
    >>> image = SatelliteImage.from_raster(
                file_path=f"/vsis3/{filepath}",
                n_bands=3,
            )
    >>> image_is_in_bb(image,[710000.0, 1618000.0, 712000.0, 1620000.0])
    True
    """
    left, bottom, right, top = bounding_box

    # Retrieve left-top coordinates
    left_image, bottom_image, right_image, top_image = image.bounds

    if left <= left_image <= right and left <= right_image <= right:
        if bottom <= bottom_image <= top and bottom <= top_image <= top:
            return True
    else:
        return False
