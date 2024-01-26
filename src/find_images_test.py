import os
import matplotlib.pyplot as plt
import yaml

from functions import download_data, labelling
from functions.plot_utils import plot_images_mask_around_point

# Mayotte
# dep = "MAYOTTE"
# point = [-12.774895, 45.218719] # 4 en haut à droite
# point = [-12.719838, 45.117112] # 4 en haut à droite
# point = [-12.961326, 45.126497] # 4 en haut à droite

# # Martinique
# dep = "MARTINIQUE"
# point = [14.650193, -61.055606]
# point = [14.574659, -60.975153]

# Guadeloupe
# dep = "GUADELOUPE"
# point = [16.205556577017383, -61.482986085610385] # 4 en haut à gauche
# point = [16.329284496925418, -61.46031328436647] # 6 sur la gauche
# point = [16.009606762305808, -61.680187477540215]

# Guyane
# dep = "GUYANE"
# point = [4.858333, -52.279172]

# Reunion
dep = "REUNION"
# point = [-20.902806594883593, 55.50138840828117] # 4 en haut à gauche
# point = [-21.05190440896064, 55.223939457016336] # 6 à droite
point = [-21.106326150694997, 55.294520868581344]  # 2 en bas à droite


source = "PLEIADES"
year = "2022"
task = "segmentation"
type_labeler = "BDTOPO"
tiles_size = "250"
n_bands = "3"
# Initialize S3 file system
fs = download_data.get_file_system()

print("\n*** 1- Téléchargement de la base d'annotation...\n")
labeler = labelling.get_labeler(type_labeler, year, dep, task)

os.makedirs(
    f"data/data-preprocessed/labels/{type_labeler}/{task}/{source}/{dep}/{year}/{tiles_size}/",  # noqa
    exist_ok=True,
)

os.makedirs(
    f"data/images-test-{dep.lower()}/",  # noqa
    exist_ok=True,
)

plt.close()
images, bb_images = plot_images_mask_around_point(
    point, source, dep, year, labeler, n_bands, fs, nb_dist=1
)
images.savefig(f"data/images-test-{dep.lower()}/images_{point}_{dep}_{year}.png")


def give_bb_of_zone(bb_images, indices_to_keep=[i for i in range(len(bb_images))]):
    bb_images_keeped = [bb_images[i] for i in indices_to_keep]

    left = min(liste[0] for liste in bb_images_keeped)
    bottom = min(liste[1] for liste in bb_images_keeped)
    right = max(liste[2] for liste in bb_images_keeped)
    top = max(liste[3] for liste in bb_images_keeped)

    return [left, bottom, right, top]


bb_zone = give_bb_of_zone(bb_images, [7, 8])

all_bb_zones = []
all_bb_zones.append(bb_zone)


# ecrire à la suite du fichier et ne pas en créer un nouveau à chaque fois
def bounding_boxes_to_yaml(bounding_boxes, dep, file_path="bb_test.yaml"):
    if not os.path.isfile(file_path):
        data = {dep: bounding_boxes}

        yaml_data = yaml.dump(data, default_flow_style=False)

        with open("bb_test.yaml", "w") as file:
            file.write(yaml_data)

    else:
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)

        data[dep] = bounding_boxes

        with open(file_path, "w") as file:
            yaml.dump(data, file)


bounding_boxes_to_yaml(all_bb_zones, dep)


def load_bounding_boxes_from_yaml(dep, file_path="bb_test.yaml"):
    with open(file_path, "r") as file:
        data = yaml.load(file, Loader=yaml.FullLoader)

    bounding_boxes = data.get(dep, [])

    return bounding_boxes


load_bounding_boxes_from_yaml(dep)
