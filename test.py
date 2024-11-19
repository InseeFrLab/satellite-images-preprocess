import os
from osgeo import gdal, osr
import numpy as np

# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C1.tif donnees_brutes/exemple_1.tif
# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C2.tif donnees_brutes/exemple_2.tif
# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C3.tif donnees_brutes/exemple_3.tif
# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C3.tif donnees_brutes/exemple_4.tif
# mc cp s3/projet-slums-detection/data-raw/PLEIADES/GUYANE/2024/IMG_PHR1A_PMS-N_202401281413510_ORT_PHR_PRO_FOP738df107408d3_20241008193330565_1_1_R1C4.tif donnees_brutes/exemple_5.tif
