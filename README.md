# Satellite image segmentation - Data preprocessing

## Getting started

To install dependencies and set up a working environment on the SSP Cloud:

```shell
cd satellite-images-preprocess
source ./setup.sh
```

## Preprocessing pipeline

The Argo Workflows preprocessing template is located at `argo-workflows/pipeline-workflow.yaml`. For each of the given sets of PLEIADES images located on the SSP Cloud object storage service, the raw data is imported onto a worker pod. It is then preprocessed and annotated automatically. The resulting annotated dataset is then uploaded to the object storage service.


## Develop
```shell
export PROJ_LIB=/opt/conda/share/proj
conda install -c conda-forge gdal=3.9.3 -y
pip install -r requirements.txt
```