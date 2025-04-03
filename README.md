# 🛰️ Satellite Image Segmentation – Data Preprocessing Pipeline

This repository provides a preprocessing pipeline for satellite imagery, focused on preparing data for semantic segmentation or object detection tasks. 

It supports:
- Preparation of semantic segmentation and object detection masks.
- Annotation via [BDTOPO](https://geoservices.ign.fr/bdtopo) and [CoSIA](https://cosia.ign.fr/) datasets.
- Cloud filtering and ROI-based patch selection.
- Auto-splitting between train/test datasets.
- Fully integrated with **Argo Workflows** for scalable cloud processing.


## 🚀 Quickstart

### 1. Clone & Setup Environment

```bash
git clone https://github.com/InseeFrLab/satellite-images-preprocess.git
cd satellite-images-preprocess
uv sync
uv run pre-commit install
```

### 2. Run Locally

Set the preprocessing parameters and execute:

```bash
bash bash/run-preprocessing.sh
```

### 3. Run with Argo Workflow ☁️

1. Update parameters in `argo-workflows/pipeline-workflow.yaml`.
2. Submit via Argo CLI or UI:
```bash
argo submit argo-workflows/pipeline-workflow.yaml
```

The Argo Workflows preprocessing template is located at `argo-workflows/pipeline-workflow.yaml`. For each of the given sets of PLEIADES images located on the SSP Cloud object storage service, the raw data is imported onto a worker pod. It is then preprocessed and annotated automatically. The resulting annotated dataset is then uploaded back to the SSP Cloud object storage service.

## ⚙️ Configuration

Preprocessing is parameterized by:
- **Source** (e.g., `PLEIADES`)
- **Department** (e.g., `MAYOTTE`)
- **Labeler** (`COSIA`, `BDTOPO`)
- **Task** (`segmentation`, `detection`)
- **Tile size**, number of bands, and more.

See `src/preprocess-satellite-images.py` for parameter details.


## 🖼️ Labeling

Two labelers are supported:
- `BDTOPO`: Buildings polygons
- `COSIA`: Multi-class semantic masks

Each labeler supports segmentation or detection depending on the `task`.

## 🧼 Filtering & Quality Control

In order mages are filtered by:
- Cloud coverage (adaptive multi-threshold)
- Excessive black pixels
- Inclusion in departmental ROI polygons

Cloud masks are computed using grayscale thresholds (see `Filter` class).


## 📊 Metrics for normalization

Patch statistics (mean, std) are computed per department & tile size and saved as YAML for downstream normalization during model training.


## 🧪 Testing

Patch test regions are configured in `src/config/bb_test.yaml` and used to split the dataset deterministically into training and testing sets.


## 📄 License

Distributed under the **MIT License**.
