# Define parameters
export SOURCE="PLEIADES"
export DEPARTMENT="MAYOTTE"
export YEAR="2020"
export NUMBER_BANDS="3"
export LABELER="BDTOPO"
export TASK="segmentation"
export TILE_SIZE="250"
export FROM_S3="1"

# Run preprocessing
uv run src/preprocess-satellite-images.py $SOURCE $DEPARTMENT $YEAR $NUMBER_BANDS $LABELER $TASK $TILE_SIZE $FROM_S3

# Save preprocessed data in Minio
mc cp -r data/data-preprocessed/ s3/projet-slums-detection/data-preprocessed/
