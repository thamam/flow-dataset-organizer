# Flow Detection Dataset - v1

## Description
This dataset contains images and annotations for the flow detection project.
This version (v1) was created on September 15, 2025.

## Data Location
- **Master Manifest:** `s3://neuron-box-items-bucket/datasets/v1/annotations.csv`
- **Raw Images:** `s3://neuron-box-items-bucket/data_items/`

## Schema (`annotations.csv`)
- **uuid:** The unique identifier for the image file. Corresponds to the filename in the `data_items/` prefix.
- **filename:** The original filename of the image during capture.
- **flow:** The annotated flow value.
- **turbidity:** The annotated turbidity value.

## Data Quality
A log of all UUIDs from the original annotations that did not correspond to an existing image file can be found at `s3://neuron-box-items-bucket/datasets/v1/missing_files.log`.