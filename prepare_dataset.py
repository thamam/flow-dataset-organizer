import boto3
import pandas as pd
import json
from tqdm import tqdm
import os


"""
    TL;DR (Too Long; Didn't Read)
    This script creates a master "manifest" file (annotations.csv) for your dataset. It combines all your existing JSON label files, cleans them up, and most importantly, verifies that every single image in your label list actually exists in your S3 bucket. It does all this without moving or renaming your original image files, ensuring nothing breaks. The final manifest is uploaded to a clean, versioned folder in S3, ready for ClearML.
    
    Returns:
        None
"""



# Part A: Boilerplate and S3 Setup
"""This is the "getting ready" phase. It imports the necessary Python libraries (boto3 for AWS, pandas for data handling) and sets up all the key configuration variables—like your S3 bucket name and the folder paths. It's the foundation for all the work that follows."""

# --- Configuration ---
BUCKET_NAME = 'neuron-box-items-bucket'
ANNOTATIONS_PREFIX = 'dataset_annotations/'
IMAGES_PREFIX = 'data_items/'
DESTINATION_PREFIX = 'datasets/v1/'
LOCAL_TEMP_DIR = 'temp_annotations'

# --- Initialize S3 client ---
s3 = boto3.client('s3')

# --- Create a local temporary directory ---
os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)

print("Setup complete. Ready to process.")




# part B - Download and Reshape Annotations
"""This section acts as your "data consolidator." It reaches into S3, downloads all the scattered JSON annotation files, and merges them into one unified table. Then, it pivots this table from a "long" format (one row per label) to a much more useful "wide" format (one row per image), which is ideal for training."""

def download_and_process_annotations():
    """Downloads, combines, and reshapes the annotation files from S3."""
    print(f"Downloading annotation files from s3://{BUCKET_NAME}/{ANNOTATIONS_PREFIX}...")
    
    # List and download all annotation files
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=ANNOTATIONS_PREFIX)
    
    all_labels = []
    for page in pages:
        for obj in page.get('Contents', []):
            if obj['Size'] > 0: # a simple check to skip 'folders'
                file_key = obj['Key']
                local_path = os.path.join(LOCAL_TEMP_DIR, os.path.basename(file_key))
                s3.download_file(BUCKET_NAME, file_key, local_path)
                
                with open(local_path, 'r') as f:
                    data = json.load(f)
                    all_labels.extend(data.get('labels', []))

    if not all_labels:
        print("No labels found. Exiting.")
        return None

    print(f"Found {len(all_labels)} total label entries. Reshaping data...")
    
    # Convert to a DataFrame and pivot
    df_long = pd.DataFrame(all_labels)
    df_wide = df_long.pivot_table(
        index=['uuid', 'filename'], 
        columns='key', 
        values='value'
    ).reset_index()
    
    print("Reshaping complete. Final columns:", df_wide.columns.tolist())
    return df_wide


#  Part C: Data Quality Check
"""This is the most critical step for fulfilling your goal of "identifying data quality issues." It loops through every single entry in your newly created manifest and asks S3, "Hey, does this image file actually exist?" If S3 says no, it records the ID of the missing image in a missing_files.log. This ensures your final dataset is clean and free of broken links."""

def verify_data_quality(df):
    """Checks if each image UUID exists in S3 and logs missing files."""
    print("Starting data quality check. This may take a while...")
    
    missing_uuids = []
    valid_indices = []
    
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Verifying images"):
        uuid = row['uuid']
        image_key = f"{IMAGES_PREFIX}{uuid}"
        try:
            s3.head_object(Bucket=BUCKET_NAME, Key=image_key)
            valid_indices.append(index)
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                missing_uuids.append(uuid)
            else:
                print(f"Unexpected error for UUID {uuid}: {e}")

    # Log missing files
    if missing_uuids:
        print(f"WARNING: Found {len(missing_uuids)} missing image files.")
        with open('missing_files.log', 'w') as f:
            for uuid in missing_uuids:
                f.write(f"{uuid}\n")
        s3.upload_file('missing_files.log', BUCKET_NAME, f"{DESTINATION_PREFIX}missing_files.log")
        print(f"Uploaded missing files log to s3://{BUCKET_NAME}/{DESTINATION_PREFIX}missing_files.log")

    return df.loc[valid_indices]


# Part D: Finalize and Upload
def main():
    """Main function to run the entire data organization pipeline."""
    # 1. Process annotations
    manifest_df = download_and_process_annotations()
    
    if manifest_df is None:
        return
        
    # 2. Verify data quality
    verified_df = verify_data_quality(manifest_df)
    
    # 3. Save and upload the final manifest
    local_manifest_path = 'annotations.csv'
    verified_df.to_csv(local_manifest_path, index=False)
    
    destination_key = f"{DESTINATION_PREFIX}annotations.csv"
    s3.upload_file(local_manifest_path, BUCKET_NAME, destination_key)
    
    print("\n--- 🚀 Process Complete! ---")
    print(f"Final manifest saved to s3://{BUCKET_NAME}/{destination_key}")
    print(f"Total images in final dataset: {len(verified_df)}")
    print("You are now ready for your ClearML setup task.")

if __name__ == "__main__":
    main()