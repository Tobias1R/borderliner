#!/bin/bash
set -e # exit on error

echo "[INFO] START -> dockerrun.sh"
if [ ! -z "$reinstall_borderliner" ]; then
   /usr/local/bin/setup_borderliner.sh
else 
   echo "[INFO] no borderliner reinstall."
fi

echo "[INFO] Downloading pipeline file locations"

echo "[INFO] locations file: $locations"

if [ ! -z "$locations" ]; then
   aws s3 cp $locations /app/locations.txt
   if [ ! -f "/app/locations.txt" ]; then
       echo "[ERROR] Failed to download locations.txt"
       exit 1
   fi
else 
   echo "[ERROR] locations file not specified."
   exit 1
fi

# The file containing the list of file locations
file_locations="/app/locations.txt"

# The directory where the files will be downloaded
download_dir="/app/"

aws s3 ls s3://uq-data-pipelines-dev
# Loop through each file location in the list and download the file
while read location; do
    # Check if the location is an S3 URL
    if [[ $location == s3://* ]]; then
        # Download the file from S3
        echo "[INFO] Downloading ${location}"
        aws s3 cp "$location" "$download_dir"
    # Check if the location is a GCP URL
    elif [[ $location == gs://* ]]; then
        # Download the file from GCP Drive
        gsutil cp "$location" "$download_dir"
    # Check if the location is an Azure URL
    elif [[ $location == az://* ]]; then
        # Download the file from Azure Blob Storage
        az storage blob download --account-name <account_name> --account-key <account_key> --container-name <container_name> --name <blob_name> --file "$download_dir"
    # Check if the location is a URL
    elif [[ $location == http* ]]; then
        # Get the filename from the URL
        filename=$(basename "$location")
        # Download the file from the URL
        curl -L "$location" -o "$download_dir$filename"
    else
        # Get the filename from the location
        filename=$(basename "$location")
        # Download the file from the directory
        cp "$location" "$download_dir$filename"
    fi
done < "$file_locations"

# Check if there is a requirements.txt file in the download directory
if [ -f "${download_dir}requirements.txt" ]; then
   # Install the dependencies using pip
   echo "[INFO] Installing dependencies from requirements.txt..."
   pip install -y -r "${download_dir}requirements.txt"
else
   echo "[INFO] No requirements.txt file found."
fi

echo "[INFO] Executing script..."

# Find the pipeline file
pipeline_file=$(find "$download_dir" -name "*_pipeline.py" -print -quit)
cd $download_dir
# Call python with the pipeline file
python "$pipeline_file"
