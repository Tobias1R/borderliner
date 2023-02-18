#!/bin/bash

# The file containing the list of file locations
file_locations="locations.txt"

# The directory where the files will be downloaded
download_dir="/app/"

# Loop through each file location in the list and download the file
while read location; do
    # Check if the location is an S3 URL
    if [[ $location == s3://* ]]; then
        # Download the file from S3
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
