#!/bin/bash
set -e # exit on error

echo "[INFO] START -> dockerrun.sh"
if [ ! -z "$reinstall_borderliner" ]; then
   if [ ! -z "$suppress_borderliner_output" ]; then
       /usr/local/bin/setup_borderliner.sh > /dev/null
   else
       /usr/local/bin/setup_borderliner.sh
   fi
else 
   echo "[INFO] no borderliner reinstall."
fi

echo "[INFO] Downloading pipeline manifest"

echo "[INFO] manifest file: $manifest"
MANIFEST_DESTINATION="pipeline/manifest.txt"
if [ ! -d "pipeline" ]; then
  mkdir "pipeline"
fi
if [ -z "$manifest" ]; then
    echo "[ERROR] manifest file not specified."
    exit 1
elif [ -f "$manifest" ]; then
    # if is a file
    cp "$manifest" "$MANIFEST_DESTINATION"
elif [[ "$manifest" == s3://* ]]; then
    aws s3 cp "$manifest" "$MANIFEST_DESTINATION"
elif [[ "$manifest" == gs://* ]]; then
    gsutil cp "$manifest" "$MANIFEST_DESTINATION"
elif [[ "$manifest" == az://* ]]; then
    az storage blob download --container-name $AZURE_container_name --name "$manifest" --file "$MANIFEST_DESTINATION"
elif [[ "$manifest" == ftp://* || "$manifest" == sftp://* ]]; then
    curl -u "$FTP_USER:$FTP_PASSWORD" "$manifest" -o "$MANIFEST_DESTINATION"
# elif [[ "$manifest" == http://* || "$manifest" == https://* ]]; then
#     curl "$manifest" -o "$MANIFEST_DESTINATION"
else
    # Minio
    mc cp "$manifest" "$MANIFEST_DESTINATION"
fi

if [ ! -f "$MANIFEST_DESTINATION" ]; then
    echo "[ERROR] Failed to download $MANIFEST_DESTINATION"
    exit 1
fi

# The file containing the list of file manifest
file_manifest=$MANIFEST_DESTINATION

# The directory where the files will be downloaded
download_dir="/pipeline/"




#aws s3 ls s3://uq-data-pipelines-dev/pipelines_v2/pret/
# Loop through each file location in the list and download the file
while read location; do
    # Skip comment lines
    if [[ $location == \#* ]]; then
        continue
    # Skip empty lines
    elif [[ -z "$location" ]]; then
        continue
    # Check if the location is an S3 URL
    elif [[ $location == s3://* ]]; then
        # Get the filename from the S3 URL
        filename=$(basename "$location")
        location=$(echo "$location" | tr -d '\n')
        location=$(echo $location | sed 's/\s*$//')
        # Download the file from S3
        aws s3 cp "$location" "$download_dir"
    # Check if the location is a GCP URL
    elif [[ $location == gs://* ]]; then
        # Download the file from GCP Drive
        gsutil cp "$location" "$download_dir"
    # Check if the location is an Azure URL
    elif [[ $location == az://* ]]; then
        # Download the file from Azure Blob Storage
        az storage blob download --account-name "$AZURE_account_name" --account-key "$AZURE_account_key" --container-name "$AZURE_container_name" --name "$AZURE_blob_name" --file "$download_dir"
    # Check if the location is a URL
    elif [[ $location == http* ]]; then
        # Get the filename from the URL
        filename=$(basename "$location")
        # Download the file from the URL
        curl -L "$location" -o "$download_dir$filename"
    elif [[ $location == sftp://* ]]; then
        # Get the filename from the SFTP URL
        filename=$(basename "$location")

        # Check if the user and password environment variables are set
        if [[ -n "$SFTP_USER" && -n "$SFTP_PASSWORD" ]]; then
            # Download the file using username and password
            curl -sS --user "$SFTP_USER:$SFTP_PASSWORD" "$location" -o "$download_dir$filename"
        elif [[ -n "$SFTP_PUBLIC_KEY" ]]; then
            # Download the file using public key
            curl -sS --key "$SFTP_PUBLIC_KEY" "$location" -o "$download_dir$filename"
        else
            echo "Error: SFTP credentials not provided"
            exit 1
        fi
    # Set environment variables from lines of the format "env var1="value1""
    elif [[ $location =~ ^env\ (.*)=(.*)$ ]]; then
        export ${BASH_REMATCH[1]}="${BASH_REMATCH[2]}"
    else
        location=$(echo "$location" | tr -d '\n')
        location=$(echo $location | sed 's/\s*$//')
        # Get the filename from the location
        filename=$(basename "$location")
        # Download the file from the directory
        cp "$location" "$download_dir$filename"
    fi
done < "$file_manifest"

# Check if there is a requirements.txt file in the download directory
if [ -f "${download_dir}requirements.txt" ]; then
   # Install the dependencies using pip
   echo "[INFO] Installing dependencies from requirements.txt..."
   pip install -r "${download_dir}requirements.txt"
else
   echo "[INFO] No requirements.txt file found."
fi

echo "[INFO] Executing script..."

if [ -z "$run_type" ]; then
    run_type="pipeline"
fi

if [ "$run_type" = "pipeline" ]; then
    # Find the pipeline file
    pipeline_file=$(find "$download_dir" -name "*_pipeline.py" -print -quit)
    if [ -z "$pipeline_file" ]; then
        echo "[ERROR] Could not find pipeline file in $download_dir"
        exit 1
    fi
    cd $download_dir
    # Call python with the pipeline file
    python "$pipeline_file"
elif [ "$run_type" = "integration" ]; then
    # Find the pipeline file
    pipeline_file=$(find "$download_dir" -name "*_integration.py" -print -quit)
    if [ -z "$pipeline_file" ]; then
        echo "[ERROR] Could not find integration file in $download_dir"
        exit 1
    fi
    cd $download_dir
    # Call python with the pipeline file
    python "$pipeline_file"
fi

