import logging
import os
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
import azure.functions 

def get_or_create_container(blob_service_client, container_name):
    container_client = blob_service_client.get_container_client(container_name)
    try:
        container_client.create_container()
        logging.info(f"Container '{container_name}' created.")
    except Exception as e:
        if not e.error_code == 'ContainerAlreadyExists':
            raise

def main(myblob: azure.functions.InputStream):
    try:
        # Assuming 'myblob' includes the path to the .XPT file in the input container
        blob_path = myblob.name
        blob_dir, blob_filename = os.path.split(blob_path)
        base_filename = os.path.splitext(blob_filename)[0]
        csv_filename = f"{base_filename}.csv"`

        # Read the XPT file into a bytes-like object
        xpt_data = myblob.read()

        # Use a BytesIO stream to load into pandas
        df = pd.read_sas(io.BytesIO(xpt_data), format='xport')

        # Convert the DataFrame to CSV format
        csv_data = df.to_csv(index=False)

        # Encode the CSV data to bytes
        csv_bytes = csv_data.encode('utf-8')

        # Get the blob service client to upload the CSV
        connection_string = os.getenv('AzureWebJobsStorage')
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Define the output container name
        output_container_name = 'bronze-level'

        # Create the container if it does not exist
        get_or_create_container(blob_service_client, output_container_name)

        # Split the path into parts
        path_parts = blob_path.split('/')

        # Extract the year portion, which we are assuming to be the second part of the path
        year_folder = path_parts[1]

        # Construct the new output path to store the file directly in the year folder of the output container
        output_blob_path = f"{year_folder}/{csv_filename}"

        # Get the blob client to upload the CSV data
        blob_client = blob_service_client.get_blob_client(output_blob_path)

        # Upload the CSV data to the output blob
        blob_client.upload_blob(csv_bytes, blob_type="BlockBlob", overwrite=True)

        logging.info(f"CSV file uploaded to blob storage: {output_blob_path}")
        
    except Exception as e:
        logging.error(f"Error processing blob: {myblob.name}")
        logging.error(e)