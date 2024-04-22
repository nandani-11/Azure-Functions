# import azure.functions as func
# from azure.storage.blob import BlobServiceClient
# import os
# import json

# def main(req: func.HttpRequest) -> func.HttpResponse:
#     try:
#         req_body = req.get_json() if req.get_body() else {}
#         # Log the body for informational purposes. In production, you might want to remove this.
#         print(f"Received body: {json.dumps(req_body)}")
#         # Set up connection string and client
#         connection_string = os.environ['AzureWebJobsStorage']  # Get connection string from environment variable
#         blob_service_client = BlobServiceClient.from_connection_string(connection_string)

#         # Define the container names
#         source_container_name = 'bronze-level'
#         target_container_name = 'silver-level'

#         # Define the patterns to look for in the file names
#         patterns = ['BMX', 'DBQ', 'DEMO', 'COT', 'OHQ', 'SLQ', 'SMQ', 'SMQFAM', 'SMQMEC', 'WHQ', 'SLQ', 'SMQRTU']

#         # List the blobs in the source container
#         source_container_client = blob_service_client.get_container_client(source_container_name)
#         target_container_client = blob_service_client.get_container_client(target_container_name)

#         # Go through the blobs in the source container
#         for blob in source_container_client.list_blobs():
#             # Check if the blob name contains any of the patterns
#             if any(pattern in blob.name for pattern in patterns):
#                 # Copy the blob to the target container while maintaining the hierarchy
#                 source_blob = source_container_client.get_blob_client(blob)
#                 target_blob = target_container_client.get_blob_client(blob.name)

#                 # Start the copy operation
#                 copy_status = target_blob.start_copy_from_url(source_blob.url)
#                 if copy_status['copy_status'] != 'success':
#                     # Handle the unsuccessful copy
#                     return func.HttpResponse(f"Failed to copy blob {blob.name}", status_code=500)

#         return func.HttpResponse("Files have been copied to the silver-level container.", status_code=200)

#     except Exception as e:
#         return func.HttpResponse(f"An error occurred: {e}", status_code=500)


import azure.functions as func
from azure.storage.blob import BlobServiceClient
import os
import json
import re

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json() if req.get_body() else {}
        # Log the body for informational purposes. In production, you might want to remove this.
        print(f"Received body: {json.dumps(req_body)}")
        # Set up connection string and client
        connection_string = os.environ['AzureWebJobsStorage']  # Get connection string from environment variable
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Define the container names
        source_container_name = 'bronze-level'
        target_container_name = 'silver-level'

        # Define the patterns to look for in the file names
        patterns = re.compile(r'\b(?:BMX|DBQ|DEMO|COT|OHQ|SLQ|SMQ|SMQFAM|SMQMEC|WHQ|SLQ|SMQRTU)\b')

        # List the blobs in the source container
        source_container_client = blob_service_client.get_container_client(source_container_name)
        target_container_client = blob_service_client.get_container_client(target_container_name)

        # Go through the blobs in the source container
        for blob in source_container_client.list_blobs():
            # Check if the blob name contains any of the patterns
            if patterns.search(blob.name):
                # Copy the blob to the target container while maintaining the hierarchy
                source_blob = source_container_client.get_blob_client(blob)
                target_blob = target_container_client.get_blob_client(blob.name)

                # Start the copy operation
                copy_status = target_blob.start_copy_from_url(source_blob.url)
                if copy_status['copy_status'] != 'success':
                    # Handle the unsuccessful copy
                    return func.HttpResponse(f"Failed to copy blob {blob.name}", status_code=500)

        return func.HttpResponse("Files have been copied to the silver-level container.", status_code=200)

    except Exception as e:
        return func.HttpResponse(f"An error occurred: {e}", status_code=500)