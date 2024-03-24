import logging
import azure.functions as func
from azure.ai.ml import MLClient
from azure.ai.ml.constants import AssetTypes
from azure.ai.ml.entities import Data
from azure.storage.blob import BlobServiceClient
import os
from azure.identity import DefaultAzureCredential

def main(myblob: func.InputStream):
    # Set environment variables beforehand in the Function App settings
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    resource_group = os.environ["AZURE_RESOURCE_GROUP"]
    workspace_name = os.environ["AZURE_ML_WORKSPACE_NAME"]
    connection_string = os.environ['AzureWebJobsStorage']
    container_name = 'gold-level'
    datastore_name = 'traintestdata'
    
    # Create MLClient using DefaultAzureCredential
    credential = DefaultAzureCredential()
    ml_client = MLClient(credential, subscription_id, resource_group, workspace_name)
    
    # Create a blob service client to retrieve files from the blob container
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    # List all blobs in the container and sort them by the last modified date
    blob_list = list(container_client.list_blobs())  # Make sure to convert the iterator to a list to be able to sort
    sorted_blobs = sorted(blob_list, key=lambda x: x.last_modified, reverse=True)

    # Separate blobs into training and test datasets
    train_blobs = [blob for blob in sorted_blobs if 'train' in blob.name]
    test_blobs = [blob for blob in sorted_blobs if 'test' in blob.name]

    if not train_blobs or not test_blobs:
        logging.info("Could not find both training and test data in the gold-level container.")
        return

    # Get the most recent train and test blobs
    most_recent_train_blob = train_blobs[0]
    most_recent_test_blob = test_blobs[0]

    train_blob_name = most_recent_train_blob.name
    test_blob_name = most_recent_test_blob.name

    logging.info(f"Most recent training file: {train_blob_name}")
    logging.info(f"Most recent test file: {test_blob_name}")

    # Register the training data asset
    train_data_asset = Data(
        path=f"azureml://datastores/{datastore_name}/paths/{train_blob_name}",
        type=AssetTypes.MLTABLE,
        name=f"training_data_{most_recent_train_blob.last_modified.strftime('%Y%m%d%H%M%S')}",
        description="New training data version registered by Azure Function."
    )
    train_data_asset = ml_client.data.create_or_update(train_data_asset)
    
    # Register the test data asset
    test_data_asset = Data(
        path=f"azureml://datastores/{datastore_name}/paths/{test_blob_name}",
        type=AssetTypes.MLTABLE,
        name=f"testing_data_{most_recent_test_blob.last_modified.strftime('%Y%m%d%H%M%S')}",
        description="New testing data version registered by Azure Function."
    )
    test_data_asset = ml_client.data.create_or_update(test_data_asset)

    logging.info(f"Registered new training data asset: {train_data_asset.name}")
    logging.info(f"Registered new testing data asset: {test_data_asset.name}")
