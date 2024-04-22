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

    # List all blobs in the container with 'train' prefix
    train_blobs = list(container_client.list_blobs(name_starts_with='train', include=['metadata']))
    test_blobs = list(container_client.list_blobs(name_starts_with='test', include=['metadata']))

    if not train_blobs or not test_blobs:
        logging.info("Could not find both training and test data in the gold-level container.")
        return

    # Get the most recent train and test blobs
    most_recent_train_blob = max(train_blobs, key=lambda x: x.metadata['last_modified'])
    most_recent_test_blob = max(test_blobs, key=lambda x: x.metadata['last_modified'])

    train_blob_name = most_recent_train_blob.name
    test_blob_name = most_recent_test_blob.name

    logging.info(f"Most recent training file: {train_blob_name}")
    logging.info(f"Most recent test file: {test_blob_name}")

    # Register the training data asset
    train_data_asset = Data(
        path=f"azureml://datastores/{datastore_name}/paths/{train_blob_name}",
        type=AssetTypes.MLTABLE,
        name=f"training_data_{most_recent_train_blob.metadata['last_modified'].strftime('%Y%m%d%H%M%S')}",
        description="New training data version registered by Azure Function."
    )
    train_data_asset = ml_client.data.create_or_update(train_data_asset)
    
    # Register the test data asset
    test_data_asset = Data(
        path=f"azureml://datastores/{datastore_name}/paths/{test_blob_name}",
        type=AssetTypes.MLTABLE,
        name=f"testing_data_{most_recent_test_blob.metadata['last_modified'].strftime('%Y%m%d%H%M%S')}",
        description="New testing data version registered by Azure Function."
    )
    test_data_asset = ml_client.data.create_or_update(test_data_asset)

    logging.info(f"Registered new training data asset: {train_data_asset.name}")
    logging.info(f"Registered new testing data asset: {test_data_asset.name}")