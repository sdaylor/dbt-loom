import json
import os
from typing import Dict

from azure.core.exceptions import ClientAuthenticationError, ServiceRequestError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


class AzureClient:
    """A client for loading manifest files from Azure storage."""

    def __init__(self, container_name: str, object_name: str, account_url: str) -> None:
        self.account_url = account_url
        self.container_name = container_name
        self.object_name = object_name

    def load_manifest(self) -> Dict:
        """Load the manifest.json file from Azure storage."""

        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        try:
            if connection_string:
                blob_service_client = BlobServiceClient.from_connection_string(
                    connection_string
                )
            else:
                blob_service_client = BlobServiceClient(
                    self.account_url, credential=DefaultAzureCredential()
                )
            blob_client = blob_service_client.get_blob_client(
                container=self.container_name, blob=self.object_name
            )
        except ClientAuthenticationError:
            raise Exception(
                "Invalid Azure credentials. Please provide valid principal authentication or storage connection string"
            )
        except ServiceRequestError:
            raise Exception(
                "Invalid connection details. Please check your host, container, and blob names."
            )

        # Deserialize the body of the object.
        try:
            content = blob_client.download_blob(encoding="utf-8").readall()
        except Exception:
            raise Exception(
                f"Unable to read the data contained in the object `{self.object_name}"
            )

        try:
            return json.loads(content)
        except Exception:
            raise Exception(
                f"The object `{self.object_name}` does not contain valid JSON."
            )
