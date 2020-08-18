import requests
import json
import os
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, PublicAccess, ContentSettings


def store_blob(data,filename,container_name,content_type,debug=True):
    if debug:
        conn_string="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

    else:
        conn_string=os.environ.get("BL_CONN")
    
    blob_service_client = BlobServiceClient.from_connection_string(conn_string)
    
    container_client = blob_service_client.get_container_client(container_name)

    try:
        container_client.create_container()
    except ResourceExistsError:
        pass
    
    blob_client=container_client.get_blob_client(filename)
    
    blob_client.upload_blob(data,
        content_settings=ContentSettings(content_type=content_type))

