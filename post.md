# 3rd Party Data Ingestion using Azure Offerings
Recently I have been dealing with moving a lot of my automatic data ingestion/ reporting pipelines from local hardware to Azure solutions. This prompted me to take a deep dive into their solutions to perform a simple task query an API with oauth2.0 authorization and push that data into Azure Blobs or a hosted SQLDB. In this case pulling my activity information from the previously discussed [Strava API](strava.com)

Azures offerings are kind of all over the place and there is significant overlap between solutions. For this specific task I identified 4 different ways to perform that given task, with some pros and cons.

1. Azure Data Factory
2. Logic Apps
3. Azure Functions
4. Container Instances

In terms of coding ability these go somewhat in ascending order, with Azure Data Factory (ADF) and Logic Apps having diagram/flow interfaces and Azure Functions and Container Instances require coding.

## Test Setup
Azure actually has quite a few options for debugging locally. Mostly relevant for the Azure Functions and Container Instances. All of them can be downloaded from the Azure website. For this project I used both the Azure Storage Emulator and the Azure SQL Emulator.

## Azure Data Factory
In order to setup this solution I mostly followed the instructions outlined in this [blog](blogpost.com). Basically we setup an _http request_ using a _refresh token_ previously obtained using postman to obtain an _authorization token_. We can further secure this by using blob storage to store the _refresh token_ instead of using plain text on the connector.

![alt text](Isolated.png "Title")

Once we have obtained the _authorization token_ we can make a call with the correct headers to the API endpoint to obtain the data (in my case activities). We can pass the resulting values in the JSON array into a blob storage. I also chose to setup a separate pipeline to then push the blobs into SQL Server. This is to keep some parallelism (obviously this is overkill for my Strava data).

![alt text](Isolated.png "Title")

I also set up a branch from the authorization http call to remind me when the _refresh token_ has expired and needs to be refreshed. The re-authorization workflow can't be kicked off directly from there so I would have to generate a new refresh token from postman.

The one downside that at least for the version I have available on Azure I do not have the capability of using private endpoints. This means that I would either have to setup private(ish) facing blob storage/ SQL database. There is a solution for using a (Azure Data Factory Managed Virtual Network)[https://docs.microsoft.com/en-us/azure/data-factory/managed-virtual-network-private-endpoint] but its in preview and I don't have access.

## Logic App
Logic App is basically the developer version of __Power Automate/ Flow__. This means we can both work on the designer or use a code editor. In order to deal with the API for Strava we create a custom logic connector. To do so we go to _Logic App Connector_.
![alt text](LA1.png "Logic custom connector")

Once we create the connector we can setup our _Logic App_. We are going to set it up with a timer function to fire of once a day to pull our activities. Form there we fire off our custom connector. Finally we process the JSON data into a blob, or if we want directly into the SQL connector. The main problem is that at least in the version I have access to, we need to set it up so its a public facing SQL, and cannot use the private endpoints.

## Azure Functions
To setup the Azure Function App I based myself on the tutorial posted in this [blog](BLOG.com).
Switiching over to VS Code as recommended (and for the Azure Integration).

The ingestion system is going to consist of two different functions.

1. Call Strava API and push resulting JSON into blob storage. Triggered with a Timer.
2. Ingest Blob and push to SQL database

Using VS Code and the Azure Functions plugins we end up with the following project.

```
Azure_API_Ingestion
|   .env
|   .funcignore
|   .gitignore
|   host.json
|   local.settings.json
|   proxies.json
|   requirements.txt
|   Untitled.ipynb
|
+---BlobTrigger
|   |   blob2sql.py
|   |   function.json
|   |   readme.md
|   |   sample.dat
|   |   __init__.py
|
+---timer_trigger
    |   function.json
    |   readme.md
    |   sample.dat
    |   strava_download.py
    |
    +---utils
       |   blob.py
       |   __init__.py

```

We start the project by selecting the 'Create Function' in the Azure Function plugin window.
This will generate the project structure

![alt text](LA1.png "Azure Function App Generation")

I first created the `timer-trigger` function, this will handle the downloading. I renamed the initial __\_\_init\_\_.py__ to __strava_download.py__ for clarity. Because of that the __function.json__ file under `timer_trigger` had to be modified. This file defines all the bindings (in and out) for a specific function, in the timer case it contains the CRON expression for it the name, and the type of the trigger (which we will use in __strava_download.py__).

```json
{
  "scriptFile": "strava_download.py",
  "bindings": [
    {
      "name": "mytimer",
      "type": "timerTrigger",
      "direction": "in",
      "schedule": "0 0 9 * * *"
    }
  ]
}
```
We change the `sciptFile` parameter to __strava_download.py__. We also set our CRON expression to what we need in my case I used `"0 0 9 * * *"`.

For __strava_download.py__ the main points are to capture the trigger, read our __.env__ file for our sensitive variables, pull data from the Strava API and push into a blob.

For our `main` function we define the input trigger for it as follows:
```python
def main(mytimer: func.TimerRequest) -> None:
```
If we had changed the timer name in the json definition we would have to adjust it.

Since we don't want to store our sensitive variables in our script we use a __.env__ file.

```
#STRAVA CREDS
CLIENT_ID= '***'
CLIENT_SECRET='***'
REFRESH_TOKEN='***'

#BLOB CREDS
BL_CONN= '***'

#SQL CREDS
SERVER = 'localhost'
DB= '***'
USR= '***'
PWD= '***'
```

Then we call `find_dotenv` and `load_dotenv` as shown in the [Azure Functions tutorial](url???.com).

Now we set our Strava Credentials as follows

```python
client_id=os.environ.get("CLIENT_ID")
client_secret=os.environ.get("CLIENT_SECRET")
refresh_token=os.environ.get("REFRESH_TOKEN")
```
For Strava I will just use the refresh token to obtain a new access token\\check if it hasn't expired. This could obviously be changed. In fact Azure does contain ways to do so using the [Azure Key Vault](url???).

Once we make the API call to get the activities (code can be found [here](url???)). We will pass the results of the call to a storage blob to be further processed (we could also generate one individual blob per record if we wanted to, which would be perhaps a better option).

This is done using the __util\\.blob.py__ functions. There is a why to use the output bindings in the Azure Function, however at the current moment there is no way to set the file type of the blob using that method.

```python
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

```

We are using `azure.storage.blob` to manage most of the heavy lifting. We first create a `blob_service_client` from our connection string which is either on our __.env__ file or using the standard debug connect to Azure Storage Emulator. Afterwards we create a container client to the container name of our choice Finally we upload our data into a blob, setting the correct content type.

The second function `BlobTrigger` is fired off whenever the blob container has a new blob created, the blob data is read and using `sqlalchemy` we upload the blob contents to a __MSSQL Server__ which in my case is just running on docker but could also be hosted on azure. Again the relevant credentials are stored in the __.env__ file.

The __function.json__ needs to be pointed towards the correct blob define its name in our code.

```json
{
  "scriptFile": "blob2sql.py",
  "bindings": [
    {
      "name": "myblob",
      "type": "blobTrigger",
      "direction": "in",
      "path": "testout/{name}",
      "connection": "AzureWebJobsStorage"
    }
  ]
}
```
Then when we define our function, in __blob2sql.py__ we use the define name.

```python
def main(myblob: func.InputStream):
```
The we can read the blob content using"
```python
acts=json.loads(myblob.read().decode('utf-8'))
```

We finally upload using the sqlalchemy session and merge the results into our sql database.

```python
session = sessionmaker(bind=engine)()

for act in acts:
    average_heartrate=act.get('average_heartrate')
    average_speed=act.get('average_speed')
    distance=act.get('distance')
    elapsed_time=act.get('elapsed_time')
    moving_time=act.get('moving_time')
    name=act.get('name')
    start_date=act.get('start_date')
    wid=act.get('id')

    print('hr:%s,sp:%s, dist:%s, etime:%s, mtime:%s, name:%s, sdate=%s, wid=%s'%(average_heartrate,
    average_speed,
    distance,
    elapsed_time,
    moving_time,
    name,
    start_date,
    wid))

    sact=StA(average_heartrate=act.get('average_heartrate'),
    average_speed=act.get('average_speed'),
    distance=act.get('distance'),
    elapsed_time=act.get('elapsed_time'),
    moving_time=act.get('moving_time'),
    name=act.get('name'),
    start_date=act.get('start_date'),
    wid=act.get('id'))
    session.merge(sact)
    session.commit()
```
Azure functions are quite convenient and will probably become the way I would do this once they develop further. The main con that would holds me back in using them for my project is that using private endpoints is not easy. Frankly the solution is [quite complex](https://www.michaelscollier.com/azure-functions-with-private-endpoints/).

## Container Instances
Finally I just used a container instance, this basically fires off a docker container image (which should be hosted in the Azure Registry). To do so I will use a fairly standard docker image (it does have to do some apt get to setup FreeTDS as the driver as opposed to the usual MSSQL drivers),

```dockerfile
FROM python:3.7-slim-buster

RUN mkdir /app
WORKDIR /app

COPY . .

# install FreeTDS and dependencies
RUN apt-get update \
 && apt-get install unixodbc -y \
 && apt-get install unixodbc-dev -y \
 && apt-get install freetds-dev -y \
 && apt-get install freetds-bin -y \
 && apt-get install tdsodbc -y \
 && apt-get install --reinstall build-essential -y

# populate "ocbcinst.ini" as this is where ODBC driver config sits
RUN echo "[FreeTDS]\n\
Description = FreeTDS Driver\n\
Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" >> /etc/odbcinst.ini

RUN pip install -r requirements.txt

CMD ["python","strava_downloader.py"]
```
Then the docker compose file (which we can fire off from VSCode with the plugin).

```docker-compose
version: '3.6'

services:
  downloader:
    build:
      context: .
      dockerfile: Dockerfile
    networks:
      - default
    ports:
      - 5000:5000
```

As far as the python script to perform and process the API call, this is just a combination of the two previous scripts (__strava_download.py___, ___blob2sql.py___).

```python
import datetime
import logging
import requests
import os
import urllib
import json
from typing import Iterator, Optional

from dotenv import find_dotenv, load_dotenv

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

def get_vars() -> Optional[bool]:
    try:
        dotenv_path = find_dotenv(".env")
        logging.info("Dotenv located, loading vars from local instance")
        return load_dotenv(dotenv_path)

    except:
        logging.info("Loading directly from system")

get_vars()
server =os.environ.get("SERVER")
database =os.environ.get("DB")
username =os.environ.get("USR")
password =os.environ.get("PWD")

odbcstr='DRIVER=FreeTDS;SERVER={};PORT={};
  DATABASE={};UID={};
    PWD={};TDS_Version=8.0;'.format(server,
      '1433',database,username,password)
stmnt='mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(odbcstr)
engine = create_engine(stmnt)
Base = declarative_base(engine)


class StA(Base):
    """"""
    __tablename__ = 'test2'
    __table_args__ = {'autoload':True}


if __name__ == "__main__":
    get_vars()
    server =os.environ.get("SERVER")
    database =os.environ.get("DB")
    username =os.environ.get("USR")
    password =os.environ.get("PWD")
    driver = 'FreeTDS'


    client_id=os.environ.get("CLIENT_ID")
    client_secret=os.environ.get("CLIENT_SECRET")
    logging.info('Trying to Refresh Token')
    refresh_base_url="https://www.strava.com/api/v3/oauth/token"
    refresh_token=os.environ.get("REFRESH_TOKEN")

    refresh_url=refresh_base_url+'?client_id='+client_id+
    '&client_secret='+client_secret+'&grant_type=refresh_token'
    +'&refresh_token='+refresh_token
    flag=False
    sql_store=False
    r=requests.post(refresh_url)
    if r.status_code==200:
        logging.info(r.text)
        flag=True
    else:
        logging.critical('Could not refresh token')
        logging.critical(r.text)

    if flag:
        token=json.loads(r.text)
        api_call_headers = {'Authorization': 'Bearer ' + token['access_token']}
        activities_url="https://www.strava.com/api/v3/athlete/activities?"
        req=requests.get(activities_url, headers=api_call_headers, verify=False)
        if req.status_code ==200:
            logging.info("Activities Obtained")
            sql_store=True
            #logging.info(act)
            acts=json.loads(req.text)
            session = sessionmaker(bind=engine)()
            for act in acts:
                average_heartrate=act.get('average_heartrate')
                average_speed=act.get('average_speed')
                distance=act.get('distance')
                elapsed_time=act.get('elapsed_time')
                moving_time=act.get('moving_time')
                name=act.get('name')
                start_date=act.get('start_date')
                wid=act.get('id')

                print('hr:%s,sp:%s, dist:%s, etime:%s,
                mtime:%s, name:%s, sdate=%s,
                wid=%s'%(average_heartrate,
                average_speed,
                distance,
                elapsed_time,
                moving_time,
                name,
                start_date,
                wid))

                sact=StA(average_heartrate=act.get('average_heartrate'),
                average_speed=act.get('average_speed'),
                distance=act.get('distance'),
                elapsed_time=act.get('elapsed_time'),
                moving_time=act.get('moving_time'),
                name=act.get('name'),
                start_date=act.get('start_date'),
                wid=act.get('id'))
                session.merge(sact)
                session.commit()


```

The main advantage is that this way I can run the container inside our virtual network and have it access the private SQL endpoint. In this way there really isn't any part of the pipeline that is public facing. The code for this setup is [here](github).
