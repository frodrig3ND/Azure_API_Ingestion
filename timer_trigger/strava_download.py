import datetime
import logging
import requests
import os
import json
from typing import Iterator, Optional

from dotenv import find_dotenv, load_dotenv
from .utils import blob
import azure.functions as func

def get_vars() -> Optional[bool]:
    """Collect the needed keys to call the APIs and access storage accounts.

    
    Returns:
        bool: Optional - if dotenv file is present then this is loaded, else the
        vars are used directly from the system env
    """
    try:
        dotenv_path = find_dotenv(".env")
        logging.info("Dotenv located, loading vars from local instance")
        return load_dotenv(dotenv_path)

    except:
        logging.info("Loading directly from system")

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    get_vars()

    client_id=os.environ.get("CLIENT_ID")
    client_secret=os.environ.get("CLIENT_SECRET")
    logging.info('Trying to Refresh Token')
    refresh_base_url="https://www.strava.com/api/v3/oauth/token"
    refresh_token=os.environ.get("REFRESH_TOKEN")

    #logging.info('client_id '+client_id, ' |client_secret '+client_secret,' |refresh '+refresh_token)

    refresh_url=refresh_base_url+'?client_id='+client_id+'&client_secret='+client_secret+'&grant_type=refresh_token'+'&refresh_token='+refresh_token
    flag=False
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
        act=requests.get(activities_url, headers=api_call_headers, verify=False)
        if act.status_code ==200:
            logging.info("Activities Obtained")

            fname='ST'+datetime.datetime.utcnow().strftime("%d_%m_%Y_%H_%M_%S")+'.json'

            blob.store_blob(act.text,fname,'testout','application/json',debug=True)
        else:
            logging.info('Could not retrieve activities')

