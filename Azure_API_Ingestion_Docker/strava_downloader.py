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

odbcstr='DRIVER=FreeTDS;SERVER={};PORT={};DATABASE={};UID={};PWD={};TDS_Version=8.0;'.format(server,'1433',database,username,password)
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

    refresh_url=refresh_base_url+'?client_id='+client_id+'&client_secret='+client_secret+'&grant_type=refresh_token'+'&refresh_token='+refresh_token
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

