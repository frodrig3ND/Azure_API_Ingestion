import logging
import json
import os

import azure.functions as func

from typing import Iterator, Optional

from dotenv import find_dotenv, load_dotenv

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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

get_vars()

server =os.environ.get("SERVER")
database =os.environ.get("DB")
username =os.environ.get("USR")
password =os.environ.get("PWD")
driver = 'ODBC+DRIVER+17+for+SQL+Server'
engine_stmt = ("mssql+pyodbc://%s:%s@%s/%s?driver=%s" % (username, password, server, database, driver))

engine = create_engine(engine_stmt)
Base = declarative_base(engine)

class StA(Base):
    """"""
    __tablename__ = 'strava_act'
    __table_args__ = {'autoload':True}


def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    acts=json.loads(myblob.read().decode('utf-8'))
    logging.info(acts)

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
