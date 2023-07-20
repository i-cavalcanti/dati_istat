import os
import jsonlines
from datafactor.log import Handler, Logger
from pymongo import MongoClient
import sys
import logging
import shutil
import requests 
import pandas as pd
import traceback
import datetime as dt
import mysql.connector
from mysql.connector import Error
from Downloader import DownloadData
from factory.selenium_downloader import ScraperFactory


handler = Handler("amqp://{}//".format(os.getenv("RABBIT_HOST", "localhost")))
data_log = Logger("Downloader {}".format(os.getenv("HARVESTER_NAME", "unknown")))

MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_USERNAME = os.getenv("MYSQL_USERNAME")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

DATA_DIR = "/opt/output/drive_location/"
DOWNLOAD_DIR = "/opt/output/store_location/"

def connection_to_dbs():
    # MONGO
    connection = MongoClient(
        f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}",
        # SET A 5 SECOND CONNECTION TIMEOUT
        serverSelectionTimeoutMS=5000
    )
    try:
        info = connection.server_info()
        filtered_info = dict(filter(lambda item: item[0] in "ok", info.items()))
        data_log.info(f"Connection to MongoDB: {list(filtered_info.keys())[0].upper()}")
    except Exception as error:
        data_log.error(str(error))
        data_log.error(f"Failed connection attempt")
        print(str(error))
        sys.exit(1)

    db, coll = MONGO_DB, MONGO_COLLECTION
    collection = connection[db][coll]
    collecetion_length = collection.count_documents({})

    with jsonlines.open(os.path.join(DOWNLOAD_DIR, "Mortality_by_territory_of_residence.jsonl"), "r") as jl:
        new_documents = list(jl)

    # MYSQL
    data_log.info(f"Connecting to MySQL")
    with mysql.connector.connect(
        host=MYSQL_HOST,
        port=int(MYSQL_PORT),
        user=MYSQL_USERNAME,
        passwd=MYSQL_PASSWORD,
        db="dash_harvester"
    ) as cn:
        cursor = cn.cursor(buffered=True)
        try:
            if cn.is_connected():
                data_log.info(f"Connection to MySQL: OK")
                # query = "SELECT * FROM dash_harvester.data_log_harvesters ORDER BY data DESC LIMIT 1"
                query = ("INSERT INTO dash_harvester.data_log_harvesters "
                                "(collection_name, data, collection_length, new_documents) "
                                "VALUES (%(collection_name)s, %(data)s, %(collection_length)s, %(new_documents)s)")

                today = dt.datetime.now()
                date_time = dt.datetime(
                    today.year, today.month, today.day, today.hour, today.minute,
                    tzinfo=dt.timezone(dt.timedelta(hours=-2)))
                date_time = date_time.astimezone(dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

                data = {
                    'collection_name': f'{db}.{coll}',
                    'data': date_time,
                    'collection_length': collecetion_length,
                    'new_documents': len(new_documents)
                }
                cursor.execute(query, data)
                # for a in cursor:
                #     print(a)
                data_log.info("Writing to MySQL DB")
                cn.commit()
        except Error as e:
            print("Error while connecting to MySQL", e)
        finally:
            if cn.is_connected():
                cursor.close()
                cn.close()
                data_log.info("MySQL connection is closed")


if __name__ == "__main__":
    handler.push_application()
    
    try:
        for d in [DATA_DIR, DOWNLOAD_DIR]:
            os.makedirs(d, exist_ok=True)
    except OSError as e:
        data_log.error(str(e))
        data_logging.error(str(e))
        sys.exit(1)
    
    try:
        jsonl = DownloadData(
            roidownload="PROVINCEMORTALITY",
            downloader=ScraperFactory, 
            data_path=DATA_DIR,
            file_path=DOWNLOAD_DIR)
    except Exception as e:
        data_log.error(str(e))
        traceback.print_exc()
        sys.exit(1)
    
    try:
        for d in [DATA_DIR, DOWNLOAD_DIR]:
            shutil.rmtree(d, ignore_errors=False, onerror=None)
    except OSError as e:
        data_log.error(str(e))
        data_logging.error(str(e))
        sys.exit(1)
    
    connection_to_dbs()
    data_log.info({ "level": "info", "message": "Empty downloader" })
    handler.close()
