import os
import requests 
from pymongo import MongoClient


MONGO_HOST = os.getenv("MONGO_HOST")
MONGO_PORT = os.getenv("MONGO_PORT")
MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

# MONGO_HOST = "localhost"
# MONGO_PORT = 27017
# MONGO_USERNAME="root"
# MONGO_PASSWORD="example"
# MONGO_COLLECTION="mortality_causes"
# MONGO_DB="istat"


class TimeFilter():
    
    def __init__(self):
        
        self.client = self.connect_mongo_client()
        self.exist = self.check_if_collection_exists()
        
        
    def from_time_to_str(self, time_periods: list):
        stamp_periods = []
        for t in time_periods:
            time = t.strftime("%Y")
            stamp_periods.append(time)
        return stamp_periods


    def connect_mongo_client(self):
        """
        Connect to a MongoDB deployment.
        """
        mongo_auth='mongodb://{}:{}@{}:{}'.format(MONGO_USERNAME,MONGO_PASSWORD,MONGO_HOST,MONGO_PORT)
        client = MongoClient(mongo_auth)
        return client 


    def check_if_collection_exists(self):
        """
        Check if a collection exists in a Mongo DB database.
        
        Parameter:
        -----------
        client: MongoClient('mongodb://username:password@host:port')
        """
        list_of_collections = self.client[MONGO_DB].list_collection_names() 
        if MONGO_COLLECTION in list_of_collections:
            return 1
        else:
            return 0


    def check_for_data(self):
        """
        If given collection exists, list avaliable years in the dataset, otherwise, 
        return an empty list.
        """
        if self.exist == 1:
            time = self.client[MONGO_DB][MONGO_COLLECTION].distinct('Data')
            time_periods = self.from_time_to_str(time)
        else: 
            time_periods = []
        return time_periods   
    
    
    
    
