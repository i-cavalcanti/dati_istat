from dataclasses import dataclass
from utils.istat_add_levels import AddLevels, Add_ID
from utils.mongo_queries import TimeFilter
from typing import Callable
import datetime
import numpy as np
import pandas as pd
import jsonlines
import logging
import zipfile
import os

logging.basicConfig(level=logging.ERROR, format="%(levelname)s:%(message)s")
logging.basicConfig(level=logging.WARNING, format="%(levelname)s:%(message)s")
logging.basicConfig(level=logging.INFO, format="%(message)s")
logging.getLogger().setLevel(logging.INFO)


select_provinces = ['Foggia', 'Bari', 'Taranto', 'Brindisi', 'Lecce', 'Barletta-Andria-Trani']

PATH = "/opt/output/"

@dataclass 
class DownloadData():
    
    downloader:Callable[[None],None]
    roidownload:str
    data_path:str
    file_path:str
    
    def __post_init__(self):
        
        self.time_periods = TimeFilter().check_for_data()
        self.data_file = self.data_path
        self.download_file = self.file_path
        self.file = self.downloader(self.roidownload).logic_runner()
        self.file_name = self.extract_zip_file()
        self.pre_processing()
    

    def extract_zip_file(self):
        
        try:
            logging.info('Zipfile extraction.')
            direc = f'{self.download_file}/{self.file}'
            with zipfile.ZipFile(direc, "r") as zip_ref:
                zip_ref.extractall(self.data_file)
            os.remove(direc)
            logging.info('Zipfile extraction concluded.')
            return os.listdir(self.data_file)[0]
        except Exception as e: 
            logging.error(str(e))
        
        
    def province_filter(self):
        
        try:
            logging.info('Pre-Processing: Province filter.')
            direc = f'{self.data_file}/{self.file_name}'    
            filtered_rows = []
            for i,chunk in enumerate(pd.read_csv(direc, chunksize=300000, sep = '|')):
                filtered = chunk[chunk['Territorio'].isin(select_provinces)]
                filtered_rows.append(filtered)
                logging.info(f'Pre-Processing: Province filter - Processing chunk: {i}.')
            c = pd.concat(filtered_rows)
            c.to_csv(f'{self.data_file}/data_puglia.csv', index=False)
            os.remove(direc)
            logging.info('Pre-Processing: Province filter concluded.')
            return str(os.listdir(self.data_file)[0])   
        except Exception as e: 
            logging.error(str(e))
        
        
    def year_filter(self, df: pd.DataFrame):
        
        try:
            logging.info('Pre-Processing: Year filter.')
            df.rename(columns = {'Seleziona_periodo':'Data'},inplace = True)
            df.Data= df.Data.astype(str)
            a_t = list(df.Data)
            avalib_time = list(np.unique(a_t))
            select_time = []
            for t in avalib_time:
                time = t.astype(str)
                if (time not in self.time_periods):
                    select_time.append(time)
            select_df = df[df.Data.isin(select_time)]
            logging.info(f'Pre-Processing: Year filter concluded - {len(select_time)} additional years found.')
            return select_df
        except Exception as e: 
            logging.error(str(e))
            
            
    def add_levels(self, df: pd.DataFrame):
        
        try:
            logging.info('Pre-Processing: Input of ISTAT group levels.')
            df.rename(columns = {'Causa_iniziale_di_morte_-_European_Short_List':'Causa_iniziale_di_morte'},inplace = True)
            logging.info('Pre-Processing: Input of GROUP UP level.')
            group_up = AddLevels(df, "UP").add_groups()
            df = df.assign(Causa_iniziale_di_morte_Group_up = group_up)
            logging.info('Pre-Processing: Input of GROUP UP level concluded.')
            logging.info('Pre-Processing: Input of GROUP DOWN level.')
            group_down = AddLevels(df, "DOWN").add_groups()
            df = df.assign(Causa_iniziale_di_morte_Group_down = group_down)
            logging.info('Pre-Processing: Input of GROUP DOWN level concluded.')
            df = df.replace(np.nan, None)
            logging.info('Pre-Processing: Input of ISTAT group levels concluded.')
            return df
        except Exception as e:
            logging.error(str(e))
            
            
    def add_id(self, df: pd.DataFrame):
        
        try:
            logging.info('Pre-Processing: Input of unique ID.')
            df = df.assign(_id = Add_ID(df).add_unique_id())
            logging.info('Pre-Processing: Input of unique ID concluded.')
            return df
        except Exception as e:
            logging.error(str(e))
            
    
    def create_jsonl(self, df: pd.DataFrame):
        
        try:
            logging.info('Pre-Processing: Jsonl file conversion.')
            values = df.to_dict(orient='records')
            with jsonlines.open(os.path.join(PATH, 'Mortality_by_territory_of_residence.jsonl'), 'w') as fout:
                fout.write_all(values)
            logging.info('Pre-Processing: Jsonl file conversion concluded.')
            pass
        except Exception as e:
            logging.error(str(e))
            

    def pre_processing(self):
        
        try:
            logging.info('Pre-Processing.')
            logging.info(f'Number of avaliable years {len(self.time_periods)}')
            direc = f'{self.data_file}/{self.province_filter()}'
            df = pd.read_csv(direc)
            df_1 = df.drop(columns=['ITTER107', 'TIPO_DATO15', 'ETA1_A', 'SEXISTAT1', 'STATCIV2', 
                                    'CAUSEMORTE_SL', 'TITOLO_STUDIO', 'T_BIS_A', 'T_BIS_B', 'ETA1_B',
                                    'T_BIS_C', 'ISO', 'TIME', 'Flag Codes', 'Flags'])
            df_1.rename(columns = {'Età':'Eta', 'Value':'Valore'},inplace = True)
            df_1.columns = df_1.columns.str.strip()
            df_1.columns = df_1.columns.str.replace(' ', '_')
            df_2 = self.add_levels(df_1)
            df_3 = self.year_filter(df_2)
            df_4 = self.add_id(df_3)
            self.create_jsonl(df_4)
            os.remove(direc)
            logging.info('Pre-Processing concluded.')
            return 1  
        except Exception as e: 
            logging.error(str(e))
            return 0
            
        
    
    
