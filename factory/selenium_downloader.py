from factory.IDownloader import IDownloader
from enum import Enum
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from utils.selenium_driver import get_driver
import os
import time
import logging 


DATA_DIR = "/opt/output/drive_location/"
DOWNLOAD_DIR = "/opt/output/store_location/"


class CollectionSources(Enum):
    PROVINCEMORTALITY:str = r'http://dati.istat.it/viewhtml.aspx?il=blank&vh=0000&vf=0&vcq=1100&graph=0&view-metadata=1&lang=it&QueryId=10944&metadata=DCIS_CMORTE1_RES'


class ChromeSetup():
    def __init__(self) -> None:
        
        self.driver = get_driver() 
            
      
class SeleniumDownloader:
    
    
    def selenium_move_mouse(self, driver): 
        action = ActionChains(driver)    
        action.move_by_offset(80, 0).perform()
        action.move_by_offset(0, 80).perform()
        action.move_by_offset(-80, 0).perform()
        action.move_by_offset(0, -80).perform()  
               
        
    def istat_load_page(self, driver, link):
        i = 0
        while i < 5:
            try:
                i = i + 1
                url = CollectionSources[link].value
                driver.get(url)
                time.sleep(10)
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//a[@class='ui-button ui-widget ui-menubar-link ui-state-default ui-button-text-icon-secondary']")))    
                logging.info('SeleniumDownloader: ISTAT page loaded.')
                break
            except:
                logging.warning(f'SeleniumDownloader: Problems on finding page - retry number: {i}')
                driver.refresh()
                time.sleep(5)
            
                    
    def istat_find_frame(self, driver):
        try:
            export_btn = driver.find_element(By.ID, "menubar-export")
            hover = ActionChains(driver).move_to_element(export_btn)
            hover.perform()
            driver.find_element(By.ID, "ui-menu-0-4").click()
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[@class='ui-dialog ui-widget ui-widget-content ui-corner-all ui-draggable']")))   
            pass
        except:
            logging.warning('SeleniumDownloader: Problems on finding frame - retrying...')
            driver.refresh()
            time.sleep(5)
            
                
    def istat_click_on_link(self, driver):
        try:    
            self.selenium_move_mouse(driver)
            frame = driver.find_element(By.XPATH,'//*[@id="DialogFrame"]')
            driver.switch_to.frame(frame)
            csv_link = driver.find_element(By.XPATH, "//a[@class='DownloadLinks'][1]")
            csv_link.click()  
            logging.info('SeleniumDownloader: Clicked on file link')
            time.sleep(10)
            pass 
        except:
            logging.warning('SeleniumDownloader: Problems on finding link - retrying...')
            driver.refresh()
            time.sleep(10)
                 
        
    def checkfile_existency(self): 
        i = 0
        while i < 160:
            for file in os.listdir(DOWNLOAD_DIR):
                if "crdownload" not in file:
                    if ".zip" in file:
                        return file
            time.sleep(5)
            i = i + 1
            logging.info('SeleniumDownloader: Downloading file...')
        return 0
        
        
    def istat_downloader(self, driver, link):
        try:
            i = 0
            while i < 30:
                i = i + 1
                self.istat_load_page(driver, link)
                self.istat_find_frame(driver)
                self.istat_click_on_link(driver)
                file = self.checkfile_existency()
                if file != 0: 
                    return file
            return 0
        except:
            logging.error('SeleniumDownloader: Error with download - limit of retries reached.')


class ScraperFactory():
    
    downloader_mapper={
            "PROVINCEMORTALITY":SeleniumDownloader}
    
    downloader_driver={
            "Chrome":ChromeSetup}

    def __init__(self, roi_download:str, **kwargs) -> None:
        
        self.chrome = ChromeSetup()
        self.roi_download = roi_download
        self.selenium = self.downloader_mapper.get(roi_download)(**kwargs)
    
    def logic_runner(self)-> str:
        file = self.selenium.istat_downloader(self.chrome.driver, self.roi_download)
        if file == 0:
            logging.info('SeleniumDownloader: Problems with .zip download')
        else: 
            logging.info('SeleniumDownloader: File .zip downloaded with success.')
            return file
        
    
if __name__ == "__main__":
    downloader = ScraperFactory("PROVINCEMORTALITY")
    downloader.logic_runner()
    