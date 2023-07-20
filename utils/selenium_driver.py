from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

DATA_DIR = "/opt/output/drive_location/"
DOWNLOAD_DIR = "/opt/output/store_location/"

# WebDriver setup options
def get_driver():
    chrome_options = Options()
    prefs = {'download.default_directory' : DOWNLOAD_DIR}
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_experimental_option('prefs', prefs)
    service = Service(executable_path="/usr/local/bin/chromedriver")
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver
